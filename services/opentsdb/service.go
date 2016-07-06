package opentsdb // import "github.com/influxdata/influxdb/services/opentsdb"

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/stat"
	"github.com/influxdata/influxdb/tsdb"
)

// statistics gathered by the openTSDB package.
const (
	statHTTPConnectionsHandled   = "httpConnsHandled"
	statTelnetConnectionsActive  = "tlConnsActive"
	statTelnetConnectionsHandled = "tlConnsHandled"
	statTelnetPointsReceived     = "tlPointsRx"
	statTelnetBytesReceived      = "tlBytesRx"
	statTelnetReadError          = "tlReadErr"
	statTelnetBadLine            = "tlBadLine"
	statTelnetBadTime            = "tlBadTime"
	statTelnetBadTag             = "tlBadTag"
	statTelnetBadFloat           = "tlBadFloat"
	statBatchesTransmitted       = "batchesTx"
	statPointsTransmitted        = "pointsTx"
	statBatchesTransmitFail      = "batchesTxFail"
	statConnectionsActive        = "connsActive"
	statConnectionsHandled       = "connsHandled"
	statDroppedPointsInvalid     = "droppedPointsInvalid"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	ln     net.Listener  // main listener
	httpln *chanListener // http channel-based listener

	mu   sync.Mutex
	wg   sync.WaitGroup
	done chan struct{}
	err  chan error
	tls  bool
	cert string

	BindAddress     string
	Database        string
	RetentionPolicy string

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	MetaClient interface {
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
	}

	// Points received over the telnet protocol are batched.
	batchSize    int
	batchPending int
	batchTimeout time.Duration
	batcher      *tsdb.PointBatcher

	LogPointErrors bool
	Logger         *log.Logger
	statMap        struct {
		HTTPConnectionsHandled   stat.Int
		ActiveTelnetConnections  stat.Int
		HandledTelnetConnections stat.Int
		TelnetPointsReceived     stat.Int
		TelnetBytesReceived      stat.Int
		TelnetReadError          stat.Int
		TelnetBadLine            stat.Int
		TelnetBadTime            stat.Int
		TelnetBadTag             stat.Int
		TelnetBadFloat           stat.Int
		BatchesTransmitted       stat.Int
		PointsTransmitted        stat.Int
		BatchesTransmitFail      stat.Int
		ActiveConnections        stat.Int
		HandledConnections       stat.Int
		InvalidDroppedPoints     stat.Int
		Tags                     models.Tags
	}
}

// NewService returns a new instance of Service.
func NewService(c Config) (*Service, error) {
	// Use defaults where necessary.
	d := c.WithDefaults()

	s := &Service{
		done:            make(chan struct{}),
		tls:             d.TLSEnabled,
		cert:            d.Certificate,
		err:             make(chan error),
		BindAddress:     d.BindAddress,
		Database:        d.Database,
		RetentionPolicy: d.RetentionPolicy,
		batchSize:       d.BatchSize,
		batchPending:    d.BatchPending,
		batchTimeout:    time.Duration(d.BatchTimeout),
		Logger:          log.New(os.Stderr, "[opentsdb] ", log.LstdFlags),
		LogPointErrors:  d.LogPointErrors,
	}
	return s, nil
}

// Open starts the service
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Logger.Println("Starting OpenTSDB service")

	// Configure expvar monitoring. It's OK to do this even if the service fails to open and
	// should be done before any data could arrive for the service.
	s.statMap.Tags = map[string]string{"bind": s.BindAddress}

	if _, err := s.MetaClient.CreateDatabase(s.Database); err != nil {
		s.Logger.Printf("Failed to ensure target database %s exists: %s", s.Database, err.Error())
		return err
	}

	s.batcher = tsdb.NewPointBatcher(s.batchSize, s.batchPending, s.batchTimeout)
	s.batcher.Start()

	// Start processing batches.
	s.wg.Add(1)
	go s.processBatches(s.batcher)

	// Open listener.
	if s.tls {
		cert, err := tls.LoadX509KeyPair(s.cert, s.cert)
		if err != nil {
			return err
		}

		listener, err := tls.Listen("tcp", s.BindAddress, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		if err != nil {
			return err
		}

		s.Logger.Println("Listening on TLS:", listener.Addr().String())
		s.ln = listener
	} else {
		listener, err := net.Listen("tcp", s.BindAddress)
		if err != nil {
			return err
		}

		s.Logger.Println("Listening on:", listener.Addr().String())
		s.ln = listener
	}
	s.httpln = newChanListener(s.ln.Addr())

	// Begin listening for connections.
	s.wg.Add(2)
	go s.serveHTTP()
	go s.serve()

	return nil
}

// Close closes the openTSDB service
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ln != nil {
		return s.ln.Close()
	}

	if s.batcher != nil {
		s.batcher.Stop()
	}
	close(s.done)
	s.wg.Wait()
	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (s *Service) SetLogOutput(w io.Writer) {
	s.Logger = log.New(w, "[opentsdb] ", log.LstdFlags)
}

func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "opentsdb",
		Tags: s.statMap.Tags,
		Values: map[string]interface{}{
			statHTTPConnectionsHandled:   s.statMap.HTTPConnectionsHandled.Load(),
			statTelnetConnectionsActive:  s.statMap.ActiveTelnetConnections.Load(),
			statTelnetConnectionsHandled: s.statMap.HandledTelnetConnections.Load(),
			statTelnetPointsReceived:     s.statMap.TelnetPointsReceived.Load(),
			statTelnetBytesReceived:      s.statMap.TelnetBytesReceived.Load(),
			statTelnetReadError:          s.statMap.TelnetReadError.Load(),
			statTelnetBadLine:            s.statMap.TelnetBadLine.Load(),
			statTelnetBadTime:            s.statMap.TelnetBadTime.Load(),
			statTelnetBadTag:             s.statMap.TelnetBadTag.Load(),
			statTelnetBadFloat:           s.statMap.TelnetBadFloat.Load(),
			statBatchesTransmitted:       s.statMap.BatchesTransmitted.Load(),
			statPointsTransmitted:        s.statMap.PointsTransmitted.Load(),
			statBatchesTransmitFail:      s.statMap.BatchesTransmitFail.Load(),
			statConnectionsActive:        s.statMap.ActiveConnections.Load(),
			statConnectionsHandled:       s.statMap.HandledConnections.Load(),
			statDroppedPointsInvalid:     s.statMap.InvalidDroppedPoints.Load(),
		},
	}}
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Wait for next connection.
		conn, err := s.ln.Accept()
		if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
			s.Logger.Println("openTSDB TCP listener closed")
			return
		} else if err != nil {
			s.Logger.Println("error accepting openTSDB: ", err.Error())
			continue
		}

		// Handle connection in separate goroutine.
		go s.handleConn(conn)
	}
}

// handleConn processes conn. This is run in a separate goroutine.
func (s *Service) handleConn(conn net.Conn) {
	defer s.statMap.ActiveConnections.Add(-1)
	s.statMap.ActiveConnections.Add(1)
	s.statMap.HandledConnections.Add(1)

	// Read header into buffer to check if it's HTTP.
	var buf bytes.Buffer
	r := bufio.NewReader(io.TeeReader(conn, &buf))

	// Attempt to parse connection as HTTP.
	_, err := http.ReadRequest(r)

	// Rebuild connection from buffer and remaining connection data.
	bufr := bufio.NewReader(io.MultiReader(&buf, conn))
	conn = &readerConn{Conn: conn, r: bufr}

	// If no HTTP parsing error occurred then process as HTTP.
	if err == nil {
		s.statMap.HTTPConnectionsHandled.Add(1)
		s.httpln.ch <- conn
		return
	}

	// Otherwise handle in telnet format.
	s.wg.Add(1)
	s.handleTelnetConn(conn)
}

// handleTelnetConn accepts OpenTSDB's telnet protocol.
// Each telnet command consists of a line of the form:
//   put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0
func (s *Service) handleTelnetConn(conn net.Conn) {
	defer conn.Close()
	defer s.wg.Done()
	defer s.statMap.ActiveTelnetConnections.Add(-1)
	s.statMap.ActiveTelnetConnections.Add(1)
	s.statMap.HandledTelnetConnections.Add(1)

	// Get connection details.
	remoteAddr := conn.RemoteAddr().String()

	// Wrap connection in a text protocol reader.
	r := textproto.NewReader(bufio.NewReader(conn))
	for {
		line, err := r.ReadLine()
		if err != nil {
			if err != io.EOF {
				s.statMap.TelnetReadError.Add(1)
				s.Logger.Println("error reading from openTSDB connection", err.Error())
			}
			return
		}
		s.statMap.TelnetPointsReceived.Add(1)
		s.statMap.TelnetBytesReceived.Add(int64(len(line)))

		inputStrs := strings.Fields(line)

		if len(inputStrs) == 1 && inputStrs[0] == "version" {
			conn.Write([]byte("InfluxDB TSDB proxy"))
			continue
		}

		if len(inputStrs) < 4 || inputStrs[0] != "put" {
			s.statMap.TelnetBadLine.Add(1)
			if s.LogPointErrors {
				s.Logger.Printf("malformed line '%s' from %s", line, remoteAddr)
			}
			continue
		}

		measurement := inputStrs[1]
		tsStr := inputStrs[2]
		valueStr := inputStrs[3]
		tagStrs := inputStrs[4:]

		var t time.Time
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			s.statMap.TelnetBadTime.Add(1)
			if s.LogPointErrors {
				s.Logger.Printf("malformed time '%s' from %s", tsStr, remoteAddr)
			}
		}

		switch len(tsStr) {
		case 10:
			t = time.Unix(ts, 0)
			break
		case 13:
			t = time.Unix(ts/1000, (ts%1000)*1000)
			break
		default:
			s.statMap.TelnetBadTime.Add(1)
			if s.LogPointErrors {
				s.Logger.Printf("bad time '%s' must be 10 or 13 chars, from %s ", tsStr, remoteAddr)
			}
			continue
		}

		tags := make(map[string]string)
		for t := range tagStrs {
			parts := strings.SplitN(tagStrs[t], "=", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				s.statMap.TelnetBadTag.Add(1)
				if s.LogPointErrors {
					s.Logger.Printf("malformed tag data '%v' from %s", tagStrs[t], remoteAddr)
				}
				continue
			}
			k := parts[0]

			tags[k] = parts[1]
		}

		fields := make(map[string]interface{})
		fv, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			s.statMap.TelnetBadFloat.Add(1)
			if s.LogPointErrors {
				s.Logger.Printf("bad float '%s' from %s", valueStr, remoteAddr)
			}
			continue
		}
		fields["value"] = fv

		pt, err := models.NewPoint(measurement, tags, fields, t)
		if err != nil {
			s.statMap.TelnetBadFloat.Add(1)
			if s.LogPointErrors {
				s.Logger.Printf("bad float '%s' from %s", valueStr, remoteAddr)
			}
			continue
		}
		s.batcher.In() <- pt
	}
}

// serveHTTP handles connections in HTTP format.
func (s *Service) serveHTTP() {
	handler := &Handler{
		Database:        s.Database,
		RetentionPolicy: s.RetentionPolicy,
		PointsWriter:    s.PointsWriter,
		Logger:          s.Logger,
	}
	handler.statMap.InvalidDroppedPoints = &s.statMap.InvalidDroppedPoints
	srv := &http.Server{Handler: handler}
	srv.Serve(s.httpln)
}

// processBatches continually drains the given batcher and writes the batches to the database.
func (s *Service) processBatches(batcher *tsdb.PointBatcher) {
	defer s.wg.Done()
	for {
		select {
		case batch := <-batcher.Out():
			if err := s.PointsWriter.WritePoints(s.Database, s.RetentionPolicy, models.ConsistencyLevelAny, batch); err == nil {
				s.statMap.BatchesTransmitted.Add(1)
				s.statMap.PointsTransmitted.Add(int64(len(batch)))
			} else {
				s.Logger.Printf("failed to write point batch to database %q: %s", s.Database, err)
				s.statMap.BatchesTransmitFail.Add(1)
			}

		case <-s.done:
			return
		}
	}
}
