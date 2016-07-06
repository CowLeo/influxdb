package stat // import "github.com/influxdata/influxdb/stat"

import "sync"

type Int struct {
	mu    sync.RWMutex
	value int64
}

func (i *Int) Add(delta int64) {
	i.mu.Lock()
	i.value += delta
	i.mu.Unlock()
}

func (i *Int) Store(value int64) {
	i.mu.Lock()
	i.value = value
	i.mu.Unlock()
}

func (i *Int) Load() int64 {
	i.mu.RLock()
	value := i.value
	i.mu.RUnlock()
	return value
}
