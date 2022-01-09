package bt2

import "sync"

type Page interface {
	GetData() []byte
	GetLock() *sync.RWMutex
	GetPageID() int
}
