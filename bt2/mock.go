package bt2

import "sync"

func newMockPage(d []byte) *mockPage {
	return &mockPage{
		data:    d,
		RWMutex: &sync.RWMutex{},
	}
}

type mockPage struct {
	data []byte
	id   int
	*sync.RWMutex
}

func (p *mockPage) GetPageID() int {
	return 0
}
func (p *mockPage) GetData() []byte {
	return p.data
}

func (p *mockPage) GetLock() *sync.RWMutex {
	return p.RWMutex
}
