package buff

import (
	"fmt"
	"os"
	"sync"
)

type DiskManager struct {
	m *sync.Mutex
	f *os.File
}

const (
	PageSize = 4096
)

func NewDiskManager(filename string) *DiskManager {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	return &DiskManager{
		m: &sync.Mutex{},
		f: file,
	}
}

func (d *DiskManager) WritePage(pageID int64, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("buffer provided must have size %d", PageSize)
	}
	d.m.Lock()
	defer d.m.Unlock()
	offset := pageID * PageSize
	_, err := d.f.Seek(offset, 0)
	if err != nil {
		return err
	}
	written, err := d.f.Write(data)
	if err != nil {
		return err
	}
	if written != PageSize {
		return fmt.Errorf("expect writtent byte %d, has %d", PageSize, written)
	}
	err = d.f.Sync()
	return err
}

func (d *DiskManager) ReadPage(pageID int64, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("buffer provided must have size %d", PageSize)
	}
	d.m.Lock()
	defer d.m.Unlock()
	offset := pageID * PageSize
	_, err := d.f.Seek(offset, 0)
	if err != nil {
		return err
	}

	readBytes, err := d.f.Read(data[:PageSize])
	if err != nil {
		return err
	}
	if readBytes != PageSize {
		return fmt.Errorf("only read %d from file, expect %d", readBytes, PageSize)
	}
	return nil
}
