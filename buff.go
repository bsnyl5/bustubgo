package buff

import (
	"container/list"
	"fmt"
	"sync"
)

type BufferPool struct {
	numInstances int
	size         int
	diskManager  *DiskManager
	pages        []Page
	pageTable    map[int]*Page
	replacer     Replacer
	freeList     *list.List
	// Page table map[page-id] frame id
	// Replacer
	mu          *sync.Mutex
	nextNewPage int
}

func NewBufferPool(size int, d *DiskManager) *BufferPool {
	pages := make([]Page, size)
	freeList := list.New()
	for idx := range pages {
		pages[idx].mu = &sync.RWMutex{}
		pages[idx].frameID = idx
		pages[idx].pageID = invalidPageID
		freeList.PushFront(idx)
	}
	return &BufferPool{
		size:         size,
		diskManager:  d,
		pages:        pages,
		replacer:     NewLRUReplacer(size),
		freeList:     freeList,
		mu:           &sync.Mutex{},
		pageTable:    map[int]*Page{},
		numInstances: 1,
	}
}
func (b *BufferPool) lockedAllocatePage() int {
	newpage := b.nextNewPage
	b.nextNewPage += b.numInstances
	return newpage
}

func (b *BufferPool) NewPage() *Page {
	var (
		pageID      int
		page        *Page
		unavailable bool
		victimed    bool
		freeFrame   int
	)
	locked(b.mu, func() {
		// new frame available
		if b.freeList.Len() != 0 {
			free := b.freeList.Front()
			b.freeList.Remove(free)
			freeFrame = free.Value.(int)
		} else {
			// try finding new frame from replacer
			frameID, ok := b.replacer.Victim()
			if !ok {
				unavailable = true
				return
			}
			freeFrame = frameID
			victimed = true
		}
		// b.replacer.Pin()
		page = &b.pages[freeFrame]
		if page.pageID != invalidPageID {
			delete(b.pageTable, page.pageID)
		}

		pageID = b.lockedAllocatePage()
		b.pageTable[pageID] = page
		// now we need to lock, releasing big lock, but still prevent
		// other thread that fetching this page from modifying it
		page.mu.Lock()

	})
	if unavailable {
		return nil
	}

	defer page.mu.Unlock()
	if victimed && page.dirty {
		err := b.diskManager.WritePage(int64(page.pageID), page.data)
		if err != nil {
			panic(fmt.Sprintf("todo: %s", err))
		}
	}

	page.assignNew(pageID, freeFrame)
	page.pin()
	return page
	// 0.   Make sure you call AllocatePage!
	// 1.   If all the pages in the buffer pool are pinned, return nullptr.
	// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
	// 3.   Update P's metadata, zero out memory and add P to the page table.
	// 4.   Set the page ID output parameter. Return a pointer to P.
}

func (b *BufferPool) FetchPage(pageID int) (*Page, error) {
	var (
		page        *Page
		unavailable bool
		inBuffer    bool
		victimed    bool
		freeFrame   int
	)
	locked(b.mu, func() {
		page = b.pageTable[pageID]
		// page already in buffer, pin++ and return
		if page != nil {
			locked(page.mu, func() {
				page.pinCount++
			})

			b.replacer.Pin(page.frameID)
			inBuffer = true
			return
		}
		// new frame available
		if b.freeList.Len() != 0 {
			free := b.freeList.Front()
			b.freeList.Remove(free)
			freeFrame = free.Value.(int)
		} else {
			// try finding new frame from replacer
			frameID, ok := b.replacer.Victim()
			if !ok {
				unavailable = true
				return
			}
			freeFrame = frameID
			victimed = true
		}
		// b.replacer.Pin()
		page = &b.pages[freeFrame]

		// don't need to use lock here, we are the only one using this free page
		if page.pageID != invalidPageID {
			delete(b.pageTable, page.pageID)
		}
		b.pageTable[pageID] = page
		// now we need to lock, releasing big lock, but still prevent
		// other thread that fetching this page from modifying it
		page.mu.Lock()

	})
	if unavailable {
		return nil, fmt.Errorf("buffer full")
	}
	if inBuffer {
		fmt.Printf("in buffer")
		return page, nil
	}
	defer page.mu.Unlock()
	if victimed && page.dirty {
		err := b.diskManager.WritePage(int64(page.pageID), page.data)
		if err != nil {
			panic(fmt.Sprintf("todo: %s", err))
		}
	}

	page.assignNew(pageID, freeFrame)
	err := b.diskManager.ReadPage(int64(pageID), page.data)
	if err != nil {
		panic(fmt.Sprintf("todo: %s", err))
	}
	page.pin()
	return page, nil
	// 1.     Search the page table for the requested page (P).
	// 1.1    If P exists, pin it and return it immediately.
	// 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
	//        Note that pages are always found from the free list first.
	// 2.     If R is dirty, write it back to the disk.
	// 3.     Delete R from the page table and insert P.
	// 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
}
func (b *BufferPool) DeletePage(pageID int) bool {
	var page *Page
	locked(b.mu, func() {
		page = b.pageTable[pageID]
		if page != nil {
			delete(b.pageTable, pageID)
		}
	})
	if page == nil {
		return true
	}
	var (
		pinCount int
		frameID  int
	)
	locked(page.mu, func() {
		pinCount = page.pinCount
		if pinCount == 0 {
			page.reset()
			frameID = page.frameID
			// page.refresh()

		}
	})
	if pinCount != 0 {
		return false
	}
	locked(b.mu, func() {
		b.freeList.PushFront(frameID)
	})

	return true
	// 0.   Make sure you call DeallocatePage!
	// 1.   Search the page table for the requested page (P).
	// 1.   If P does not exist, return true.
	// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
	// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
}

func (b *BufferPool) UnpinPage(pageID int, isDirty bool) bool {
	var page *Page
	locked(b.mu, func() {
		page = b.pageTable[pageID]
	})
	if page == nil {
		// not sure true or false
		return false
	}

	var (
		frameID int
		prevPin int
	)
	locked(page.mu, func() {
		prevPin = page.pinCount
		page.pinCount--
		frameID = page.frameID
		page.dirty = isDirty
	})
	if prevPin == 1 {
		b.replacer.Unpin(frameID)
	}
	return prevPin > 0
}

func (b *BufferPool) FlushPage(pageID int) {
	var (
		page *Page
	)
	locked(b.mu, func() {
		page = b.pageTable[pageID]
		// page already in buffer, pin++ and return
		if page != nil {
			page.mu.Lock()
			return
		}
	})
	if page == nil {
		return
	}
	defer page.mu.Unlock()
	err := b.diskManager.WritePage(int64(pageID), page.data)
	if err != nil {
		panic(err)
	}
}

const (
	invalidPageID = -1
)

type Page struct {
	frameID  int
	pageID   int
	pinCount int
	data     []byte
	dataSize int
	dirty    bool
	mu       *sync.RWMutex
}

func (p *Page) flushIfIsDirty() bool {
	var ret bool
	p.mu.Lock()
	ret = p.dirty
	p.mu.Unlock()
	return ret
}

func (p *Page) Write(data []byte) error {
	if len(data) > PageSize {
		return fmt.Errorf("cannot write more than page size %d", PageSize)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dataSize = copy(p.data, data)
	return nil
}

func (p *Page) GetData() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.data
}

func (p *Page) reset() {
	p.data = make([]byte, PageSize) //todo: reuse somehow
	p.dirty = false
	p.pageID = invalidPageID
}

func (p *Page) assignNew(pageID int, frameID int) {
	p.pageID = pageID
	p.frameID = frameID
	p.data = make([]byte, PageSize) //todo: reuse somehow
	p.dirty = false
}

func (p *Page) pin() {
	p.pinCount++
}

func locked(m sync.Locker, h func()) {
	m.Lock()
	defer m.Unlock()
	h()
}
