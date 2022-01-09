package bt2

import (
	"sync"
	"unsafe"
)

func castHeaderPage(sl []byte) *headerPage {
	h := (*headerPage)(unsafe.Pointer(&sl[0]))
	return h
}

// pageData is mmap
func castLeafFromEmpty(nodeSize int, page Page) *genericNode {
	pageData := page.GetData()
	fixed := unsafe.Sizeof(pageHeader{})
	p := (*pageHeader)(unsafe.Pointer(&pageData[0]))

	var values []valT
	dataPointer := unsafeAdd(unsafe.Pointer(p), fixed)
	unsafeSlice(unsafe.Pointer(&values), dataPointer, nodeSize)
	// for sure
	p.isLeafNode = true
	return &genericNode{
		mu:         page.GetLock(),
		pageHeader: p,
		leafData: &leafData{
			datas: values,
		},
		osPage: page,
	}
}

// pageData is mmap
func castBranchFromEmpty(nodeSize int, page Page) *genericNode {
	pageData := page.GetData()
	fixed := unsafe.Sizeof(pageHeader{})
	p := (*pageHeader)(unsafe.Pointer(&pageData[0]))

	var values []valT
	dataPointer := unsafeAdd(unsafe.Pointer(p), fixed)
	unsafeSlice(unsafe.Pointer(&values), dataPointer, nodeSize)
	// for sure
	p.isLeafNode = false
	var keys []keyT
	keysData := unsafeAdd(unsafe.Pointer(p), fixed)
	unsafeSlice(unsafe.Pointer(&keys), keysData, nodeSize)
	var children []nodeID
	childrenData := unsafeAdd(unsafe.Pointer(p), fixed+uintptr(nodeSize)*16)
	unsafeSlice(unsafe.Pointer(&children), childrenData, nodeSize)
	return &genericNode{
		mu:         page.GetLock(),
		pageHeader: p,
		branchData: &branchData{
			keys:     keys,
			children: children,
		},
		osPage: page,
	}
}

// pageData is mmap
func castGenericNode(nodeSize int, page Page) *genericNode {
	pageData := page.GetData()
	fixed := unsafe.Sizeof(pageHeader{})
	p := (*pageHeader)(unsafe.Pointer(&pageData[0]))

	if p.isLeafNode {
		var values []valT
		dataPointer := unsafeAdd(unsafe.Pointer(p), fixed)
		unsafeSlice(unsafe.Pointer(&values), dataPointer, nodeSize)
		return &genericNode{
			mu:         page.GetLock(),
			pageHeader: p,
			osPage:     page,
			leafData: &leafData{
				datas: values,
			},
		}
	}
	var keys []keyT
	keysData := unsafeAdd(unsafe.Pointer(p), fixed)
	unsafeSlice(unsafe.Pointer(&keys), keysData, nodeSize)
	var children []nodeID
	childrenData := unsafeAdd(unsafe.Pointer(p), fixed+uintptr(nodeSize)*16)
	unsafeSlice(unsafe.Pointer(&children), childrenData, nodeSize)
	return &genericNode{
		mu:         page.GetLock(),
		pageHeader: p,
		osPage:     page,
		branchData: &branchData{
			keys:     keys,
			children: children,
		},
	}
}

// TODO don't know what it does
const maxAllocSize = 0x7FFFFFFF

type headerPage struct {
	flags     int64
	rootPgid  nodeID
	nodeSize  int64
	_padding1 [7]byte
}

type bnodeHeader struct {
	size int64
}

type pageHeader struct {
	isDeleted  bool
	isLeafNode bool
	_padding2  [6]byte
	level      int64
	size       int64
	next       nodeID
}
type genericNode struct {
	mu     *sync.RWMutex
	osPage Page // reference back to the OS/buffer pool page
	*pageHeader
	*branchData
	*leafData
}
type branchData struct {
	keys     []keyT // pointer|key|pointer|key
	children []nodeID
}
type leafData struct {
	datas []valT
}

// valT=keyT for simplicity
type valT struct {
	key keyT
	val keyT
}

type keyT struct {
	main int64
	sub  int64
}
type nodeID int64

const (
	//TODO more flag
	headerFlagInit int64 = 1 << iota

	invalidID nodeID = -1
)
