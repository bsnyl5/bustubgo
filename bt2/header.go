package bt2

import (
	"sync"
	"unsafe"
)

func castHeaderPage(sl []byte) *headerPage {
	return (*headerPage)(unsafe.Pointer(&sl[0]))
}

func castBranchNode(nodeSize int, sl []byte, mu *sync.RWMutex) *branchNode {
	fixed := 24 //size(8) + keyT (16)
	p := (*mmapedBranchNode)(unsafe.Pointer(&sl[0]))
	var keys []keyT
	keysData := unsafeAdd(unsafe.Pointer(&sl[0]), unsafe.Sizeof(fixed))
	unsafeSlice(unsafe.Pointer(&keys), keysData, nodeSize)
	var children []nodeID
	childrenData := unsafeAdd(unsafe.Pointer(&sl[0]), unsafe.Sizeof(fixed)+uintptr(nodeSize)*16)
	unsafeSlice(unsafe.Pointer(&children), childrenData, nodeSize)
	return &branchNode{
		mu:               mu,
		mmapedBranchNode: p,
		keys:             keys,
		children:         children,
	}
}
func castLeafNode(nodeSize int, sl []byte, mu *sync.RWMutex) *leafNodeV2 {
	fixed := 16 //size(8) + keyT (8)
	p := (*mmapedLeafNode)(unsafe.Pointer(&sl[0]))
	var values []valT
	dataPointer := unsafeAdd(unsafe.Pointer(&sl[0]), unsafe.Sizeof(fixed))
	unsafeSlice(unsafe.Pointer(&values), dataPointer, nodeSize)
	return &leafNodeV2{
		mu:             mu,
		mmapedLeafNode: p,
		datas:          values,
	}
}

// TODO don't know what it does
const maxAllocSize = 0x7FFFFFFF

type headerPage struct {
	flags    int64
	rootPgid int64
}

type bnodeHeader struct {
	size int64
}

type mmapedBranchNode struct {
	size     int64
	highKey  keyT
	keys     []keyT // pointer|key|pointer|key
	children []nodeID
}
type branchNode struct {
	mu *sync.RWMutex
	*mmapedBranchNode

	keys     []keyT // pointer|key|pointer|key
	children []nodeID
}

type leafNodeV2 struct {
	mu *sync.RWMutex
	*mmapedLeafNode

	datas []valT
}

type mmapedLeafNode struct {
	size int64
	next nodeID
}

// valT=keyT for simplicity
type valT keyT

type keyT struct {
	main int64
	sub  int64
}
type nodeID int64

const (
	//TODO more flag
	headerFlagInit int64 = 1 << iota
)
