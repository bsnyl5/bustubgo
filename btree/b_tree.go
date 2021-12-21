package btree

import (
	"buff"
	"bufio"
	"errors"
	"io"
	"os"
)

type Tree struct {
	idxName         string
	rootPageID      int
	bpm             *buff.BufferPool
	comp            Comparator
	leafMaxSize     int
	internalMaxSize int
}

type Comparator func(lhs, fsh interface{}) bool

func NewTree(
	treename string,
	bpm *buff.BufferPool,
	comp Comparator,
	leafMaxSize,
	internalMaxSize int) *Tree {
	return &Tree{
		idxName:         treename,
		bpm:             bpm,
		comp:            comp,
		leafMaxSize:     leafMaxSize,
		internalMaxSize: internalMaxSize,
	}
}

// Used for test only
func (t *Tree) RemoveFromFile(filename string, tx *Tx) {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(f)
	for {
		newline, err := r.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			panic(err)
		}
		t.Remove(parseInt(newline), tx)
	}
}

func (t *Tree) Print(bpm *buff.BufferPool) {
	page, err := bpm.FetchPage(t.rootPageID)
	if err != nil {
		panic(err)
	}
	treeData := TreeDataFromBytes(page.GetData())
	treeData.ToString(bpm)
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
func (t *Tree) Insert(key int, rid rid, tx *Tx) {
	if t.rootPageID == -1 {
		// t.startNew()
		//
	}

}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
func (t *Tree) startNew(key int, rid rid) {
	// root := t.bpm.NewPage()
	// if root == nil {
	// 	panic("oom")
	// }
	// t.rootPageID = root.GetPageID()
	// rootPage := rawCastInternal(root.GetData())
}

//TODO
func (t *Tree) Remove(key int, tx *Tx) {

}

// Used for test only
func (t *Tree) InsertFromFile(filename string, tx *Tx) {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(f)
	for {
		newline, err := r.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			panic(err)
		}
		key := parseInt(newline)
		t.Insert(key, rid{
			pageID:  key,
			slotNum: key,
		}, tx)
	}
}

func NewTx(txid int) *Tx {
	return &Tx{
		state:            Growing,
		isolationLevel:   RepeatableRead,
		txID:             txid,
		prevLsn:          -1,
		sharedLockSet:    make(map[rid]struct{}),
		exclusiveLockSet: make(map[rid]struct{}),
	}
}

type Tx struct {
	state            TxState
	isolationLevel   IsolationLevel
	threadID         int
	txID             int
	prevLsn          int // log sequence
	sharedLockSet    map[rid]struct{}
	exclusiveLockSet map[rid]struct{}
}
type rid struct {
	pageID  int
	slotNum int
}
type TxState int
type IsolationLevel int

const (
	Growing TxState = iota
	Shrinking
	Committed
	Aborted

	ReadUnCommitted IsolationLevel = iota
	RepeatableRead
	ReadCommitted
)

type TreeData struct {
}

func (t *TreeData) ToString(bpm *buff.BufferPool) string {
	return ""

}

func TreeDataFromBytes(b []byte) TreeData {
	return TreeData{}
}
