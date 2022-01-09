package bt2

import (
	"buff"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

type treeVal struct {
	key treeKey
	val int
}
type treeKey struct {
	main int
	sub  int // for duplicate key, inject a value to make it unique
}

func compareKey(k1, k2 keyT) int {
	if k1.main != k2.main {
		ret := -1
		if k1.main > k2.main {
			ret = 1
		}
		return ret
	}
	ret := 0
	switch {
	case k1.sub > k2.sub:
		ret = 1
	case k1.sub < k2.sub:
		ret = -1
	}
	return ret
}

type leafNode struct {
	mu   *sync.RWMutex
	prev *leafNode
	next *leafNode
	data []treeVal
	size int
}

type node struct {
	level    int
	mu       *sync.RWMutex
	key      []treeKey // pointer|key|pointer|key|pointer
	children []*genericNode
	size     int

	leafNode   leafNode
	isLeafNode bool
}

type btreeCursor struct {
	bpm            *buff.BufferPool
	_header        *headerPage
	headerPageLock *sync.RWMutex
}

func NewBtree(filepath string, nsize int64) *btreeCursor {
	disk := buff.NewDiskManager(filepath)
	bpm := buff.NewBufferPool(30, disk)
	header, err := bpm.FetchPage(0)
	if err != nil {
		if errors.Is(err, io.EOF) {
			header = bpm.NewPage()
			if header == nil {
				panic("either fetch page(0) or new page failed")
			}
			if header.GetPageID() != 0 {
				panic("page id of first new page call is not 0")
			}
		} else {
			panic(err)
		}
	}
	h := castHeaderPage(header.GetData())
	if h.flags&headerFlagInit == 0 {
		h.flags ^= headerFlagInit
		h.nodeSize = nsize
		rootpage := bpm.NewPage()
		if rootpage == nil {
			panic("cannot create root page")
		}
		castLeafFromEmpty(int(h.nodeSize), rootpage)
		h.rootPgid = nodeID(rootpage.GetPageID())
		bpm.FlushPage(0)
		bpm.FlushPage(int(h.rootPgid))
		bpm.UnpinPage(int(h.rootPgid), false)
	}
	return &btreeCursor{
		bpm:            bpm,
		_header:        h,
		headerPageLock: header.GetLock(),
	}
}

func _leafNodeRemove(n *genericNode, idx int) {
	copy(n.datas[idx:n.size], n.datas[idx+1:n.size])
	n.datas[n.size-1] = valT{}
	n.size--
}
func (t *tx) addFlush(pageID nodeID) {
	t.tobeFlushed = append(t.tobeFlushed, pageID)
}
func (t *tx) addUnpin(pageID nodeID) {
	t.tobeCleaned = append(t.tobeCleaned, pageID)
}
func (t *tx) unpinPages(bpm *buff.BufferPool) {
	for _, pageID := range t.tobeCleaned {
		bpm.UnpinPage(int(pageID), true)
	}
}

func (t *btreeCursor) delete(key keyT) error {
	curs := tx{}
	// cur.stack from root -> nearest parent
	err := curs.searchLeafNode(t, key)
	if err != nil {
		return fmt.Errorf("searchLeafNode error: %v", err)
	}
	defer curs.unpinPages(t.bpm)

	breadCrumb, ok := curs.popNext()
	if !ok {
		return fmt.Errorf("no reached")
	}
	n := breadCrumb.node

	// normal deletion
	idx, exact := t.leafNodeFindKeySlot(n, key)
	if !exact {
		return fmt.Errorf("key %v does not exist", key)
	}
	_leafNodeRemove(n, idx)
	if n.size < t._header.nodeSize/2 {
		parInfo, ok := curs.popNext()
		thisNodeIdx := breadCrumb.idx
		// n has no parent which means n is a root+leaf node
		if !ok {
			return nil
		}
		par := parInfo.node
		// check if we can borrow from cousin
		done, err := t._tryBorrowLeafKey(par, thisNodeIdx, n)
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		var maybeNewRoot *genericNode
		// must merge with either previous or next cousins
		if thisNodeIdx > 0 {
			leftPageID := par.children[thisNodeIdx-1]
			leftPage, err := t.getGenericNode(leftPageID)
			if err != nil {
				return err
			}
			t.mergeLeafNodeRightToLeft(par, thisNodeIdx, leftPage, n)
			curs.addUnpin(leftPageID)
			maybeNewRoot = leftPage
		} else if thisNodeIdx < int(par.size) {
			rightPageID := par.children[thisNodeIdx-1]
			rightPage, err := t.getGenericNode(rightPageID)
			if err != nil {
				return err
			}
			t.mergeLeafNodeRightToLeft(par, thisNodeIdx+1, n, rightPage)
			maybeNewRoot = n
		} else {
			_assert(false, "should not reach here")
		}

		curBranch := par
		refIdx := parInfo.idx
		// for parInCursor, ok := curs.popNext(); ok && curBranch.size < t._header.nodeSize/2; {
		for {
			parInCursor, ok := curs.popNext()
			if !ok {
				// no more parent, which means curBranch is root node
				if curBranch.size == 0 {
					t._header.rootPgid = nodeID(maybeNewRoot.osPage.GetPageID())
					curs.addFlush(0)
					return nil
				}
			}
			if curBranch.size >= t._header.nodeSize/2 {
				return nil
			}
			// ok && curBranch.size < t._header.nodeSize/2
			// parent of current branch
			newPar := parInCursor.node
			done, err := t._tryBorrowBranchKey(&curs, newPar, refIdx, curBranch)
			if err != nil {
				return err
			}

			if done {
				return nil
			}

			if refIdx > 0 {
				leftPageID := par.children[thisNodeIdx-1]
				leftPage, err := t.getGenericNode(leftPageID)
				if err != nil {
					return err
				}
				t.mergeBranchNodeRightToLeft(newPar, refIdx, leftPage, curBranch)
				maybeNewRoot = leftPage
			} else if refIdx < int(newPar.size) {
				rightPageID := par.children[thisNodeIdx-1]
				rightPage, err := t.getGenericNode(rightPageID)
				if err != nil {
					return err
				}

				t.mergeBranchNodeRightToLeft(newPar, refIdx+1, curBranch, rightPage)
				maybeNewRoot = curBranch
			} else {
				_assert(false, "should not reach here")
			}
			// this parent may have be less than half full, continue
			curBranch = newPar
			refIdx = parInCursor.idx
		}
		// reaching this code means that curBranch is a root node
	}
	return nil
}

func (t *btreeCursor) _tryBorrowLeafKey(newPar *genericNode, refIdx int, curBranch *genericNode) (bool, error) {
	if refIdx > 0 {
		leftPageID := newPar.children[refIdx-1]
		left, err := t.getGenericNode(leftPageID)
		if err != nil {
			return false, err
		}
		if left.size > t._header.nodeSize/2 {
			// after borrow, parent nodesize stay the same, safe to return
			t.leafBorrowLeftForRight(newPar, refIdx, left, curBranch)
			t.bpm.UnpinPage(int(leftPageID), true)
			return true, nil
		}
		// try borrow from prev cousin
	}
	if refIdx < int(newPar.size) {
		rightPageID := newPar.children[refIdx+1]
		right, err := t.getGenericNode(rightPageID)
		if err != nil {
			return false, err
		}
		if right.size > t._header.nodeSize/2 {
			// after borrow, parent nodesize stay the same, safe to return
			t.leafBorrowRightForLeft(newPar, refIdx, curBranch, right)
			t.bpm.UnpinPage(int(rightPageID), true)
			return true, nil
		}
		// try borrow from next cousin
	}
	return false, nil
}

func (t *btreeCursor) _tryBorrowBranchKey(tx *tx, newPar *genericNode, refIdx int, curBranch *genericNode) (bool, error) {
	if refIdx > 0 {
		leftPageID := newPar.children[refIdx-1]
		left, err := t.getGenericNode(leftPageID)
		if err != nil {
			return false, err
		}
		tx.addUnpin(leftPageID)
		if left.size > t._header.nodeSize/2 {
			// after borrow, parent nodesize stay the same, safe to return
			t.borrowLeftForRight(newPar, refIdx, left, curBranch)
			return true, nil
		}
		// try borrow from prev cousin
	}
	if refIdx < int(newPar.size) {
		rightPageID := newPar.children[refIdx+1]
		right, err := t.getGenericNode(rightPageID)
		if err != nil {
			return false, err
		}
		tx.addUnpin(rightPageID)
		if right.size > t._header.nodeSize/2 {
			// after borrow, parent nodesize stay the same, safe to return
			t.borrowRightForLeft(newPar, refIdx, curBranch, right)
			return true, nil
		}
		// try borrow from next cousin
	}

	return false, nil
}

func (t *btreeCursor) borrowRightForLeft(par *genericNode, leftIdx int, left, right *genericNode) {
	// prepend current key to current parent
	splitKey := par.keys[leftIdx]

	// n := copy(right.keys[1:right.size+1], right.keys[:right.size])
	left.keys[left.size] = splitKey

	// bring right cousin first pointer to current parent last pointer
	left.children[left.size+1] = right.children[0]
	left.size++
	rightFirstKey := right.keys[0]

	// shrink right cousin to the left
	copy(right.keys[:right.size-1], right.keys[1:right.size])
	right.keys[right.size-1] = keyT{}
	copy(right.children[:right.size], right.children[1:right.size+1])
	right.children[right.size] = invalidID
	right.size--

	// new split key = right cousin (old) first key
	par.keys[leftIdx] = rightFirstKey
}

func (t *btreeCursor) leafBorrowRightForLeft(par *genericNode, leftIdx int, left, right *genericNode) {
	// prepend current key to current parent

	rightFirstKey := right.datas[0]

	// transfer right's first data to left
	left.datas[left.size] = rightFirstKey

	// bring right cousin first pointer to current parent last pointer
	// left.children[left.size+1] = right.children[0]
	left.size++

	// shrink right cousin to the left
	copy(right.datas[:right.size-1], right.datas[1:right.size])
	right.datas[right.size-1] = valT{}
	// copy(right.children[:right.size], right.children[1:right.size+1])
	// right.children[right.size+1] = nil
	right.size--
	newRightFirstKey := right.datas[0]

	// new split key = right cousin (old) first key
	par.keys[leftIdx] = newRightFirstKey.key
}

func (t *btreeCursor) leafBorrowLeftForRight(par *genericNode, rightIdx int, left, right *genericNode) {
	// prepend current key to current parent
	leftLastKey := left.datas[left.size-1]

	n := copy(right.datas[1:right.size+1], right.datas[:right.size])
	_assert(n == int(right.size), "copy key failed")
	right.datas[0] = leftLastKey

	right.size++

	left.datas[left.size-1] = valT{}
	left.size--

	// replace parent entry with the value of last key
	par.keys[rightIdx-1] = right.datas[0].key
}

// TODO: make direction generic
func (t *btreeCursor) borrowLeftForRight(par *genericNode, rightIdx int, left, right *genericNode) {
	// prepend current key to current parent
	splitKey := par.keys[rightIdx-1]

	n := copy(right.keys[1:right.size+1], right.keys[:right.size])
	_assert(n == int(right.size), "copy key failed")
	right.keys[0] = splitKey

	n = copy(right.children[1:right.size+2], right.children[:right.size+1])
	_assert(n == int(right.size)+1, "copy key failed")
	right.size++

	// bring left cousin last pointer to current parent first pointer
	right.children[0] = left.children[left.size]
	// delete left cousin last pointer (n+1), last key (n)
	left.children[left.size] = invalidID
	lastKey := left.keys[left.size-1]
	left.keys[left.size-1] = keyT{}
	left.size--

	// replace parent entry with the value of last key
	par.keys[rightIdx-1] = lastKey
}

func (t *btreeCursor) mergeLeafNodeRightToLeft(par *genericNode, rightPointerIdx int, left, right *genericNode) {
	keySplitIdx := rightPointerIdx - 1
	// left values + right values
	high, low := left.size, left.size+right.size
	n := copy(left.datas[high:low], right.datas[:right.size])
	_assert(n == int(right.size), "copy failed")
	left.size += right.size

	copy(par.keys[keySplitIdx:par.size-1], par.keys[keySplitIdx+1:par.size])
	copy(par.children[rightPointerIdx:par.size], par.children[rightPointerIdx+1:par.size+1])
	// empty last key because of shrink
	par.keys[par.size-1] = keyT{}
	par.children[par.size] = invalidID
	par.size--
	left.next = right.next

	deleted := t.bpm.DeletePage(right.osPage.GetPageID())
	if !deleted {
		// some other thread is using this deleted page, should detect when
		panic("delete page failed")
	}
}

// 			root:3
// 		|  				\
// 		2(2 children) 	4(2 children)
// 		|	\			|	\
// 		1 	2			3	4
//
// => delete 4
// 		-----------------
// 			root:3
// 		|  				\
// 		2(2 children) 	null(1 children)
// 		|	\			|
// 		1 	2			3
// 		-----------------
// 			root:2            3
// 		|  				|	 	 		|
// 		1 				2				3
func (t *btreeCursor) mergeBranchNodeRightToLeft(par *genericNode, rightPointerIdx int, left, right *genericNode) {
	// left pointers + right pointers
	high, low := left.size+1, left.size+1+right.size+1
	n := copy(left.children[high:low], right.children[:right.size+1])
	_assert(n == int(right.size)+1, "copy failed")

	toDeletedKeyIdx := rightPointerIdx - 1
	toDeletedChildIdx := rightPointerIdx
	splitKey := par.keys[toDeletedKeyIdx]

	// left keys + split keys + right keys
	left.keys[left.size] = splitKey
	high, low = left.size+1, left.size+1+right.size
	n = copy(left.keys[high:low], right.keys[:right.size])
	_assert(n == int(right.size), "copy failed")
	left.size += right.size + 1

	// delete pointer from parent to the right node by shrinking left
	copy(par.keys[toDeletedKeyIdx:par.size-1], par.keys[toDeletedKeyIdx+1:par.size])
	// empty last key because of shrink
	par.keys[par.size-1] = keyT{}
	copy(par.children[toDeletedChildIdx:par.size], par.children[toDeletedChildIdx+1:par.size+1])
	// empty last children because of shrink
	par.children[par.size] = invalidID
	par.size--
	deleted := t.bpm.DeletePage(right.osPage.GetPageID())
	if !deleted {
		panic("delete branch page failed")
	}
}

func (t *btreeCursor) insert(key keyT, val int64) error {
	tx := tx{}
	// cur.stack from root -> nearest parent
	err := tx.searchLeafNode(t, key)
	if err != nil {
		return err
	}
	defer tx.unpinPages(t.bpm)
	breadCrumb, ok := tx.popNext()
	if !ok {
		panic("not reached")
	}
	n := breadCrumb.node

	// normal insertion
	{
		idx, exact := t.leafNodeFindKeySlot(n, key)
		if exact {
			return fmt.Errorf("duplicate key found %v", key)
		}
		copy(n.datas[idx+1:n.size+1], n.datas[idx:n.size])
		n.datas[idx] = valT{
			val: keyT{main: int64(val)},
			key: key,
		}
		n.size++
	}

	if n.size < t._header.nodeSize {
		return nil
	}
	orphan, splitKey, err := t.splitLeafNode(&tx, n)
	if err != nil {
		return err
	}

	// retrieve currentParent from cursor latest stack
	// if currentParent ==nil, create new currentParent(in this case current leaf is also the root node)
	var currentParent *genericNode

	for len(tx.breadCrumbs) > 0 {
		curStack := tx.breadCrumbs[len(tx.breadCrumbs)-1]
		currentParent = curStack.node
		tx.breadCrumbs = tx.breadCrumbs[:len(tx.breadCrumbs)-1]

		idx, err := currentParent.findUniquePointerIdx(splitKey)
		if err != nil {
			return err
		}

		currentParent._insertPointerAtIdx(idx, &orphanNode{
			key:        splitKey,
			rightChild: orphan,
		})
		if currentParent.size < t._header.nodeSize {
			return nil
		}
		// this parent is also full

		newOrphan, newSplitKey, err := t.splitBranchNode(&tx, currentParent)
		if err != nil {
			return err
		}

		// let the next iteration handle this split with new parent propagated up the stack
		orphan = newOrphan
		splitKey = newSplitKey
	}
	// if reach this line, the higest level parent (root) has been recently split
	root, err := t.getRootNode()
	if err != nil {
		return err
	}

	newLevel := root.level + 1

	newRoot, err := t.newEmptyBranchNode()
	if err != nil {
		return err
	}
	tx.addUnpin(nodeID(newRoot.osPage.GetPageID()))
	newRoot.level = newLevel
	newRoot.children[0] = nodeID(root.osPage.GetPageID())
	newRoot._insertPointerAtIdx(0, &orphanNode{
		key:        splitKey,
		rightChild: orphan,
	})
	t._header.rootPgid = nodeID(newRoot.osPage.GetPageID())

	// headerPage Updated, flush instead of unpin (header page is always pinned)
	tx.addFlush(0)
	return nil
}

func (t *btreeCursor) newEmptyLeafNode() (*genericNode, error) {
	page := t.bpm.NewPage()
	if page == nil {
		return nil, fmt.Errorf("buffer full")
	}
	newLeaf := castLeafFromEmpty(int(t._header.nodeSize), page)
	return newLeaf, nil
}

func (t *btreeCursor) newEmptyBranchNode() (*genericNode, error) {
	page := t.bpm.NewPage()
	if page == nil {
		return nil, fmt.Errorf("buffer full")
	}
	newBranch := castBranchFromEmpty(int(t._header.nodeSize), page)
	return newBranch, nil
}

type tx struct {
	breadCrumbs []breadCrumb
	tobeCleaned []nodeID
	tobeFlushed []nodeID
}
type breadCrumb struct {
	node *genericNode
	idx  int // idx at which parent references this node
}

func (c *tx) popNext() (breadCrumb, bool) {
	if len(c.breadCrumbs) == 0 {
		return breadCrumb{}, false
	}
	ret := c.breadCrumbs[len(c.breadCrumbs)-1]
	c.breadCrumbs = c.breadCrumbs[:len(c.breadCrumbs)-1]
	c.addUnpin(nodeID(ret.node.osPage.GetPageID()))
	return ret, true
}

func (t *btreeCursor) getGenericNode(pageID nodeID) (*genericNode, error) {
	page, err := t.bpm.FetchPage(int(pageID))
	if err != nil {
		return nil, err
	}
	node := castGenericNode(int(t._header.nodeSize), page)
	return node, nil
}

func (t *btreeCursor) getRootNode() (*genericNode, error) {
	rootPage, err := t.bpm.FetchPage(int(t._header.rootPgid))
	if err != nil {
		return nil, err
	}
	node := castGenericNode(int(t._header.nodeSize), rootPage)
	return node, nil
}

// TODO: add some cursor to manage lock
func (c *tx) searchLeafNode(t *btreeCursor, searchKey keyT) error {
	_assert(len(c.breadCrumbs) == 0, "length of cursor is not cleaned up")
	root, err := t.getRootNode()
	if err != nil {
		return fmt.Errorf("failed to get root node: %v", err)
	}
	var curNode = root
	curLevel := root.level
	var pointerIdx int
	for !curNode.isLeafNode {
		_assert(curLevel > 0, "reached level 0 node but still have not found leaf node")
		c.breadCrumbs = append(c.breadCrumbs, breadCrumb{
			node: curNode,
			idx:  pointerIdx,
		})
		pointerIdx = curNode.branchNodeFindPointerIdx(searchKey)
		if curNode.children[pointerIdx] != invalidID {
			nextNodePageID := curNode.children[pointerIdx]
			curNode, err = t.getGenericNode(nextNodePageID)
			if err != nil {
				return err
			}
			curLevel--
			continue
		}
		panic(fmt.Sprintf("cannot find correct node for key %v", searchKey))
	}
	c.breadCrumbs = append(c.breadCrumbs, breadCrumb{
		node: curNode,
		idx:  pointerIdx,
	})
	return nil
}

type orphanNode struct {
	rightChild *genericNode
	key        keyT
}

//TODO: refactor this function
func (n *genericNode) _insertPointerAtIdx(idx int, orphan *orphanNode) {
	if n.size > int64(idx) {
		copy(n.children[idx+2:n.size+1], n.children[idx+1:n.size+1])
	}
	copy(n.keys[idx+1:n.size+1], n.keys[idx:n.size])
	n.children[idx+1] = nodeID(orphan.rightChild.osPage.GetPageID())
	n.keys[idx] = orphan.key
	n.size++
}

func (t *btreeCursor) splitBranchNode(tx *tx, n *genericNode) (*genericNode, keyT, error) {
	newLeftNode, err := t.newEmptyBranchNode()
	if err != nil {
		return nil, keyT{}, err
	}
	tx.addUnpin(nodeID(newLeftNode.osPage.GetPageID()))
	newLeftNode.level = n.level
	splitIdx := t._header.nodeSize / 2 // right >= left
	splitKey := n.keys[splitIdx]
	copy(newLeftNode.keys[:n.size-splitIdx-1], n.keys[splitIdx+1:n.size])
	copy(newLeftNode.children[:n.size-splitIdx], n.children[splitIdx+1:n.size+1])
	newLeftNode.size = n.size - splitIdx - 1
	for i := splitIdx; i < n.size; i++ {
		n.keys[i] = keyT{}
	}

	// left child will hold more children pointer
	// p|1|p|2|p|3|p => p|1|p + (splitkey=2) p|3|p
	for i := splitIdx + 1; i < n.size+1; i++ {
		n.children[i] = invalidID
	}

	n.size = splitIdx
	return newLeftNode, splitKey, nil
}

// splitKey returned to create new pointer entry on parent
func (t *btreeCursor) splitLeafNode(tx *tx, n *genericNode) (*genericNode, keyT, error) {
	newLeaf, err := t.newEmptyLeafNode()
	if err != nil {
		return nil, keyT{}, err
	}
	tx.addUnpin(nodeID(newLeaf.osPage.GetPageID()))

	idx := t._header.nodeSize / 2 // right >= left
	copy(newLeaf.datas[:n.size-idx], n.datas[idx:n.size])
	// hacky empty values
	for i := idx; i < n.size; i++ {
		n.datas[i] = valT{}
	}
	newLeaf.size = n.size - idx
	n.size = idx

	newLeaf.next = n.next
	n.next = nodeID(newLeaf.osPage.GetPageID())
	splitKey := newLeaf.datas[0].key
	return newLeaf, splitKey, nil
}

// func (t *btreeCursor) insertVal(n *leafNode, val treeVal) error {
// 	idx, exact := t.leafNodeFindKeySlot(n, val.key)
// 	if exact {
// 		return fmt.Errorf("exact key has already exist")
// 	}
// 	copy(n.data[idx+1:n.size+1], n.data[idx:n.size])
// 	n.data[idx] = val
// 	n.size++
// 	return nil
// }

func (t *btreeCursor) leafNodeFindKeySlot(n *genericNode, newKey keyT) (int, bool) {
	_assert(n.size < t._header.nodeSize, "findingIdx for new value is meaning less when the leaf node is full")
	var (
		exact bool
	)
	foundIdx := sort.Search(int(n.size), func(curIdx int) bool {
		curTuple := n.datas[curIdx]
		comp := compareKey(curTuple.key, newKey)
		if comp == 0 {
			exact = true
		}
		return comp == 0 || comp == 1
	})
	return foundIdx, exact
}

func (n *genericNode) findUniquePointerIdx(searchKey keyT) (int, error) {
	var (
		exactmatch bool
	)
	foundIdx := sort.Search(int(n.size), func(curIdx int) bool {
		curKey := n.keys[curIdx]
		comp := compareKey(curKey, searchKey)
		if comp == 0 {
			exactmatch = true
		}
		return comp == 0 || comp == 1
	})

	if exactmatch {
		return 0, fmt.Errorf("found existing pointer with key %v", searchKey)
	}
	return foundIdx, nil
}

// only apply to branch node
func (n *genericNode) branchNodeFindPointerIdx(searchKey keyT) int {
	var (
		exactmatch bool
	)
	foundIdx := sort.Search(int(n.size), func(curIdx int) bool {
		curKey := n.keys[curIdx]
		comp := compareKey(curKey, searchKey)
		if comp == 0 {
			exactmatch = true
		}
		return comp == 0 || comp == 1
	})

	if exactmatch {
		foundIdx = foundIdx + 1
	}
	return foundIdx
}

func _assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}
