package bt

import (
	"fmt"
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

func compareKey(k1, k2 treeKey) int {
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
	children []*node
	keySize  int

	leafNode   leafNode
	isLeafNode bool
}

type btree struct {
	root     *node
	nodesize int
	// leftmostLeafNode  *leafNode
	// rightmostLeafNode *leafNode
}

func NewBtree(nsize int) *btree {
	leaf := leafNode{
		mu:   &sync.RWMutex{},
		data: make([]treeVal, nsize),
	}
	return &btree{
		nodesize: nsize,
		root: &node{
			isLeafNode: true,
			leafNode:   leaf,
		},
	}
}

func _leafNodeRemove(n *leafNode, idx int) {
	copy(n.data[idx:n.size], n.data[idx+1:n.size])
	n.data[n.size-1] = treeVal{}
	n.size--
}

func (t *btree) delete(key treeKey) error {
	curs := cursor{}
	// cur.stack from root -> nearest parent
	n := curs.searchLeafNode(t, key)
	leaf := &n.leafNode

	// normal deletion
	{
		idx, exact := t.findIdxForKey(leaf, key)
		if !exact {
			return fmt.Errorf("key %v does not exist", key)
		}
		_leafNodeRemove(&n.leafNode, idx)
	}
	if n.leafNode.size < t.nodesize/2 {
		parInfo, ok := curs.popNext()
		thisNodeIdx := parInfo.idx
		// n has no parent which means n is a root+leaf node
		if !ok {
			return nil
		}
		par := parInfo.node
		// check if we can borrow from cousin
		done := t._tryBorrowLeafKey(par, thisNodeIdx, n)
		if done {
			return nil
		}

		// must merge with either previous or next cousins
		if thisNodeIdx > 0 {
			t.mergeLeafNodeRightToLeft(par, thisNodeIdx, par.children[thisNodeIdx-1], n)
		} else if thisNodeIdx < par.keySize {
			t.mergeLeafNodeRightToLeft(par, thisNodeIdx, n, par.children[thisNodeIdx+1])
		} else {
			_assert(false, "should not reach here")
		}

		var maybeNewRoot *node
		curBranch := par
		refIdx := parInfo.idx
		// for parInCursor, ok := curs.popNext(); ok && curBranch.keySize < t.nodesize/2; {
		for {
			parInCursor, ok := curs.popNext()
			if !ok {
				// no more parent, which means curBranch is root node
				if curBranch.keySize == 0 {
					t.root = maybeNewRoot
					return nil
				}
			}
			if curBranch.keySize >= t.nodesize/2 {
				return nil
			}
			// ok && curBranch.keySize < t.nodesize/2
			// parent of current branch
			newPar := parInCursor.node
			done := t._tryBorrowBranchKey(newPar, refIdx, curBranch)

			if done {
				return nil
			}

			if refIdx > 0 {
				t.mergeBranchNodeRightToLeft(newPar, refIdx, newPar.children[refIdx-1], curBranch)
				maybeNewRoot = newPar.children[refIdx-1]
			} else if refIdx < newPar.keySize {
				t.mergeBranchNodeRightToLeft(newPar, refIdx, curBranch, newPar.children[refIdx+1])
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

func (t *btree) _tryBorrowLeafKey(newPar *node, refIdx int, curBranch *node) bool {
	if refIdx > 0 {
		left := newPar.children[refIdx-1]
		if left.leafNode.size-1 >= t.nodesize/2 {
			// after borrow, parent nodesize stay the same, safe to return
			t.borrowLeftForRight(newPar, refIdx, left, curBranch)
			return true
		}
		// try borrow from prev cousin
	}
	if refIdx < newPar.keySize {
		right := newPar.children[refIdx+1]
		if right.leafNode.size-1 >= t.nodesize/2 {
			// after borrow, parent nodesize stay the same, safe to return
			t.borrowRightForLeft(newPar, refIdx, curBranch, right)
			return true
		}
		// try borrow from next cousin
	}
	return false
}

func (t *btree) _tryBorrowBranchKey(newPar *node, refIdx int, curBranch *node) bool {
	if refIdx > 0 {
		left := newPar.children[refIdx-1]
		if left.leafNode.size-1 >= t.nodesize/2 {
			// after borrow, parent nodesize stay the same, safe to return
			t.borrowLeftForRight(newPar, refIdx, left, curBranch)
			return true
		}
		// try borrow from prev cousin
	}
	if refIdx < newPar.keySize {
		right := newPar.children[refIdx+1]
		if right.leafNode.size-1 >= t.nodesize/2 {
			// after borrow, parent nodesize stay the same, safe to return
			t.borrowRightForLeft(newPar, refIdx, curBranch, right)
			return true
		}
		// try borrow from next cousin
	}

	return false
}

func (t *btree) borrowRightForLeft(par *node, splitIdx int, left, right *node) {
	// prepend current key to current parent
	splitKey := par.key[splitIdx]

	// n := copy(right.key[1:right.keySize+1], right.key[:right.keySize])
	left.key[left.keySize] = splitKey

	// bring right cousin first pointer to current parent last pointer
	left.children[left.keySize+1] = right.children[0]
	left.keySize++
	leftFirstKey := left.key[0]

	// shrink right cousin to the left
	copy(right.key[:right.keySize-1], right.key[1:right.keySize])
	right.key[right.keySize] = treeKey{}
	copy(right.children[:right.keySize], right.children[1:right.keySize+1])
	right.children[right.keySize+1] = nil
	right.keySize--

	// new split key = right cousin (old) first key
	par.key[splitIdx] = leftFirstKey
}

func (t *btree) leafBorrowRightForLeft(par *node, splitIdx int, left, right *node) {
	// prepend current key to current parent
	splitKey := par.key[splitIdx]

	// n := copy(right.key[1:right.keySize+1], right.key[:right.keySize])
	left.key[left.keySize] = splitKey

	// bring right cousin first pointer to current parent last pointer
	left.children[left.keySize+1] = right.children[0]
	left.keySize++
	leftFirstKey := left.key[0]

	// shrink right cousin to the left
	copy(right.key[:right.keySize-1], right.key[1:right.keySize])
	right.key[right.keySize] = treeKey{}
	copy(right.children[:right.keySize], right.children[1:right.keySize+1])
	right.children[right.keySize+1] = nil
	right.keySize--

	// new split key = right cousin (old) first key
	par.key[splitIdx] = leftFirstKey
}

func (t *btree) leafBorrowLeftForRight(par *node, splitIdx int, left, right *node) {
	// prepend current key to current parent
	splitKey := par.key[splitIdx]

	n := copy(right.key[1:right.keySize+1], right.key[:right.keySize])
	_assert(n == right.keySize, "copy key failed")
	right.key[0] = splitKey

	n = copy(right.children[1:right.keySize+1], right.children[:right.keySize+1])
	_assert(n == right.keySize+1, "copy key failed")
	right.keySize++

	// bring left cousin last pointer to current parent first pointer
	right.children[0] = left.children[left.keySize+1]
	// delete left cousin last pointer (n+1), last key (n)
	left.children[left.keySize+1] = nil
	lastKey := left.key[left.keySize]
	left.key[left.keySize] = treeKey{}
	left.keySize--

	// replace parent entry with the value of last key
	par.key[splitIdx] = lastKey
}

// TODO: make direction generic
func (t *btree) borrowLeftForRight(par *node, splitIdx int, left, right *node) {
	// prepend current key to current parent
	splitKey := par.key[splitIdx]

	n := copy(right.key[1:right.keySize+1], right.key[:right.keySize])
	_assert(n == right.keySize, "copy key failed")
	right.key[0] = splitKey

	n = copy(right.children[1:right.keySize+1], right.children[:right.keySize+1])
	_assert(n == right.keySize+1, "copy key failed")
	right.keySize++

	// bring left cousin last pointer to current parent first pointer
	right.children[0] = left.children[left.keySize+1]
	// delete left cousin last pointer (n+1), last key (n)
	left.children[left.keySize+1] = nil
	lastKey := left.key[left.keySize]
	left.key[left.keySize] = treeKey{}
	left.keySize--

	// replace parent entry with the value of last key
	par.key[splitIdx] = lastKey
}

func (t *btree) mergeLeafNodeRightToLeft(par *node, childSplitIdx int, left, right *node) {
	keySplitIdx := childSplitIdx - 1
	// left values + right values
	high, low := left.leafNode.size, left.leafNode.size+right.leafNode.size
	n := copy(left.leafNode.data[high:low], right.leafNode.data[:right.leafNode.size])
	_assert(n == right.leafNode.size, "copy failed")
	left.leafNode.size += right.leafNode.size

	copy(par.key[keySplitIdx:par.keySize-1], par.key[keySplitIdx+1:par.keySize])
	copy(par.children[childSplitIdx:par.keySize], par.children[childSplitIdx+1:par.keySize+1])
	// empty last key because of shrink
	par.key[par.keySize-1] = treeKey{}
	par.children[par.keySize] = nil
	par.keySize--
	left.leafNode.next = right.leafNode.next
}

// TODO: write test
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
func (t *btree) mergeBranchNodeRightToLeft(par *node, parentRefIdx int, left, right *node) {
	// left pointers + right pointers
	high, low := left.keySize+1, left.keySize+1+right.keySize+1
	n := copy(left.children[high:low], right.children[:right.keySize+1])
	_assert(n == right.keySize+1, "copy failed")

	toDeletedKeyIdx := parentRefIdx - 1
	toDeletedChildIdx := parentRefIdx
	splitKey := par.key[toDeletedKeyIdx]

	// left keys + split keys + right keys
	left.key[left.keySize] = splitKey
	high, low = left.keySize+1, left.keySize+1+right.keySize
	n = copy(left.key[high:low], right.key[:right.keySize])
	_assert(n == right.keySize, "copy failed")
	left.keySize += right.keySize + 1

	// delete pointer from parent to the right node by shrinking left
	copy(par.key[toDeletedKeyIdx:par.keySize-1], par.key[toDeletedKeyIdx+1:par.keySize])
	// empty last key because of shrink
	par.key[par.keySize-1] = treeKey{}
	copy(par.children[toDeletedChildIdx:par.keySize], par.children[toDeletedChildIdx+1:par.keySize+1])
	// empty last children because of shrink
	par.children[par.keySize] = nil
	par.keySize--
}

func (t *btree) insert(key treeKey, val int) error {
	cur := cursor{}
	// cur.stack from root -> nearest parent
	n := cur.searchLeafNode(t, key)
	leaf := &n.leafNode

	// normal insertion
	{
		idx, exact := t.findIdxForKey(leaf, key)
		if exact {
			return fmt.Errorf("duplicate key found %v", key)
		}
		copy(leaf.data[idx+1:leaf.size+1], leaf.data[idx:leaf.size])
		leaf.data[idx] = treeVal{
			val: val,
			key: key,
		}
		leaf.size++
	}

	if leaf.size < t.nodesize {
		return nil
	}
	orphan, splitKey := t.splitNode(leaf)

	// retrieve currentParent from cursor latest stack
	// if currentParent ==nil, create new currentParent(in this case current leaf is also the root node)
	var currentParent *node

	for len(cur.stack) > 0 {
		curStack := cur.stack[len(cur.stack)-1]
		currentParent = curStack.node
		cur.stack = cur.stack[:len(cur.stack)-1]

		idx, err := currentParent.findUniquePointerIdx(splitKey)
		if err != nil {
			return err
		}

		currentParent._insertPointerAtIdx(idx, &orphanNode{
			key:        splitKey,
			rightChild: orphan,
		})
		if currentParent.keySize < t.nodesize {
			return nil
		}
		// this parent is also full

		newOrphan, newSplitKey := t.splitBranchNode(currentParent)

		// let the next iteration handle this split with new parent propagated up the stack
		orphan = newOrphan
		splitKey = newSplitKey
	}
	// if reach this line, the higest level parent (root) has been recently split
	firstChild := t.root
	newRoot := &node{
		level:    t.root.level + 1,
		mu:       &sync.RWMutex{},
		keySize:  0,
		children: make([]*node, t.nodesize+1),
		key:      make([]treeKey, t.nodesize),
	}
	newRoot.children[0] = firstChild
	// newRoot.key[0] = splitKey
	newRoot._insertPointerAtIdx(0, &orphanNode{
		key:        splitKey,
		rightChild: orphan,
	})
	t.root = newRoot
	return nil
}

type cursor struct {
	stack []cursorInfo
}
type cursorInfo struct {
	node *node
	idx  int // idx at which parent references this node
}

func (c *cursor) popNext() (cursorInfo, bool) {
	if len(c.stack) == 0 {
		return cursorInfo{}, false
	}
	ret := c.stack[len(c.stack)-1]
	c.stack = c.stack[:len(c.stack)-1]
	return ret, true
}

// TODO: add some cursor to manage lock
func (c *cursor) searchLeafNode(t *btree, searchKey treeKey) *node {
	_assert(len(c.stack) == 0, "length of cursor is not cleaned up")
	var curNode = t.root
	curLevel := t.root.level
	var pointerIdx int
	for !curNode.isLeafNode {
		_assert(curLevel > 0, "reached level 0 node but still have not found leaf node")
		c.stack = append(c.stack, cursorInfo{
			node: curNode,
			idx:  pointerIdx,
		})
		pointerIdx = curNode.findPointerIdx(searchKey)
		if curNode.children[pointerIdx] != nil {
			curNode = curNode.children[pointerIdx]
			curLevel--
			continue
		}
		panic(fmt.Sprintf("cannot find correct node for key %v", searchKey))
	}
	return curNode
}

type orphanNode struct {
	rightChild *node
	key        treeKey
}

func (n *node) _insertPointerAtIdx(idx int, orphan *orphanNode) {
	copy(n.children[idx+2:n.keySize+2], n.children[idx+1:n.keySize+1])
	copy(n.key[idx+1:n.keySize+1], n.key[idx:n.keySize])
	n.children[idx+1] = orphan.rightChild
	n.key[idx] = orphan.key
	n.keySize++
}

func (t *btree) splitBranchNode(n *node) (*node, treeKey) {
	newLeftNode := &node{
		mu:       &sync.RWMutex{},
		level:    n.level,
		key:      make([]treeKey, t.nodesize),
		children: make([]*node, t.nodesize+1),
	}
	splitIdx := t.nodesize / 2 // right >= left
	splitKey := n.key[splitIdx]
	copy(newLeftNode.key[:n.keySize-splitIdx-1], n.key[splitIdx+1:n.keySize])
	copy(newLeftNode.children[:n.keySize-splitIdx], n.children[splitIdx+1:n.keySize+1])
	newLeftNode.keySize = n.keySize - splitIdx - 1
	for i := splitIdx; i < n.keySize; i++ {
		n.key[i] = treeKey{}
	}

	// left child will hold more children pointer
	// p|1|p|2|p|3|p => p|1|p + (splitkey=2) p|3|p
	for i := splitIdx + 1; i < n.keySize+1; i++ {
		n.children[i] = nil
	}

	n.keySize = splitIdx
	return newLeftNode, splitKey
}

// splitKey returned to create new pointer entry on parent
func (t *btree) splitNode(n *leafNode) (*node, treeKey) {
	newnode := &node{
		isLeafNode: true,
		leafNode: leafNode{
			mu:   &sync.RWMutex{},
			data: make([]treeVal, t.nodesize),
		},
	}
	newLeaf := &newnode.leafNode

	idx := t.nodesize / 2 // right >= left
	copy(newLeaf.data[:n.size-idx], n.data[idx:n.size])
	// hacky empty values
	for i := idx; i < n.size; i++ {
		n.data[i] = treeVal{}
	}
	newLeaf.size = n.size - idx
	n.size = idx

	// re assign neighbor leaves if any
	if n.next != nil {
		n.next.prev = newLeaf
	}

	newLeaf.next = n.next
	n.next = newLeaf
	newLeaf.prev = n
	splitKey := newLeaf.data[0].key
	return newnode, splitKey
}

func (t *btree) insertVal(n *leafNode, val treeVal) error {
	idx, exact := t.findIdxForKey(n, val.key)
	if exact {
		return fmt.Errorf("exact key has already exist")
	}
	copy(n.data[idx+1:n.size+1], n.data[idx:n.size])
	n.data[idx] = val
	n.size++
	return nil
}

func (t *btree) findIdxForKey(n *leafNode, newKey treeKey) (int, bool) {
	_assert(n.size < t.nodesize, "findingIdx for new value is meaning less when the leaf node is full")
	var (
		exact bool
	)
	foundIdx := sort.Search(n.size, func(curIdx int) bool {
		curKey := n.data[curIdx]
		comp := compareKey(curKey.key, newKey)
		if comp == 0 {
			exact = true
		}
		return comp == 0 || comp == 1
	})
	return foundIdx, exact
}

func (n *node) findUniquePointerIdx(searchKey treeKey) (int, error) {
	var (
		exactmatch bool
	)
	foundIdx := sort.Search(n.keySize, func(curIdx int) bool {
		curKey := n.key[curIdx]
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
func (n *node) findPointerIdx(searchKey treeKey) int {
	var (
		exactmatch bool
	)
	foundIdx := sort.Search(n.keySize, func(curIdx int) bool {
		curKey := n.key[curIdx]
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
