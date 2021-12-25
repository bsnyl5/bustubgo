package bt

import (
	"fmt"
	"sort"
	"sync"
)

const (
	nodesize = 10
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
	data [nodesize]treeVal
	size int
}

type node struct {
	level    int
	mu       *sync.RWMutex
	key      [nodesize]treeKey // pointer|key|pointer|key|pointer
	children [nodesize + 1]*node
	keySize  int

	leafNode   leafNode
	isLeafNode bool
}

type btree struct {
	root *node
	// leftmostLeafNode  *leafNode
	// rightmostLeafNode *leafNode
}

func NewBtree() *btree {
	leaf := leafNode{
		mu: &sync.RWMutex{},
	}
	return &btree{
		root: &node{
			isLeafNode: true,
			leafNode:   leaf,
		},
	}
}

func (t *btree) insert(key treeKey, val int) error {
	cur := cursor{}
	// cur.stack from root -> nearest parent
	n := cur.searchLeafNode(t, key)
	leaf := &n.leafNode

	// normal insertion
	{
		idx, exact := leaf.findIdxForKey(key)
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

	if leaf.size < nodesize {
		return nil
	}
	// 	idx, exact := leaf.findIdxForKey(key)
	// 	if exact {
	// 		return fmt.Errorf("duplicate key found %v", key)
	// 	}
	// 	copy(leaf.data[idx+1:leaf.size+1], leaf.data[idx:leaf.size])
	// 	leaf.data[idx] = treeVal{
	// 		val: val,
	// 		key: key,
	// 	}
	// 	leaf.size++
	// 	return nil
	// }
	orphan, splitKey := leaf.splitNode()
	// comp := compareKey(key, splitKey)
	// switch comp {
	// case -1:
	// 	leaf.insertVal(treeVal{key: key, val: val})
	// case 1:
	// 	newRight.insertVal(treeVal{key: key, val: val})
	// case 0:
	// 	return fmt.Errorf("exact key value pair already exist")
	// }

	// retrieve currentParent from cursor latest stack
	// if currentParent ==nil, create new currentParent(in this case current leaf is also the root node)
	var currentParent *node

	for len(cur.stack) > 0 {
		currentParent = cur.stack[len(cur.stack)-1]
		cur.stack = cur.stack[:len(cur.stack)-1]

		idx, err := currentParent.findUniquePointerIdx(splitKey)
		if err != nil {
			return err
		}

		currentParent._insertPointerAtIdx(idx, &orphanNode{
			key:        splitKey,
			rightChild: orphan,
		})
		if currentParent.keySize < nodesize {
			return nil
		}
		// this parent is also full

		newOrphan, newSplitKey := currentParent.splitNode()

		// let the next iteration handle this split with new parent propagated up the stack
		orphan = newOrphan
		splitKey = newSplitKey
	}
	// if reach this line, the higest level parent (root) has been recently split
	firstChild := t.root
	newRoot := &node{
		level:   t.root.level + 1,
		mu:      &sync.RWMutex{},
		keySize: 0,
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
	stack []*node
}

// TODO: add some cursor to manage lock
func (c *cursor) searchLeafNode(t *btree, searchKey treeKey) *node {
	_assert(len(c.stack) == 0, "length of cursor is not cleaned up")
	var curNode = t.root
	curLevel := t.root.level
	for !curNode.isLeafNode {
		_assert(curLevel > 0, "reached level 0 node but still have not found leaf node")
		c.stack = append(c.stack, curNode)
		pointerIdx := curNode.findPointerIdx(searchKey)
		curNode = curNode.children[pointerIdx]
		curLevel--
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

func (n *node) splitNode() (*node, treeKey) {
	newNode := &node{
		mu:    &sync.RWMutex{},
		level: n.level,
	}
	idx := nodesize / 2 // right >= left
	copy(newNode.key[:n.keySize-idx], n.key[idx:n.keySize])
	copy(newNode.children[1:n.keySize-idx], n.children[idx+1:n.keySize])
	newNode.keySize = n.keySize - idx
	n.keySize = idx
	for i := idx; i < n.keySize; i++ {
		n.key[i] = treeKey{}
	}
	// left child will hold more children pointer
	// p|1|p|2|p|3|p => p|1|p + p|2|p|3|p
	for i := idx + 1; i < n.keySize; i++ {
		n.children[i] = nil
	}

	splitKey := newNode.key[0]
	return newNode, splitKey
}

// splitKey returned to create new pointer entry on parent
func (n *leafNode) splitNode() (*node, treeKey) {
	newnode := &node{
		isLeafNode: true,
		leafNode: leafNode{
			mu: &sync.RWMutex{},
		},
	}
	newLeaf := &newnode.leafNode

	idx := nodesize / 2 // right >= left
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

func (n *leafNode) insertVal(val treeVal) error {
	idx, exact := n.findIdxForKey(val.key)
	if exact {
		return fmt.Errorf("exact key has already exist")
	}
	copy(n.data[idx+1:n.size+1], n.data[idx:n.size])
	n.data[idx] = val
	n.size++
	return nil
}

func (n *leafNode) findIdxForKey(newKey treeKey) (int, bool) {
	_assert(n.size < nodesize, "findingIdx for new value is meaning less when the leaf node is full")
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
