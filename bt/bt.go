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
	size     int

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
	n := cur.searchLeafNode(t, key)
	leaf := &n.leafNode

	if leaf.size < nodesize {
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
		return nil
	}
	newRight, splitKey := leaf.splitNode()
	comp := compareKey(key, splitKey)
	switch comp {
	case -1:
		leaf.insertVal(treeVal{key: key, val: val})
	case 1:
		newRight.insertVal(treeVal{key: key, val: val})
	case 0:
		return fmt.Errorf("exact key value pair already exist")
	}

	// retrieve currentParent from cursor latest stack
	// if currentParent ==nil, create new currentParent(in this case current leaf is also the root node)
	var currentParent = n
	parLevel := 1
	orphan := &node{
		isLeafNode: true,
		leafNode:   *newRight,
	}

	for {
		// root node
		if len(cur.stack)-parLevel+1 == 0 {
			firstChild := currentParent
			currentParent = &node{
				level: parLevel,
				mu:    &sync.RWMutex{},
				size:  1,
			}
			t.root = currentParent
			currentParent.children[0] = firstChild
			currentParent.key[0] = splitKey
			currentParent._insertPointerAtIdx(1, orphan)
			break
		} else {
			currentParent = cur.stack[len(cur.stack)-parLevel]
		}

		idx, err := currentParent.findUniquePointerIdx(splitKey)
		if err != nil {
			return err
		}
		if currentParent.size < nodesize {
			currentParent._insertPointerAtIdx(idx, orphan)
			break
		}

		newParent, newSplitKey := currentParent.splitNode()
		parLevel++

		currentParent._insertPointerAtIdx(idx, orphan)

		// let the next iteration handle this split with new parent propagated up the stack
		orphan = newParent
		splitKey = newSplitKey

	}
	return nil
}

type cursor struct {
	stack []*node
}

// TODO: add some cursor to manage lock
func (c *cursor) searchLeafNode(t *btree, searchKey treeKey) *node {
	_assert(len(c.stack) == 0, "length of cursor is not cleaned up")
	var curNode = t.root
	for !curNode.isLeafNode {
		c.stack = append(c.stack, curNode)
		pointerIdx := curNode.findPointerIdx(searchKey)
		curNode = curNode.children[pointerIdx]
	}
	_assert(curNode.isLeafNode, "result of search leaf node return non leaf node")
	return curNode
}

func (n *node) _insertPointerAtIdx(idx int, child *node) {
	copy(n.children[idx+1:n.size+1], n.children[idx:n.size])
	n.children[idx] = child
	n.size++
}

func (n *node) splitNode() (*node, treeKey) {
	newNode := &node{
		mu:    &sync.RWMutex{},
		level: n.level,
	}
	idx := nodesize / 2 // right >= left
	copy(newNode.key[:n.size-idx], n.key[idx:n.size])
	copy(newNode.children[:n.size-idx], n.children[idx:n.size])
	newNode.size = n.size - idx
	n.size = idx

	splitKey := newNode.key[0]
	return newNode, splitKey
}

// splitKey returned to create new pointer entry on parent
func (n *leafNode) splitNode() (*leafNode, treeKey) {
	newLeaf := &leafNode{
		mu: &sync.RWMutex{},
		// data [nodesize]treeVal
		// size int
	}
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
	n.next = nil
	splitKey := newLeaf.data[0].key
	return newLeaf, splitKey
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
	foundIdx := sort.Search(n.size, func(curIdx int) bool {
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
	foundIdx := sort.Search(n.size, func(curIdx int) bool {
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
