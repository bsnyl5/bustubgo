package bt

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeTreeVal(k []int) []treeVal {
	ret := make([]treeVal, 0, len(k))
	for _, item := range k {
		ret = append(ret, treeVal{
			key: treeKey{main: item},
			val: item,
		})
	}
	return ret
}

func makeTreeKey(k []int) []treeKey {
	ret := make([]treeKey, 0, len(k))
	for _, item := range k {
		ret = append(ret, treeKey{item, 0})
	}
	return ret
}

// test case 1,2,4, key = 4 => foundIDx = 2, result pointerIdx = 3
// test case 1,2,4,5 key = 4 => foundIDx = 2, result pointer idx = 3
// test case 1,2,4,5 key = 3 => foundIDx = 2, result pointerG idx = 2
func Test_searchIdx(t *testing.T) {
	type testcase struct {
		input     []treeKey
		searchKey treeKey
		expect    int
	}
	n := &node{
		mu: &sync.RWMutex{},
	}
	cases := []testcase{
		{
			input:     makeTreeKey([]int{1, 2, 4}),
			searchKey: treeKey{4, 0},
			expect:    3,
		},
		{

			input:     makeTreeKey([]int{1, 2, 4, 5}),
			searchKey: treeKey{4, 0},
			expect:    3,
		},
		{

			input:     makeTreeKey([]int{1, 2, 4, 5}),
			searchKey: treeKey{3, 0},
			expect:    2,
		},
		{

			input:     makeTreeKey([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			searchKey: treeKey{10, 0},
			expect:    10,
		},
	}
	for _, tcase := range cases {
		keys := tcase.input
		copy(n.key[:len(keys)], keys)
		n.keySize = len(keys)
		idx := n.findPointerIdx(tcase.searchKey)
		assert.Equal(t, tcase.expect, idx)
	}
}
func Test_btreeDelete(t *testing.T) {

	// cur.stack from root -> nearest parent
	type deleteTestCase struct {
		insertions  []int
		deletions   []int
		rootKeys    []treeKey
		leafKeyVals [][]treeVal
		nodesize    int
	}
	tcases := []deleteTestCase{
		{
			nodesize:   3,
			insertions: sequentialUntil(5),
			deletions:  []int{2},
			rootKeys:   makeTreeKey([]int{3, 4}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1}),
				makeTreeVal([]int{3}),
				makeTreeVal([]int{4, 5}),
			},
		},
		{
			nodesize:   3,
			insertions: []int{1, 2, 3},
			deletions:  []int{2, 1},
			rootKeys:   makeTreeKey([]int{3}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{3}),
			},
		},
		{
			nodesize:   3,
			insertions: []int{1, 2, 3},
			deletions:  []int{1},
			rootKeys:   makeTreeKey([]int{3}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{2}),
				makeTreeVal([]int{3}),
			},
		},
		{
			nodesize:   3,
			insertions: invertedSequentialUntil(10),
			deletions:  []int{10, 9, 8},
			rootKeys:   makeTreeKey([]int{5}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1, 2}),
				makeTreeVal([]int{3, 4}),
				makeTreeVal([]int{5, 6}),
				makeTreeVal([]int{7}),
			},
		},
		{
			nodesize:   3,
			insertions: sequentialUntil(8),
			deletions:  []int{4},
			rootKeys:   makeTreeKey([]int{3, 6}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1}),
				makeTreeVal([]int{2}),
				makeTreeVal([]int{3}),
				makeTreeVal([]int{5}),
				makeTreeVal([]int{6}),
				makeTreeVal([]int{7, 8}),
			},
		},
		{
			nodesize:   3,
			insertions: []int{1, 2, 3, 4, 5, 6},
			deletions:  []int{2},
			rootKeys:   makeTreeKey([]int{4}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1}),
				makeTreeVal([]int{3}),
				makeTreeVal([]int{4}),
				makeTreeVal([]int{5, 6}),
			},
		},
		{
			nodesize:   3,
			rootKeys:   makeTreeKey([]int{2}),
			insertions: []int{1, 2, 3, 4, 5},
			deletions:  []int{5, 4, 3},
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1}),
				makeTreeVal([]int{2}),
			},
		},
		{
			nodesize:   3,
			insertions: []int{1, 2, 3, 4, 5},
			deletions:  []int{5, 4, 3, 2},
			rootKeys:   makeTreeKey([]int{1}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1}),
			},
		},
	}
	for _, tc := range tcases {
		tr := NewBtree(tc.nodesize)
		for _, insertItem := range tc.insertions {
			assert.NoError(t, tr.insert(treeKey{main: insertItem}, insertItem))
		}
		for _, deleteItem := range tc.deletions {
			assert.NoError(t, tr.delete(treeKey{main: deleteItem}))
		}

		root := tr.root
		if root.isLeafNode {
			assert.Equal(t, tc.rootKeys, keysFromVals(root.leafNode.data[:root.leafNode.size]))
		} else {
			assert.Equal(t, tc.rootKeys, root.key[:root.keySize])
		}
		cur := cursor{}
		n := cur.searchLeafNode(tr, treeKey{main: -1})
		assert.NotNil(t, n)
		var (
			prev    *leafNode
			current = &n.node.leafNode
		)
		for idx := range tc.leafKeyVals {
			expectVals := tc.leafKeyVals[idx]
			if prev != nil {
				assert.Equal(t, prev, current.prev)
			}
			assert.Equal(t, expectVals, current.data[:current.size])
			assertNullVals(t, current.data[current.size:])
			prev = current
			current = current.next
		}
	}
}

func Test_btreeInsert(t *testing.T) {

	// cur.stack from root -> nearest parent
	type insertTestCase struct {
		insertions  []int
		rootKeys    []treeKey
		leafKeyVals [][]treeVal
		nodesize    int
	}
	tcases := []insertTestCase{
		{
			nodesize:   3,
			insertions: invertedSequentialUntil(10),
			rootKeys:   makeTreeKey([]int{7}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1, 2}),
				makeTreeVal([]int{3, 4}),
				makeTreeVal([]int{5, 6}),
				makeTreeVal([]int{7, 8}),
				makeTreeVal([]int{9, 10}),
			},
		},
		{
			nodesize:   3,
			insertions: []int{1, 2, 3, 4, 5, 6},
			rootKeys:   makeTreeKey([]int{3}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1}),
				makeTreeVal([]int{2}),
				makeTreeVal([]int{3}),
				makeTreeVal([]int{4}),
				makeTreeVal([]int{5, 6}),
			},
		},
		{
			nodesize:   4,
			insertions: []int{1, 3, 5, 9, 10},
			rootKeys:   makeTreeKey([]int{5}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1, 3}),
				makeTreeVal([]int{5, 9, 10}),
			},
		},
		{
			nodesize:   7,
			insertions: sequentialUntil(13),
			rootKeys:   makeTreeKey([]int{4, 7, 10}),
			leafKeyVals: [][]treeVal{
				makeTreeVal([]int{1, 2, 3}),
				makeTreeVal([]int{4, 5, 6}),
				makeTreeVal([]int{7, 8, 9}),
				makeTreeVal([]int{10, 11, 12, 13}),
			},
		},
	}
	for _, tc := range tcases {
		tr := NewBtree(tc.nodesize)
		for _, insertItem := range tc.insertions {
			assert.NoError(t, tr.insert(treeKey{main: insertItem}, insertItem))
		}
		root := tr.root
		assert.Equal(t, tc.rootKeys, root.key[:root.keySize])

		cur := cursor{}
		n := cur.searchLeafNode(tr, treeKey{main: -1})
		assert.NotNil(t, n)
		var (
			prev    *leafNode
			current = &n.node.leafNode
		)
		for idx := range tc.leafKeyVals {
			expectVals := tc.leafKeyVals[idx]
			if prev != nil {
				assert.Equal(t, prev, current.prev)
			}
			assert.Equal(t, expectVals, current.data[:current.size])
			assertNullVals(t, current.data[current.size:])
			prev = current
			current = current.next
		}
	}
}
func assertNullVals(t *testing.T, vals []treeVal) {
	for _, item := range vals {
		assert.Equal(t, treeVal{}, item)
	}
}

func invertedSequentialUntil(last int) []int {
	ks := make([]int, 0, last)
	for i := last; i > 0; i-- {
		ks = append(ks, i)
	}
	return ks
}

func sequentialUntil(last int) []int {
	ks := make([]int, 0, last)
	for i := 1; i < last+1; i++ {
		ks = append(ks, i)
	}
	return ks
}

func keysFromVals(vals []treeVal) (ks []treeKey) {
	for _, item := range vals {
		ks = append(ks, item.key)
	}
	return ks
}
