package bt

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeTreeVal(k []int, defaultVal int) []treeVal {
	ret := make([]treeVal, 0, len(k))
	for _, item := range k {
		ret = append(ret, treeVal{
			key: treeKey{main: item},
			val: defaultVal,
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
		n.size = len(keys)
		idx := n.findPointerIdx(tcase.searchKey)
		assert.Equal(t, tcase.expect, idx)
	}
}

func Test_btreeInsert(t *testing.T) {
	tr := NewBtree()
	ks := make([]int, 0, 11)
	for i := 0; i < 11; i++ {
		ks = append(ks, i)
	}
	keys := makeTreeKey(ks)
	for _, item := range keys {
		assert.NoError(t, tr.insert(item, 1))
	}
	root := tr.root
	assert.False(t, root.isLeafNode)
	assert.Equal(t, 2, root.size)

	leftChild := root.children[0]
	rightChild := root.children[1]
	assert.Equal(t, treeKey{main: 5}, root.key[0])
	leftData := leftChild.leafNode.data
	rightData := rightChild.leafNode.data
	assert.Equal(t, leftData[:leftChild.leafNode.size], makeTreeVal([]int{0, 1, 2, 3, 4}, 1))
	assert.Equal(t, rightData[:rightChild.leafNode.size], makeTreeVal([]int{5, 6, 7, 8, 9, 10}, 1))
}
