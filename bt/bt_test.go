package bt

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeTreeKey(k []int) []treeKey {
	ret := make([]treeKey, 0, len(k))
	for _, item := range k {
		ret = append(ret, treeKey{item, 0})
	}
	return ret
}

// test case 1,2,4, key = 4 => foundIDx = 2, result pointerIdx = 3
// test case 1,2,4,5 key = 4 => foundIDx = 2, result pointer idx = 3
// test case 1,2,4,5 key = 3 => foundIDx = 2, result pointer idx = 2
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
