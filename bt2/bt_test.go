package bt2

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeTreeVal(k []int64) []valT {
	ret := make([]valT, 0, len(k))
	for _, item := range k {
		ret = append(ret, valT{
			key: keyT{main: item},
			val: keyT{main: item},
		})
	}
	return ret
}

func makeTreeKey(k []int64) []keyT {
	ret := make([]keyT, 0, len(k))
	for _, item := range k {
		ret = append(ret, keyT{item, 0})
	}
	return ret
}

// test case 1,2,4, key = 4 => foundIDx = 2, result pointerIdx = 3
// test case 1,2,4,5 key = 4 => foundIDx = 2, result pointer idx = 3
// test case 1,2,4,5 key = 3 => foundIDx = 2, result pointerG idx = 2
func Test_searchIdx(t *testing.T) {
	type testcase struct {
		input     []keyT
		searchKey keyT
		expect    int
	}

	buf := make([]byte, 4096)
	n := castBranchFromEmpty(10, newMockPage(buf))
	cases := []testcase{
		{
			input:     makeTreeKey([]int64{1, 2, 4}),
			searchKey: keyT{4, 0},
			expect:    3,
		},
		{

			input:     makeTreeKey([]int64{1, 2, 4, 5}),
			searchKey: keyT{4, 0},
			expect:    3,
		},
		{

			input:     makeTreeKey([]int64{1, 2, 4, 5}),
			searchKey: keyT{3, 0},
			expect:    2,
		},
		{

			input:     makeTreeKey([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			searchKey: keyT{10, 0},
			expect:    10,
		},
	}
	for _, tcase := range cases {
		keys := tcase.input
		copy(n.keys[:len(keys)], keys)
		n.size = int64(len(keys))
		idx := n.branchNodeFindPointerIdx(tcase.searchKey)
		assert.Equal(t, tcase.expect, idx)
	}
}
func Test_NewBtree(t *testing.T) {
	var nodeSize int64 = 10
	file := fmt.Sprintf("test-%s.db", t.Name())
	defer os.Remove(file)
	tr := newBtree(t, file, nodeSize)
	tr.bpm.Close()
}

func newBtree(t *testing.T, filename string, nsize int64) *btreeCursor {
	tr := NewBtree(filename, nsize)
	assert.Equal(t, headerFlagInit, tr._header.flags&headerFlagInit)
	assert.Equal(t, nodeID(1), tr._header.rootPgid)
	assert.Equal(t, nsize, tr._header.nodeSize)
	return tr
}

// func Test_btreeDelete(t *testing.T) {

// 	// cur.stack from root -> nearest parent
// 	type deleteTestCase struct {
// 		insertions  []int
// 		deletions   []int
// 		rootKeys    []keyT
// 		leafKeyVals [][]valT
// 		nodesize    int
// 	}
// 	tcases := []deleteTestCase{
// 		{
// 			nodesize:   3,
// 			insertions: sequentialUntil(5),
// 			deletions:  []int{2},
// 			rootKeys:   makeTreeKey([]int{3, 4}),
// 			leafKeyVals: [][]valT{
// 				makeTreeVal([]int{1}),
// 				makeTreeVal([]int{3}),
// 				makeTreeVal([]int{4, 5}),
// 			},
// 		},
// 		{
// 			nodesize:   3,
// 			insertions: []int{1, 2, 3},
// 			deletions:  []int{2, 1},
// 			rootKeys:   makeTreeKey([]int{3}),
// 			leafKeyVals: [][]valT{
// 				makeTreeVal([]int{3}),
// 			},
// 		},
// 		{
// 			nodesize:   3,
// 			insertions: []int{1, 2, 3},
// 			deletions:  []int{1},
// 			rootKeys:   makeTreeKey([]int{3}),
// 			leafKeyVals: [][]valT{
// 				makeTreeVal([]int{2}),
// 				makeTreeVal([]int{3}),
// 			},
// 		},
// 		{
// 			nodesize:   3,
// 			insertions: invertedSequentialUntil(10),
// 			deletions:  []int{10, 9, 8},
// 			rootKeys:   makeTreeKey([]int{5}),
// 			leafKeyVals: [][]valT{
// 				makeTreeVal([]int{1, 2}),
// 				makeTreeVal([]int{3, 4}),
// 				makeTreeVal([]int{5, 6}),
// 				makeTreeVal([]int{7}),
// 			},
// 		},
// 		{
// 			nodesize:   3,
// 			insertions: sequentialUntil(8),
// 			deletions:  []int{4},
// 			rootKeys:   makeTreeKey([]int{3, 6}),
// 			leafKeyVals: [][]valT{
// 				makeTreeVal([]int{1}),
// 				makeTreeVal([]int{2}),
// 				makeTreeVal([]int{3}),
// 				makeTreeVal([]int{5}),
// 				makeTreeVal([]int{6}),
// 				makeTreeVal([]int{7, 8}),
// 			},
// 		},
// 		{
// 			nodesize:   3,
// 			insertions: []int{1, 2, 3, 4, 5, 6},
// 			deletions:  []int{2},
// 			rootKeys:   makeTreeKey([]int{4}),
// 			leafKeyVals: [][]valT{
// 				makeTreeVal([]int{1}),
// 				makeTreeVal([]int{3}),
// 				makeTreeVal([]int{4}),
// 				makeTreeVal([]int{5, 6}),
// 			},
// 		},
// 		{
// 			nodesize:   3,
// 			rootKeys:   makeTreeKey([]int{2}),
// 			insertions: []int{1, 2, 3, 4, 5},
// 			deletions:  []int{5, 4, 3},
// 			leafKeyVals: [][]valT{
// 				makeTreeVal([]int{1}),
// 				makeTreeVal([]int{2}),
// 			},
// 		},
// 		{
// 			nodesize:   3,
// 			insertions: []int{1, 2, 3, 4, 5},
// 			deletions:  []int{5, 4, 3, 2},
// 			rootKeys:   makeTreeKey([]int{1}),
// 			leafKeyVals: [][]valT{
// 				makeTreeVal([]int{1}),
// 			},
// 		},
// 	}
// 	for _, tc := range tcases {
// 		tr := NewBtree(tc.nodesize)
// 		for _, insertItem := range tc.insertions {
// 			assert.NoError(t, tr.insert(keyT{main: insertItem}, insertItem))
// 		}
// 		for _, deleteItem := range tc.deletions {
// 			assert.NoError(t, tr.delete(keyT{main: deleteItem}))
// 		}

// 		root := tr.root
// 		if root.isLeafNode {
// 			assert.Equal(t, tc.rootKeys, keysFromVals(root.leafNode.data[:root.leafNode.size]))
// 		} else {
// 			assert.Equal(t, tc.rootKeys, root.key[:root.keySize])
// 		}
// 		cur := tx{}
// 		n := cur.searchLeafNode(tr, keyT{main: -1})
// 		assert.NotNil(t, n)
// 		var (
// 			prev    *leafNode
// 			current = &n.node.leafNode
// 		)
// 		for idx := range tc.leafKeyVals {
// 			expectVals := tc.leafKeyVals[idx]
// 			if prev != nil {
// 				assert.Equal(t, prev, current.prev)
// 			}
// 			assert.Equal(t, expectVals, current.data[:current.size])
// 			assertNullVals(t, current.data[current.size:])
// 			prev = current
// 			current = current.next
// 		}
// 	}
// }

func Test_btreeSimpleInsert(t *testing.T) {
	file := fmt.Sprintf("test-%s.db", t.Name())
	file = strings.ReplaceAll(file, string(filepath.Separator), "_")
	tr := newBtree(t, file, 10)
	defer func() {
		tr.bpm.Close()
		os.Remove(file)
	}()
	assert.NoError(t, tr.insert(keyT{main: 1}, 1))
}

func Test_btreeInsert(t *testing.T) {

	// cur.stack from root -> nearest parent
	type insertTestCase struct {
		insertions  []int64
		rootKeys    []keyT
		leafKeyVals [][]valT
		nodesize    int64
	}
	tcases := []insertTestCase{
		{
			nodesize:   3,
			insertions: invertedSequentialUntil(3),
			rootKeys:   makeTreeKey([]int64{2}),
			leafKeyVals: [][]valT{
				makeTreeVal([]int64{1}),
				makeTreeVal([]int64{2, 3}),
			},
		},
		// {
		// 	nodesize:   3,
		// 	insertions: invertedSequentialUntil(10),
		// 	rootKeys:   makeTreeKey([]int64{7}),
		// 	leafKeyVals: [][]valT{
		// 		makeTreeVal([]int64{1, 2}),
		// 		makeTreeVal([]int64{3, 4}),
		// 		makeTreeVal([]int64{5, 6}),
		// 		makeTreeVal([]int64{7, 8}),
		// 		makeTreeVal([]int64{9, 10}),
		// 	},
		// },
		// {
		// 	nodesize:   3,
		// 	insertions: []int64{1, 2, 3, 4, 5, 6},
		// 	rootKeys:   makeTreeKey([]int64{3}),
		// 	leafKeyVals: [][]valT{
		// 		makeTreeVal([]int64{1}),
		// 		makeTreeVal([]int64{2}),
		// 		makeTreeVal([]int64{3}),
		// 		makeTreeVal([]int64{4}),
		// 		makeTreeVal([]int64{5, 6}),
		// 	},
		// },
		// {
		// 	nodesize:   4,
		// 	insertions: []int64{1, 3, 5, 9, 10},
		// 	rootKeys:   makeTreeKey([]int64{5}),
		// 	leafKeyVals: [][]valT{
		// 		makeTreeVal([]int64{1, 3}),
		// 		makeTreeVal([]int64{5, 9, 10}),
		// 	},
		// },
		// {
		// 	nodesize:   7,
		// 	insertions: sequentialUntil(13),
		// 	rootKeys:   makeTreeKey([]int64{4, 7, 10}),
		// 	leafKeyVals: [][]valT{
		// 		makeTreeVal([]int64{1, 2, 3}),
		// 		makeTreeVal([]int64{4, 5, 6}),
		// 		makeTreeVal([]int64{7, 8, 9}),
		// 		makeTreeVal([]int64{10, 11, 12, 13}),
		// 	},
		// },
	}
	for idx, tc := range tcases {
		t.Run(fmt.Sprintf("insert %d", idx), func(t *testing.T) {
			file := fmt.Sprintf("test-%s.db", t.Name())
			file = strings.ReplaceAll(file, string(filepath.Separator), "_")
			tr := newBtree(t, file, tc.nodesize)
			defer func() {
				tr.bpm.Close()
				os.Remove(file)
			}()
			for _, insertItem := range tc.insertions {
				assert.NoError(t, tr.insert(keyT{main: insertItem}, insertItem))
			}
			root, err := tr.getRootNode()
			assert.NoError(t, err)
			assert.NotNil(t, root.branchData)
			assert.Equal(t, tc.rootKeys, root.keys[:root.size])

			cur := tx{}
			// search left most leaf node
			err = cur.searchLeafNode(tr, keyT{main: -1})
			assert.NoError(t, err)
			leftmost, bool := cur.popNext()
			assert.True(t, bool)
			var (
				current = leftmost.node
			)
			for idx := range tc.leafKeyVals {
				expectVals := tc.leafKeyVals[idx]
				assert.Equal(t, expectVals, current.datas[:current.size])
				assertNullVals(t, current.datas[current.size:])
				current, err = tr.getGenericNode(current.next)
				assert.NoError(t, err)
			}
		})

	}
}
func assertNullVals(t *testing.T, vals []valT) {
	for _, item := range vals {
		assert.Equal(t, valT{}, item)
	}
}

func invertedSequentialUntil(last int64) []int64 {
	ks := make([]int64, 0, last)
	for i := last; i > 0; i-- {
		ks = append(ks, i)
	}
	return ks
}

func sequentialUntil(last int64) []int64 {
	ks := make([]int64, 0, last)
	for i := int64(1); i < last+1; i++ {
		ks = append(ks, i)
	}
	return ks
}

func keysFromVals(vals []valT) (ks []keyT) {
	for _, item := range vals {
		ks = append(ks, item.key)
	}
	return ks
}
