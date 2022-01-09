package bt2

import (
	"buff"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_castLeafPage(t *testing.T) {
	var (
		size     int64  = 9
		nodeSize int    = 1
		next     nodeID = 7
	)
	testFile := "./testdb"
	somePage := make([]byte, buff.PageSize)
	h := castLeafFromEmpty(nodeSize, newMockPage(somePage))
	h.size = size
	h.next = next
	assert.Equal(t, nodeSize, len(h.datas))
	assert.Equal(t, nodeSize, cap(h.datas))
	seed := 1
	for i := 0; i < seed; i++ {
		key := keyT{rand.Int63n(100), rand.Int63n(100)}
		val := keyT{rand.Int63n(100), rand.Int63n(100)}
		h.datas[i] = valT{key, val}
	}
	// h.children
	assert.NoError(t, os.WriteFile(testFile, somePage, os.ModePerm))
	defer os.Remove(testFile)
	file, err := os.OpenFile(testFile, os.O_RDONLY, os.ModePerm)
	assert.NoError(t, err)
	newBuf := make([]byte, buff.PageSize)
	n, err := file.Read(newBuf)
	assert.NoError(t, err)
	assert.Equal(t, buff.PageSize, n)

	h2 := castGenericNode(nodeSize, newMockPage(newBuf))
	assert.Equal(t, nodeSize, len(h2.datas))
	assert.Equal(t, nodeSize, cap(h2.datas))
	assert.Equal(t, size, h2.size)
	assert.Equal(t, next, h2.next)
	assert.Equal(t, h.datas, h2.datas)
	assert.True(t, h2.isLeafNode)
	h2.datas[0] = valT{keyT{3, 3}, keyT{0, 0}}
	assert.Equal(t, h.level, h2.level)
}

func Test_castBranchPage(t *testing.T) {
	testFile := "./testdb"
	somePage := make([]byte, buff.PageSize)
	h := castBranchFromEmpty(10, newMockPage(somePage))
	h.size = 9
	assert.Len(t, h.keys, 10)
	assert.Len(t, h.children, 10)
	seed := 10
	for i := 0; i < seed; i++ {
		h.keys[i] = keyT{rand.Int63n(100), rand.Int63n(100)}
		h.children[i] = nodeID(rand.Int63n(100))
	}
	// h.children
	assert.NoError(t, os.WriteFile(testFile, somePage, os.ModePerm))
	defer os.Remove(testFile)
	file, err := os.OpenFile(testFile, os.O_RDONLY, os.ModePerm)
	assert.NoError(t, err)
	newBuf := make([]byte, buff.PageSize)
	n, err := file.Read(newBuf)
	assert.NoError(t, err)
	assert.Equal(t, buff.PageSize, n)

	h2 := castGenericNode(10, newMockPage(newBuf))
	assert.Equal(t, h.size, h2.size)
	assert.Equal(t, h.keys, h2.keys)
	assert.Equal(t, h.children, h2.children)
	assert.False(t, h2.isLeafNode)
}

func Test_castHeaderPage(t *testing.T) {
	testFile := "./testdb"
	somePage := make([]byte, buff.PageSize)
	h := castHeaderPage(somePage)
	h.flags = headerFlagInit
	h.rootPgid = 1
	h.nodeSize = 11
	assert.NoError(t, os.WriteFile(testFile, somePage, os.ModePerm))
	defer os.Remove(testFile)
	file, err := os.OpenFile(testFile, os.O_RDONLY, os.ModePerm)
	assert.NoError(t, err)
	newBuf := make([]byte, buff.PageSize)
	n, err := file.Read(newBuf)
	assert.NoError(t, err)
	assert.Equal(t, buff.PageSize, n)

	h2 := castHeaderPage(newBuf)
	assert.Equal(t, h.flags, h2.flags)
	assert.Equal(t, h.rootPgid, h2.rootPgid)
	assert.Equal(t, h.nodeSize, h2.nodeSize)
}
