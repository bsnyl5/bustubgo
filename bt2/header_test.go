package bt2

import (
	"buff"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_castLeafPage(t *testing.T) {
	testFile := "./testdb"
	somePage := make([]byte, buff.PageSize)
	h := castLeafNode(10, somePage, &sync.RWMutex{})
	h.size = 9
	h.next = nodeID(7)
	assert.Len(t, h.datas, 10)
	seed := 10
	for i := 0; i < seed; i++ {
		h.datas[i] = valT{rand.Int63n(100), rand.Int63n(100)}
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

	h2 := castLeafNode(10, newBuf, &sync.RWMutex{})
	assert.Equal(t, h.size, h2.size)
	assert.Equal(t, h.next, h2.next)
	assert.Equal(t, h.datas, h2.datas)
}

func Test_castBranchPage(t *testing.T) {
	testFile := "./testdb"
	somePage := make([]byte, buff.PageSize)
	h := castBranchNode(10, somePage, &sync.RWMutex{})
	h.size = 9
	h.highKey = keyT{11, 1}
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

	h2 := castBranchNode(10, newBuf, &sync.RWMutex{})
	assert.Equal(t, h.size, h2.size)
	assert.Equal(t, h.highKey, h2.highKey)
	assert.Equal(t, h.keys, h2.keys)
	assert.Equal(t, h.children, h2.children)
}

func Test_castHeaderPage(t *testing.T) {
	testFile := "./testdb"
	somePage := make([]byte, buff.PageSize)
	h := castHeaderPage(somePage)
	h.flags = headerFlagInit
	h.rootPgid = 1
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
}
