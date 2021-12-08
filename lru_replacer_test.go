package buff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testReplacer(t *testing.T, r Replacer) {
	r.Unpin(1)
	r.Unpin(2)
	r.Unpin(3)
	r.Unpin(4)
	r.Unpin(5)
	r.Unpin(6)
	r.Unpin(1)
	assert.Equal(t, 6, r.Size())

	var (
		evictedFrame int
		evicted      bool
	)
	evictedFrame, evicted = r.Victim()
	assert.True(t, evicted)
	assert.Equal(t, 1, evictedFrame)

	evictedFrame, evicted = r.Victim()
	assert.True(t, evicted)
	assert.Equal(t, 2, evictedFrame)

	evictedFrame, evicted = r.Victim()
	assert.True(t, evicted)
	assert.Equal(t, 3, evictedFrame)

	r.Pin(3)
	r.Pin(4)

	assert.Equal(t, 2, r.Size())

	r.Unpin(4)

	evictedFrame, evicted = r.Victim()
	assert.True(t, evicted)
	assert.Equal(t, 5, evictedFrame)

	evictedFrame, evicted = r.Victim()
	assert.True(t, evicted)
	assert.Equal(t, 6, evictedFrame)

	evictedFrame, evicted = r.Victim()
	assert.True(t, evicted)
	assert.Equal(t, 4, evictedFrame)

}

func Test_LRUReplacer(t *testing.T) {
	r := NewLRUReplacer(7)
	testReplacer(t, r)
}
