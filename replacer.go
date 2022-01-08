package buff

import (
	lru "github.com/hashicorp/golang-lru"
)

type Replacer interface {
	// Remove the victim frame as defined by replace ment  policy
	// return evicted frameID if found
	Victim() (int, bool)

	// frameID should not be victimized until unpin
	Pin(frameID int)

	// allow frame to be victimizedable
	Unpin(frameID int)

	// items that can be victimized
	Size() int
}

type LRUReplacer struct {
	internal *lru.Cache
}

type ClockReplacer struct{}

// TODO
func NewLRUReplacer(numPage int) *LRUReplacer {
	c, err := lru.New(numPage)
	if err != nil {
		panic(err)
	}
	return &LRUReplacer{
		internal: c,
	}
}
func (r *LRUReplacer) Pin(frameID int) {
	r.internal.Remove(frameID)

}
func (r *LRUReplacer) Victim() (int, bool) {
	key, _, ok := r.internal.RemoveOldest()
	if !ok {
		return 0, false
	}
	return key.(int), ok
}

func (r *LRUReplacer) Unpin(frameID int) {
	r.internal.ContainsOrAdd(frameID, 1)
}
func (r *LRUReplacer) Size() int { return r.internal.Len() }
