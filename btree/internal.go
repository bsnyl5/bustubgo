package btree

import "unsafe"

type internalPage struct {
	pageType     pageType
	lsn          int64
	maxSize      int64
	parentPageID int64
	pageID       int64
}

func rawCastInternal(bs []byte) *internalPage {
	return (*internalPage)(unsafe.Pointer(&bs))
}

type pageType int64

const (
	pageTypeInvalid = iota
	pageTypeLeaf
	pageTypeInternal
)
