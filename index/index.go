package index

import (
	"bitcask/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	Btree IndexType = iota + 1

	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupport index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(bi btree.Item) bool {
	return bytes.Compare(item.key, bi.(*Item).key) == -1
}

type Iterator interface {
	ReWind()
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() *data.LogRecordPos
	Close()
}
