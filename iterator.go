package bitcask

import (
	"bitcask/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

func (iterator *Iterator) ReWind() {
	iterator.indexIter.ReWind()
	iterator.skipToNext()
}
func (iterator *Iterator) Seek(key []byte) {
	iterator.indexIter.Seek(key)
	iterator.skipToNext()
}
func (iterator *Iterator) Next() {
	iterator.indexIter.Next()
	iterator.skipToNext()
}
func (iterator *Iterator) Valid() bool {
	return iterator.indexIter.Valid()
}
func (iterator *Iterator) Key() []byte {
	return iterator.indexIter.Key()
}
func (iterator *Iterator) Value() ([]byte, error) {
	logRecordPos := iterator.indexIter.Value()
	iterator.db.mu.RLock()
	defer iterator.db.mu.RUnlock()
	return iterator.db.getValueByPosition(logRecordPos)
}
func (iterator *Iterator) Close() {
	iterator.indexIter.Close()
}

func (iter *Iterator) skipToNext() {
	prefixLen := len(iter.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; iter.indexIter.Valid(); iter.indexIter.Next() {
		key := iter.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(iter.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
