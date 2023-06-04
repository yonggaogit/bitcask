package index

import (
	"bitcask/data"
	"github.com/google/btree"
	"sync"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (btree *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &Item{
		key: key,
		pos: pos,
	}
	btree.lock.Lock()
	btree.tree.ReplaceOrInsert(item)
	btree.lock.Unlock()
	return true
}
func (btree *BTree) Get(key []byte) *data.LogRecordPos {
	item := &Item{
		key: key,
	}
	btreeItem := btree.tree.Get(item)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (btree *BTree) Delete(key []byte) bool {
	item := &Item{key: key}
	btree.lock.Lock()
	oldItem := btree.tree.Delete(item)
	if oldItem == nil {
		return false
	}
	btree.lock.Unlock()

	return true
}
