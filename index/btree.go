package index

import (
	"bitcask/data"
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}

	bt.lock.Lock()
	defer bt.lock.Unlock()
	return newBTreeIterator(bt.tree, reverse)
}

type btreeIterator struct {
	curIndex int
	reverse  bool
	values   []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (bti *btreeIterator) ReWind() {
	bti.curIndex = 0
}

func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *btreeIterator) Next() {
	bti.curIndex += 1
}

func (bti *btreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
