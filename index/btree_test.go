package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		1, 100,
	})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 200})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		1, 100,
	})
	assert.True(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{2, 200})
	assert.True(t, res2)
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos2.Fid)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		1, 100,
	})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	bt1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())

	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	bt1.Put([]byte("dasdewfa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("fgtw"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("rtgtb"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("qefdvf"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.ReWind(); iter3.Valid(); iter3.Next() {
		t.Log("key = ", string(iter3.Key()))
	}
}
