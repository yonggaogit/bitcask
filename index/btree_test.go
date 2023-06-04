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
