package bitcask

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_NewWriteBatch(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}
