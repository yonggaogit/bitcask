package fio

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewFileIOManager(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("./", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("./", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}
