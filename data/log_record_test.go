package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {

}
