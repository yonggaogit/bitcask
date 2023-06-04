package data

import "bitcask/fio"

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IOManager fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}
