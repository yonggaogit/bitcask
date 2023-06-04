package bitcask

type Options struct {
	DirPath string

	DataFileSize int64

	SynWrite bool

	IndexType IndexerType
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1
	ART
)
