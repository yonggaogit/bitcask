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

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

var DefaultOptions = Options{
	DirPath:      "./database",
	DataFileSize: 256 * 1024 * 1024,
	SynWrite:     false,
	IndexType:    BTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	// 一个批次中最大的数据量
	MaxBatchNum uint

	// 提交事务时是否 sync 持久化
	SyncWrites bool
}
