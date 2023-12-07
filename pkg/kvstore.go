// This package implements a write-ahead log (aka commit log), an append-only log structure and a Key-Value store that utilizes it for data durability.
// For more information, see https://martinfowler.com/articles/patterns-of-distributed-systems/write-ahead-log.html
package write_ahead_log

// An in-memory key-value store. Data is backed with a append-only log, which can be replayed to recreate the object.
type KVStore struct {
	data map[string]string
	Wal *WriteAheadLog
}

func (kvs *KVStore) Get(key string) string {
	v, ok := kvs.data[key]
	if ok {
		return v
	}
	return ""
}

func (kvs *KVStore) Put(key string, value string) error {
	err := kvs.Wal.AppendLog(key, value)
	if err != nil {
		return err
	}
	kvs.data[key] = value
	return nil
}

// Recreates a KVStore by successively applying each command in the log.
func (kvs *KVStore) ApplyLog(wal *WriteAheadLog) error {
	for {
		entry, err := wal.ReadNextEntry()
		if err != nil {
			return err
		}
		if err == nil && entry.key == "" && entry.value == "" {
			return nil
		}
		kvs.data[entry.key] = entry.value
	}
}

// Recreates a KVStore from a WriteAheadLog file.
func RecreateKVStore(wal_file string) (*KVStore, error) {
	kvs := KVStore{}
	kvs.data = make(map[string]string)
	wal, err := OpenWriteAheadLog(wal_file)
	if err != nil {
		return nil, err
	}
	kvs.Wal = wal
	err = kvs.ApplyLog(wal)
	if err != nil {
		return nil, err
	}
	return &kvs, nil
}

// Creates a KVStore, placing the WriteAheadLog in the log_root directory.
func NewKVStore(log_root string) (*KVStore, error) {
	kvs := KVStore{}
	kvs.data = make(map[string]string)
	wal, err := NewWriteAheadLog(log_root)
	if err != nil {
		return nil, err
	}
	kvs.Wal = wal

	return &kvs, nil
}