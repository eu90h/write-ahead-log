// This package implements a write-ahead log (aka commit log), an append-only log structure and a Key-Value store that utilizes it for data durability.
// For more information, see https://martinfowler.com/articles/patterns-of-distributed-systems/write-ahead-log.html
package write_ahead_log

import (
	"bufio"
	"fmt"
	"os"
	"path"

	"github.com/google/uuid"
)

// WriteAheadLog is an append-only log structure.
type WriteAheadLog struct {
	Underlying_file_path string
	fd *os.File
	scanner *bufio.Scanner
}

// Represents a single entry in a WriteAheadLog file.
type WALEntry struct {
	key string
	value string
}

// Assumes key and value have no newlines in them.
func (wal *WriteAheadLog) AppendLog(key string, value string) error {
	message := fmt.Sprintf("%s\n%s\n", key, value)
	bytes_written := 0
	for {
		n, err := wal.fd.WriteString(message[bytes_written:])
		if err != nil {
			//TODO: is there any way to recover? What if it partially wrote and then errored?
			return err
		}
		bytes_written += n
		if bytes_written >= len(message) {
			break
		}
	}
	err := wal.fd.Sync() // This flush isn't free but helps increase probability that the write made it to the disk
	if err != nil {
		//TODO: is there any way to recover? What if it partially wrote and then errored?
		return err
	}
	return nil
}

// Reads the next Key-Value pair from the log.
func (wal *WriteAheadLog) ReadNextEntry() (WALEntry, error) {
	if !wal.scanner.Scan() {
		err := wal.scanner.Err()
		return WALEntry{}, err
	}
	key := wal.scanner.Text()
	if !wal.scanner.Scan() {
		err := wal.scanner.Err()
		return WALEntry{}, err
	}
	value := wal.scanner.Text()
	return WALEntry{key, value}, nil
}

func (wal *WriteAheadLog) Close() error {
	return wal.fd.Close()
}

// Open an existing WriteAheadLog, given a path to the file itself.
func OpenWriteAheadLog(underlying_file_path string) (*WriteAheadLog, error) {
	wal := WriteAheadLog{}
	wal.Underlying_file_path = underlying_file_path
	fd, err := os.OpenFile(wal.Underlying_file_path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	wal.fd = fd
	wal.scanner = bufio.NewScanner(wal.fd)
	return &wal, nil
}

// Create a new WriteAheadLog.
func NewWriteAheadLog(log_root string) (*WriteAheadLog, error) {
	underlying_file_path := path.Join(log_root, "wal-" + uuid.New().String())
	return OpenWriteAheadLog(underlying_file_path)
}