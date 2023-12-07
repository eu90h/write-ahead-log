package test

import (
	"log"
	"os"
	"testing"

	wal "github.com/eu90h/write_ahead_log/pkg"
	assert "github.com/go-playground/assert/v2"
)

const LOG_ROOT = "."

func TestKVStore(t *testing.T) {
	kvs, err := wal.NewKVStore(LOG_ROOT)
	if err != nil {
		log.Panic(err)
	}
	kvs.Put("hey", "hi")
	kvs.Put("yo", "hey")
	kvs.Put("log", "stuff")
	kvs.Put("whats up", "hey")
	kvs.Put("yo", "hi")
	assert.Equal(t, kvs.Get("yo"), "hi")
	kvs, err = wal.RecreateKVStore(kvs.Wal.Underlying_file_path)
	if err != nil {
		log.Panic(err)
	}
	assert.NotEqual(t, kvs.Get("yo"), "hey")
	assert.Equal(t, kvs.Get("yo"), "hi")
	err = os.Remove(kvs.Wal.Underlying_file_path)
	if err != nil {
		log.Panic(err)
	}
}