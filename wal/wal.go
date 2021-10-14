package main

import (
	"fmt"
	"io"
	"os"
	"github.com/golang-collections/go-datastructures/bitarray"
)

// Each write-ahead log entry contains the following metadata
// +-------------------+----------+---------------------+--...--+---...---+----------------------+
// | Key Size (8 bits) | Deleted? | Value Size (8 bits) |  Key  |  Value  |  Timestamp (16 bits) | 
// +-------------------+----------+---------------------+--...--+---...---+----------------------+
// Key Size: length of the key data
// Deleted: Indicate if the record was deleted and originally had a value
// Value Size: length of the value data
// Key: Key data
// Value: Value data
// Timestamp: Timestamp of the operation

type WALEntry struct {
	Key []int8
	Value []int8
	Timestamp uint64
	Deleted bool
}

func readWAL(WALFilename string) {
	file, err = os.Open(WALFilename)
	if err != nil {
		log.Fatal(err)
	}

	keyLenBuffer := make([]byte, 1)
	keyLen, err := file.Read(keyLenBuffer)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	fmt.Printf("read %d bytes: %q\n", keyLen, keyLenBuffer[:keyLen])

	boolBuffer := bitarray.NewBitArray(1)
	isDeleted, err := file.Read(boolBuffer)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	fmt.Printf("read %d bytes: %q\n", isDeleted, boolBuffer[:isDeleted])

	keyBuffer := make([]byte, keyLen)
	key := nil
	value := nil
	if isDeleted {
		_ , err := file.Read(key)
		if err != nil {
			log.Fatal(err)
			return nil
		}
	} else {
		valueLenBuffer := make([]byte, 1)
		valueLen, err := file.Read(valueLenBuffer)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		key, err := file.Read(key)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		valueBuffer := make([]byte, valueLen)
		value, err = file.Read(valueBuffer)
		if err != nil {
			log.Fatal(err)
			return nil
		}
	}

	timestampBuffer := make([]byte, 2)
	timestamp, err := file.Read(timestampBuffer)
	if err != nil {
		log.Fatal(err)
		return nil
	}

	return WALEntry {
		key
		value
		timestamp
		isDeleted
	}

}