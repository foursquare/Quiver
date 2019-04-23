// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"encoding/binary"
	"fmt"
	"os"
)

func MockKeyInt(i int) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

func MockValueForMockKey(key []byte) []byte {
	return MockValueInt(int(binary.BigEndian.Uint32(key)))
}

func MockValueInt(i int) []byte {
	return []byte(fmt.Sprintf("value-for-%d", i))
}
func MockMultiValueInt(i, k int) []byte {
	return []byte(fmt.Sprintf("value-for-%d-%d", i, k))
}

func GenerateMockHfile(path string, keyCount, blockSize int, compress, verbose, progress bool) error {
	w, err := NewLocalWriter(path, compress, blockSize, verbose)
	if err != nil {
		return err
	}
	return WriteMockIntPairs(w, keyCount, progress, false)
}

func GenerateMockMultiHfile(path string, keyCount, blockSize int, compress, verbose, progress bool) error {
	w, err := NewLocalWriter(path, compress, blockSize, verbose)
	if err != nil {
		return err
	}
	return WriteMockIntPairs(w, keyCount, progress, true)
}

func WriteMockIntPairs(w *Writer, keyCount int, progress bool, multi bool) error {
	for i := 0; i < keyCount; i++ {
		if progress && i%10000 == 0 {
			fmt.Printf("\r %d %.02f%%", i, (float64(i)*100.0)/float64(keyCount))
		}
		if multi && i%2 == 1 {
			for k := 0; k < 3; k++ {
				if err := w.Write(MockKeyInt(i), MockMultiValueInt(i, k)); err != nil {
					return err
				}
			}
		} else {
			if err := w.Write(MockKeyInt(i), MockValueInt(i)); err != nil {
				return err
			}
		}
	}

	if progress {
		fmt.Println()
	}

	w.Close()
	return nil
}

func TestdataCollectionSet(name string, count int, compress bool, load LoadMethod) (*CollectionSet, error) {
	path := fmt.Sprintf("testdata/%s.%d.hfile", name, count)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = GenerateMockHfile(path, count, 4098, compress, false, false)
		if err != nil {
			cmd := fmt.Sprintf("mockhfile -keys %d -compress=%v %s", count, compress, path)
			GenerateMockHfile(path, count, 4098, compress, false, false)
			return nil, fmt.Errorf("%s doesn't exist and generation failed: %v.\ngenerate with:\n\t%s", path, err, cmd)
		}
	} else if err != nil {
		return nil, err
	}
	return LoadCollections([]*CollectionConfig{{name, path, path, nil, load, false, name, "", "", ""}}, os.TempDir(), false, 0, nil)
}
