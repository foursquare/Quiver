// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"log"
	"sync/atomic"
	"time"
)

type Scanner struct {
	reader *Reader
	idx    int
	block  []byte
	pos    *int
	buf    []byte

	// When off, maybe be faster but may return incorrect results rather than error on out-of-order keys.
	EnforceKeyOrder bool
	OrderedOps
}

func NewScanner(r *Reader) *Scanner {
	var buf []byte
	if r.CompressionCodec > CompressionNone {
		buf = make([]byte, int(float64(r.TotalUncompressedDataBytes/uint64(len(r.index)))*1.5))
	}
	return &Scanner{r, 0, nil, nil, buf, true, OrderedOps{nil}}
}

func (s *Scanner) Reset() {
	s.idx = 0
	s.block = nil
	s.pos = nil
	s.ResetState()
}

func (s *Scanner) blockFor(key []byte) ([]byte, error, bool) {
	if s.EnforceKeyOrder {
		err := s.CheckIfKeyOutOfOrder(key)
		if err != nil {
			return nil, err, false
		}
	}

	if s.reader.index[s.idx].IsAfter(key) {
		return nil, nil, false
	}

	idx := s.reader.FindBlock(s.idx, key)

	if idx != s.idx || s.block == nil { // need to load a new block
		data, err := s.reader.GetBlockBuf(idx, s.buf)
		if err != nil {
			return nil, err, false
		}
		i := 8
		s.pos = &i
		s.idx = idx
		s.block = data
	}

	return s.block, nil, true
}

func (s *Scanner) GetFirst(key []byte) ([]byte, error, bool) {
	t := time.Now().UnixNano()
	data, err, ok := s.blockFor(key)
	if s.reader.Usage != nil {
		atomic.AddInt64(&s.reader.Usage.scannerBlockForSumNs, time.Now().UnixNano()-t)
	}

	if !ok {
		if s.reader.Debug {
			log.Printf("[Scanner.GetFirst] No Block for key: %s (err: %s, found: %s)\n", hex.EncodeToString(key), err, ok)
		}
		return nil, err, ok
	}

	if s.reader.Debug {
		log.Printf("[Scanner.GetFirst] Searching Block for key: %s (pos: %d)\n", hex.EncodeToString(key), *s.pos)
	}

	t = time.Now().UnixNano()
	value, _, found := s.getValuesFromBuffer(data, s.pos, key, true)
	if s.reader.Usage != nil {
		atomic.AddInt64(&s.reader.Usage.scannerGetValuesSumNs, time.Now().UnixNano()-t)
	}
	if s.reader.Debug {
		log.Printf("[Scanner.GetFirst] After pos pos: %d\n", *s.pos)
	}

	return value, nil, found
}

func (s *Scanner) GetAll(key []byte) ([][]byte, error) {
	t := time.Now().UnixNano()
	data, err, ok := s.blockFor(key)
	if s.reader.Usage != nil {
		atomic.AddInt64(&s.reader.Usage.scannerBlockForSumNs, time.Now().UnixNano()-t)
	}

	if !ok {
		if s.reader.Debug {
			log.Printf("[Scanner.GetAll] No Block for key: %s (err: %s, found: %s)\n", hex.EncodeToString(key), err, ok)
		}
		return nil, err
	}

	t = time.Now().UnixNano()
	_, found, _ := s.getValuesFromBuffer(data, s.pos, key, false)
	if s.reader.Usage != nil {
		atomic.AddInt64(&s.reader.Usage.scannerGetValuesSumNs, time.Now().UnixNano()-t)
	}
	return found, err
}

func (s *Scanner) getValuesFromBuffer(buf []byte, pos *int, key []byte, first bool) ([]byte, [][]byte, bool) {
	var acc [][]byte

	i := *pos

	if s.reader.Debug {
		log.Printf("[Scanner.getValuesFromBuffer] buf before %d / %d\n", i, len(buf))
	}

	for len(buf)-i > 8 {
		keyLen := int(binary.BigEndian.Uint32(buf[i : i+4]))
		valLen := int(binary.BigEndian.Uint32(buf[i+4 : i+8]))

		cmp := bytes.Compare(buf[i+8:i+8+keyLen], key)

		switch {
		case cmp == 0:
			i += 8 + keyLen

			ret := make([]byte, valLen)
			copy(ret, buf[i:i+valLen])

			i += valLen // now on next length pair

			if first {
				*pos = i
				return ret, nil, true
			}
			acc = append(acc, ret)
		case cmp > 0:
			*pos = i
			return nil, acc, len(acc) > 0
		default:
			i += 8 + keyLen + valLen
		}
	}

	*pos = i

	if s.reader.Debug {
		log.Printf("[Scanner.getValuesFromBuffer] walked off block\n")
	}
	return nil, acc, len(acc) > 0
}

func (s *Scanner) Release() {
	s.Reset()
	select {
	case s.reader.scannerCache <- s:
	default:
	}
}
