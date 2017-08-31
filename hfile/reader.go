// Copyright (C) 2014 Foursquare Labs Inc.

package hfile

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"

	"unicode/utf8"

	"github.com/AndreasBriese/bbloom"
	"github.com/golang/snappy"
)

type Reader struct {
	CollectionConfig

	data []byte

	majorVersion uint32
	minorVersion uint32

	FileInfo
	Trailer
	index []Block

	scannerCache  chan *Scanner
	iteratorCache chan *Iterator

	disableBloom bool
	bloom        *bbloom.Bloom
}

type FileInfo struct {
	InfoFields map[string]string // Human-readable fields read from the FileInfo block.
}

type Trailer struct {
	offset int

	FileInfoOffset             uint64
	DataIndexOffset            uint64
	DataIndexCount             uint32
	MetaIndexOffset            uint64
	MetaIndexCount             uint32
	TotalUncompressedDataBytes uint64
	EntryCount                 uint32
	CompressionCodec           uint32
}

type Block struct {
	offset        uint64
	size          uint32
	firstKeyBytes []byte
}

func NewReader(name, path string, load LoadMethod, debug bool) (*Reader, error) {
	return NewReaderFromConfig(CollectionConfig{name, path, path, nil, load, debug, name, "", "", ""})
}

func NewReaderFromConfig(cfg CollectionConfig) (*Reader, error) {
	hfile := new(Reader)
	hfile.CollectionConfig = cfg

	if cfg.cachedContent != nil {
		hfile.data = *cfg.cachedContent
	} else if data, err := loadFile(cfg.Name, cfg.LocalPath, cfg.LoadMethod); err != nil {
		return nil, err
	} else {
		hfile.data = data
	}

	v := binary.BigEndian.Uint32(hfile.data[len(hfile.data)-4:])
	hfile.majorVersion = v & 0x00ffffff
	hfile.minorVersion = v >> 24

	err := hfile.readTrailer(hfile.data)
	if err != nil {
		return nil, err
	}

	err = hfile.readFileInfo(hfile.data)
	if err != nil {
		return nil, err
	}

	err = hfile.loadIndex(hfile.data)
	if err != nil {
		return hfile, err
	}
	hfile.scannerCache = make(chan *Scanner, 5)
	hfile.iteratorCache = make(chan *Iterator, 5)
	return hfile, nil
}

func (r *Reader) PrintDebugInfo(out io.Writer, includeStartKeys int) {
	fmt.Fprintln(out, "entries: ", r.EntryCount)
	fmt.Fprintf(out, "compressed: %v (codec: %d)\n", r.CompressionCodec != CompressionNone, r.CompressionCodec)
	fmt.Fprintln(out, "blocks: ", len(r.index))
	for i, blk := range r.index {
		if i > includeStartKeys {
			fmt.Fprintf(out, "\t... and %d more\n", len(r.index)-i)
			return
		}
		fmt.Fprintf(out, "\t#%d: %s\n", i, hex.EncodeToString(blk.firstKeyBytes))
	}
}

func (r *Reader) readTrailer(data []byte) error {
	if r.majorVersion != 1 || r.minorVersion != 0 {
		return fmt.Errorf("wrong version: %d.%d", r.majorVersion, r.minorVersion)
	}

	r.Trailer.offset = len(data) - 60
	buf := bytes.NewReader(data[r.Trailer.offset:])

	trailerMagic := make([]byte, 8)
	buf.Read(trailerMagic)
	if bytes.Compare(trailerMagic, TrailerMagic) != 0 {
		return errors.New("bad trailer magic")
	}

	binary.Read(buf, binary.BigEndian, &r.FileInfoOffset)
	binary.Read(buf, binary.BigEndian, &r.DataIndexOffset)
	binary.Read(buf, binary.BigEndian, &r.DataIndexCount)
	binary.Read(buf, binary.BigEndian, &r.MetaIndexOffset)
	binary.Read(buf, binary.BigEndian, &r.MetaIndexCount)
	binary.Read(buf, binary.BigEndian, &r.TotalUncompressedDataBytes)
	binary.Read(buf, binary.BigEndian, &r.EntryCount)
	binary.Read(buf, binary.BigEndian, &r.CompressionCodec)
	return nil
}

func (r *Reader) loadIndex(data []byte) error {

	dataIndexEnd := r.MetaIndexOffset
	if r.MetaIndexOffset == 0 {
		dataIndexEnd = uint64(r.Trailer.offset)
	}

	i := r.DataIndexOffset

	r.index = make([]Block, 0, r.DataIndexCount)

	if bytes.Compare(data[i:i+8], IndexMagic) != 0 {
		return errors.New("bad data index magic")
	}
	i += 8

	for i < dataIndexEnd {
		dataBlock := Block{}

		dataBlock.offset = binary.BigEndian.Uint64(data[i:])
		i += uint64(binary.Size(dataBlock.offset))

		dataBlock.size = binary.BigEndian.Uint32(data[i:])
		i += uint64(binary.Size(dataBlock.size))

		firstKeyLen, s := vintAndLen(data[i:])
		if s < 1 || firstKeyLen < 1 {
			return fmt.Errorf("Failed to read key length, err %d", s)
		}
		i += uint64(s)

		dataBlock.firstKeyBytes = data[i : i+uint64(firstKeyLen)]
		i += uint64(firstKeyLen)

		r.index = append(r.index, dataBlock)
	}

	return nil
}

func After(a, b []byte) bool {
	return bytes.Compare(a, b) > 0
}

func (b *Block) IsAfter(key []byte) bool {
	return After(b.firstKeyBytes, key)
}

func (r *Reader) FirstKey() ([]byte, error) {
	if len(r.index) < 1 {
		return nil, fmt.Errorf("empty collection has no first key")
	}
	return r.index[0].firstKeyBytes, nil
}

func (r *Reader) FindBlock(from int, key []byte) int {
	remaining := len(r.index) - from - 1
	if r.Debug {
		log.Printf("[Reader.findBlock] cur %d, remaining %d\n", from, remaining)
	}

	if remaining <= 0 {
		if r.Debug {
			log.Println("[Reader.findBlock] last block")
		}
		return from // s.cur is the last block, so it is only choice.
	}

	if r.index[from+1].IsAfter(key) {
		if r.Debug {
			log.Println("[Reader.findBlock] next block is past key")
		}
		return from
	}

	offset := sort.Search(remaining, func(i int) bool {
		return r.index[from+i+1].IsAfter(key)
	})

	return from + offset
}

func (r *Reader) GetBlockBuf(i int, dst []byte) ([]byte, error) {
	block := r.index[i]

	switch r.CompressionCodec {
	case CompressionNone:
		dst = r.data[block.offset : block.offset+uint64(block.size)]
	case CompressionSnappy:
		/*
		   SnappyCompressor internally uses BlockCompressorStream, which writes "blocks" of compressed data.

		   These are NOT the "blocks" in the hfile sense. A "block" of an hfile may, when written by BlockCompressorStream
		   write out many of what it calls "blocks". To avoid confusion, in this code, we shall call these "subblocks".

		   Unfortunately the confusing usages of "block" do not stop there.

		   BlockCompressorStream described the "blocks" (subblocks) that it writes thus[1]:
		   "Each block contains the uncompressed length for the block, followed by one or more length-prefixed *blocks* of compressed data." (emphasis mine)

		   Yes, that is a third, distinct, "block".

		   In the hope that we might, with luck, navigate this correctly, we'll refer to these as "chunks" of "subblocks".

		   Thus, reading a "block" of an hfile we read its "subblocks" in a loop, inside of which we, in a nested loop, read the subblock's chunks.

		   1: http://grepcode.com/file/repo1.maven.org/maven2/org.apache.hadoop/hadoop-common/0.22.0/org/apache/hadoop/io/compress/BlockCompressorStream.java?av=f
		*/

		// If our pre-allocated buffer too small, alloc replacement up front, to make sure Decode doesn't.
		if len(dst) < int(block.size) {
			dst = make([]byte, block.size)
		}

		p := int(block.offset)
		decompressed := 0

		for decompressed < int(block.size) {
			subblockSize := binary.BigEndian.Uint32(r.data[p : p+4])
			subblockRead := uint32(0)
			p += 4
			for subblockRead < subblockSize {
				chunkSz := int(binary.BigEndian.Uint32(r.data[p : p+4]))
				p += 4
				target := dst[decompressed:]
				if ret, err := snappy.Decode(target, r.data[p:p+chunkSz]); err != nil {
					return nil, err
				} else {
					decompressed += len(ret)
					subblockRead += uint32(len(ret))
					_ = ret // TODO(davidt): check if Decode alloc'ed its own []byte
				}
				p += chunkSz
			}
		}
		dst = dst[:decompressed]

	default:
		return nil, errors.New("Unsupported compression codec " + string(r.CompressionCodec))
	}

	if bytes.Compare(dst[0:8], DataMagic) != 0 {
		return nil, errors.New("bad data block magic")
	}

	return dst, nil
}

func (r *Reader) CalculateBloom(falsePosRate float64) error {
	i := NewIterator(r)
	bloom := bbloom.New(float64(r.Trailer.EntryCount), falsePosRate)
	ok, err := i.Next()
	for ok && err == nil {
		bloom.Add(i.Key())
		ok, err = i.Next()
	}
	r.bloom = &bloom
	return err
}

func (r *Reader) DisableBloom() {
	r.disableBloom = true
}

func (r *Reader) EnableBloom() {
	r.disableBloom = false
}

func (r *Reader) MightContain(key []byte) bool {
	return r.bloom == nil || r.disableBloom || r.bloom.Has(key)
}

func (r *Reader) GetScanner() *Scanner {
	select {
	case s := <-r.scannerCache:
		return s
	default:
		return NewScanner(r)
	}
}

func (r *Reader) GetIterator() *Iterator {
	select {
	case i := <-r.iteratorCache:
		return i
	default:
		return NewIterator(r)
	}
}

// Grab a variable-length sequence of bytes from the buffer.
func varLenBytes(buf *bytes.Reader) ([]byte, error) {
	// Read the length of the sequence, as a varint.
	seqLen, err := vint(buf)
	if err != nil {
		return nil, err
	}
	// Read the sequence itself.
	seq := make([]byte, seqLen)
	n, err := buf.Read(seq)
	if err != nil {
		return nil, err
	}
	if n != seqLen {
		return nil, errors.New("Buffer too short to read sequence of requested length.")
	}
	return seq, nil
}

// Heuristic to figure out how to print the value in a hopefully human-readable way.
func printableValue(buf []byte) string {
	if len(buf) == 4 {
		return fmt.Sprintf("%d", binary.BigEndian.Uint32(buf))
	} else if len(buf) == 8 {
		return fmt.Sprintf("%d", binary.BigEndian.Uint64(buf))
	} else if utf8.Valid(buf) {
		return string(buf)
	} else {
		return fmt.Sprintf("0x%x", buf)
	}
}

// TODO(benjy): Write tests for this, once the writer supports writing the FileInfo block.
func (r *Reader) readFileInfo(data []byte) (err error) {
	r.FileInfo.InfoFields = make(map[string]string)

	if r.FileInfoOffset == r.DataIndexOffset {
		log.Println("[Reader.readFileInfo] No FileInfo block found. Skipping.")
		return nil
	}

	buf := bytes.NewReader(data[r.FileInfoOffset:r.DataIndexOffset])

	var entryCount uint32
	binary.Read(buf, binary.BigEndian, &entryCount)

	for i := uint32(0); i < entryCount; i++ {
		key, err := varLenBytes(buf)
		if err != nil {
			return err
		}
		buf.ReadByte() // Skip the one-byte 'id' field.  We don't care about it.
		val, err := varLenBytes(buf)
		if err != nil {
			return err
		}
		r.FileInfo.InfoFields[string(key)] = printableValue(val)
	}
	return nil
}
