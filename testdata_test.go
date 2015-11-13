// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"math/rand"
	"sort"

	"github.com/foursquare/fsgo/report"
	"github.com/foursquare/quiver/gen"
	"github.com/foursquare/quiver/hfile"
	"github.com/stretchr/testify/assert"
)

var uncompressed *ThriftRpcImpl
var compressed *ThriftRpcImpl
var compressedMapped *ThriftRpcImpl
var maxKey int

type hasFatal interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	assert.TestingT
}

func Setup(t hasFatal) {
	maxKey = 10000000
	if uncompressed != nil && compressed != nil {
		return
	}
	if cs, err := hfile.TestdataCollectionSet("uncompressed", maxKey, false, hfile.CopiedToMem); err != nil {
		t.Fatal(err)
	} else {
		uncompressed = NewThriftRpcImpl(cs, report.NewRecorder())
	}
	if cs, err := hfile.TestdataCollectionSet("compressed", maxKey, true, hfile.CopiedToMem); err != nil {
		t.Fatal(err)
	} else {
		compressed = NewThriftRpcImpl(cs, report.NewRecorder())
	}
}

func SetupMapped(t hasFatal) {
	if cs, err := hfile.TestdataCollectionSet("compressed", maxKey, true, hfile.MemlockFile); err != nil {
		t.Fatal(err)
	} else {
		compressedMapped = NewThriftRpcImpl(cs, report.NewRecorder())
	}
}

func GetTestIntReq(name string, keys []int) *gen.SingleHFileKeyRequest {
	keyBytes := make([][]byte, len(keys))
	for i, v := range keys {
		keyBytes[i] = hfile.MockKeyInt(v)
	}
	return &gen.SingleHFileKeyRequest{&name, keyBytes, nil, nil}
}

func MakeTestKeyIntList(r *rand.Rand, count, max int) []int {
	if count < 1 {
		return nil
	}
	keys := make([]int, count)
	for i := 0; i < count; i++ {
		keys[i] = r.Intn(max)
	}
	sort.Ints(keys)
	return keys
}

func GetRandomTestReqs(name string, count, reqSize, max int) []*gen.SingleHFileKeyRequest {
	r := rand.New(rand.NewSource(int64(count + reqSize + max)))

	reqs := make([]*gen.SingleHFileKeyRequest, count)
	for i := 0; i < count; i++ {
		reqs[i] = GetTestIntReq(name, MakeTestKeyIntList(r, reqSize, max))
	}
	return reqs
}

func CheckReqAndRes(t assert.TestingT, req *gen.SingleHFileKeyRequest, res *gen.SingleHFileKeyResponse) {
	for i, k := range req.GetSortedKeys() {
		expected := hfile.MockValueForMockKey(k)
		actual, ok := res.Values[int32(i)]
		assert.True(t, ok)
		assert.Equal(t, actual, expected)
	}
}
