package main

import (
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/dt/httpthrift"
	"github.com/foursquare/quiver/gen"
)

func DummyServer(t hasFatal) *httptest.Server {
	Setup(t)
	return httptest.NewServer(NewHttpRpcHandler(compressed.(*ThriftRpcImpl).CollectionSet, nil))
}

func DummyClient(url string, compact bool) *gen.HFileServiceClient {
	recv, send := httpthrift.NewClientProts(url, compact)
	return gen.NewHFileServiceClientProtocol(nil, recv, send)
}

func TestRoundTrip(t *testing.T) {
	Setup(t)
	srv := DummyServer(t)
	defer srv.Close()
	client := DummyClient(srv.URL, false)
	reqs := GetRandomTestReqs("compressed", 100, 5, 50000)

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			t.Fatal("error: ", err)
		} else {
			CheckReqAndRes(t, req, res)
		}
	}
}

// The same as above except we flip the `compact` flag in the client.
func TestRoundTripTCompact(t *testing.T) {
	Setup(t)
	srv := DummyServer(t)
	defer srv.Close()
	client := DummyClient(srv.URL, true)
	reqs := GetRandomTestReqs("compressed", 100, 5, 50000)

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			t.Fatal("error: ", err)
		} else {
			CheckReqAndRes(t, req, res)
		}
	}
}

func dummyWorker(t hasFatal, client *gen.HFileServiceClient, work chan *gen.SingleHFileKeyRequest, done *sync.WaitGroup) {
	defer done.Done()
	// warmup
	if _, err := client.GetValuesSingle(GetRandomTestReqs("compressed", 1, 5, 50000)[0]); err != nil {
		return
	}

	for {
		if req, ok := <-work; !ok {
			return
		} else if res, err := client.GetValuesSingle(req); err != nil {
			t.Fatal("error: ", err)
		} else {
			CheckReqAndRes(t, req, res)
		}
	}
}

func TestConcurrentRoundTrip(t *testing.T) {
	srv := DummyServer(t)
	defer srv.Close()
	time.Sleep(time.Millisecond * 10)

	reqs := GetRandomTestReqs("compressed", 100, 5, 50000)

	workers := 100
	work := make(chan *gen.SingleHFileKeyRequest, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go dummyWorker(t, DummyClient(srv.URL, false), work, &wg)
	}

	for _, req := range reqs {
		work <- req
	}
	close(work)
	wg.Wait()
}

func BenchmarkTBinaryHTTP(b *testing.B) {
	b.StopTimer()
	srv := DummyServer(b)
	defer srv.Close()
	client := DummyClient(srv.URL, false)
	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)
	b.StartTimer()

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		} else {
			b.StopTimer()
			CheckReqAndRes(b, req, res)
			b.StartTimer()
		}
	}
}

// The same as above except we flip the `compact` flag in the client.
func BenchmarkTCompactHTTP(b *testing.B) {
	b.StopTimer()
	srv := DummyServer(b)
	defer srv.Close()
	client := DummyClient(srv.URL, true)
	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)
	b.StartTimer()

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		} else {
			b.StopTimer()
			CheckReqAndRes(b, req, res)
			b.StartTimer()
		}
	}
}

func SetupTrpc(b *testing.B, f thrift.TProtocolFactory) (*TRpcServer, func() *gen.HFileServiceClient) {
	s, err := NewTRpcServer("localhost:0", gen.NewHFileServiceProcessor(compressed), f)

	if err != nil {
		b.Fatal(err)
	} else if err = s.Listen(); err != nil {
		b.Fatal(err)
	}

	go func() {
		if err := s.Serve(); err != nil {
			b.Fatal(err)
		}
	}()

	getClient := func() *gen.HFileServiceClient {
		conn, err := s.GetClientTransport()
		if err != nil {
			b.Fatal(err)
		}
		return gen.NewHFileServiceClientFactory(conn, f)
	}
	return s, getClient
}

func BenchmarkTBinaryRaw(b *testing.B) {
	b.StopTimer()

	s, clientFactory := SetupTrpc(b, thrift.NewTBinaryProtocolFactory(true, true))
	defer s.Close()
	client := clientFactory()

	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)

	b.StartTimer()

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		} else {
			b.StopTimer()
			CheckReqAndRes(b, req, res)
			b.StartTimer()
		}
	}
}

func BenchmarkTCompactRaw(b *testing.B) {
	b.StopTimer()

	s, clientFactory := SetupTrpc(b, thrift.NewTCompactProtocolFactory())
	defer s.Close()
	client := clientFactory()

	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)

	b.StartTimer()

	for _, req := range reqs {
		if res, err := client.GetValuesSingle(req); err != nil {
			b.Fatal("error: ", err)
		} else {
			b.StopTimer()
			CheckReqAndRes(b, req, res)
			b.StartTimer()
		}
	}
}

func BenchmarkConcurrentHttp(b *testing.B) {
	b.StopTimer()
	srv := DummyServer(b)
	defer srv.Close()

	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)

	workers := 5

	work := make(chan *gen.SingleHFileKeyRequest, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go dummyWorker(b, DummyClient(srv.URL, false), work, &wg)
	}

	b.StartTimer()

	for _, req := range reqs {
		work <- req
	}

	close(work)
	wg.Wait()

	b.StopTimer()
}

func BenchmarkConcurrentRaw(b *testing.B) {
	b.StopTimer()

	s, clientFactory := SetupTrpc(b, thrift.NewTCompactProtocolFactory())
	defer s.Close()

	reqs := GetRandomTestReqs("compressed", b.N, 5, 50000)
	workers := 5
	work := make(chan *gen.SingleHFileKeyRequest, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go dummyWorker(b, clientFactory(), work, &wg)
	}
	b.StartTimer()

	for _, req := range reqs {
		work <- req
	}
	close(work)
	wg.Wait()
}
