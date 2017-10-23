# Quiver Key-Value Server
[![Build Status](https://api.travis-ci.org/foursquare/quiver.svg)](https://travis-ci.org/foursquare/quiver)

A light-weight server for querying immutable data stored in HFiles.

Quiver provides a low-latency server that can be run directly top of large, generated datasets, such as those commonly output by offline / "big data" pipelines, as an alternative to trying to bulk-load their output back into a mutable database.

## Protocol
Quiver uses Thrift-RPC-over-HTTP to communicate - standard Thrift RPC calls are encoded and sent as HTTP request/response bodies. This allows any off-the-shelf http tools (eg HAProxy) to interact with this thrift-RPC traffic.

## The HFile Format
HFiles are designed to be written incrementally (metadata is in a "trailer" at the end rather than in a header, so you do not have to buffer the whole dataset while writing) -- and include an index, meaning they can be mapped into memory and used to answer queries quickly "as-is", without needing to build indexes at serving time.

The actual implementation of the HFile file interaction (finding keys, iterating, etc) is implemented in a [standalone library](https://github.com/foursquare/quiver/tree/master/hfile). A Scala implementation exists inside Foursquare's primary open source repo [fsq.io](https://github.com/foursquare/fsqio/tree/master/src/jvm/io/fsq/hfile).

# Running Quiver
Quiver can be started with collections to serve specified on the command-line, or can be configured to read a (json-encoded) list of collections to serve from a URL.

### Specifying Collections
Collections are specified on the commandline as `servedAs=path/to/file` pairs.

`servedAs` is the name for the collection (clients include the name of the collection they are querying in requests).

Optionally, during generation, larger collections may be broken into smaller "shards" served by different quiver servers. In that case, `servedAs` can be of the form `collection/sharding-function/partition-count/partition`, in which case the "name" will be `collection/partition`, while the other fields are used for service discovery registration.

`./quiver demo=path/to/demo.hfile bigcol/mod_first_byte/40/4=path/to/bigcol/part4.hfile`

### `-config-json`
Rather than individually specifying collection information and paths on the command line, the URL to a json document containing a list of collection configs can be provided. Each config should specify:

  * `url` (string) the location of the hfile to serve.
  * `collection` (string)
  * `partition` (int)
  * `capacity` (int) total partitions
  * `function` (string)
  * `ondemand` (bool) if the hfile does _not_ need to be locked into memory.

The `servedAs` name when loading configuration from json is always `collection/partition`.

## Load Testing and Diffing

`cmd/load` is a small utility to load test or compare two running quiver servers.

It asks the server for a random sample of keys and then generates traffic at the specified QPS, generating requests from those keys.

It can optionally send the same request to a second server and compare the results, printing a warning if they returned different responses and logging timing information, and can also write timings and diff counts to graphite.

You can `go get github.com/foursquare/quiver/cmd/load` to install it in your `$GOPATH/bin`, or can just `go build` in the `cmd/load` directory and run the resulting binary.

# Contributing

## Go Version and PATH
Quiver is tested and developed assuming Go 1.9 and `$GOPATH/bin` is on PATH.

_Foursquare engineers_: add [this](https://gist.github.com/dt/12eea95cc054a2a6018f2ca839e146b9) to your `bashrc`

## First-time Setup
You will need to install [govendor](https://github.com/kardianos/govendor):

`go get github.com/kardianos/govendor`

Then for first-time setup and whenever the vendored dependencies change, run:

`govendor sync`

## Building Quiver
To build Quiver, run:

`go build`

## GoImports
Quiver uses `goimports` formatting (which groups stdlib imports).

* Install `goimports`: `go get golang.org/x/tools/cmd/goimports`

* Fix up files: `goimports -w *.go`

## Benchmarking
When making changes, keep an eye on performance changes, particularly memory allocation:

```go test -bench . -benchtime 5s -test.benchmem```

Obviously raw performance varies across machines, so run with and without your change (using `git stash` or branches) and compare results. Include before-and-after output in commit messages for performance-related changes.

For reference the results on a relatively high-spec mid-2014 Macbook Pro:

```
BenchmarkUncompressed-8    500000       13806 ns/op       544 B/op       13 allocs/op
BenchmarkCompressed-8      300000       25106 ns/op       623 B/op       18 allocs/op
```
## Re-generate Thrift
Use `./regen.sh` to re-generate the thrift code after making changes to `quiver.thrift`.
