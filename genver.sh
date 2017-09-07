#!/usr/bin/env bash
# genver.sh -- called by go:generate to create version.go

echo "package main
var version string = \"`git describe --always`\"
var buildTime = \"`date`, by `whoami`, on `hostname`\"
"
