VERSION=`git describe --always`
BUILD_TIME=`date`, by `whoami`, on `hostname`

LDFLAGS=-X \"main.version=${VERSION}\" -X \"main.buildTime=${BUILD_TIME}\"

build:
	go get github.com/kardianos/govendor
	govendor sync
	go test ./...
	go build -ldflags "${LDFLAGS}"

benchmark:
	go test -bench . -benchtime 5s -test.benchmem

install: uninstall
	go install -ldflags "${LDFLAGS}"

uninstall:
	rm ${GOPATH}/bin/quiver
