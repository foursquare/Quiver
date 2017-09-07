build:
	go get github.com/kardianos/govendor
	govendor sync
	go generate
	go test ./...
	go build

benchmark:
	go generate
	go test -bench . -benchtime 5s -test.benchmem

install: uninstall
	go generate
	go install

uninstall:
	rm ${GOPATH}/bin/quiver
