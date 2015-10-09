package main

import (
	"net"

	"github.com/apache/thrift/lib/go/thrift"
)

type TRpcServer struct {
	server thrift.TServer
	socket *thrift.TServerSocket
}

func NewTRpcServer(listen string, handler thrift.TProcessor, prot thrift.TProtocolFactory) (*TRpcServer, error) {
	transport, err := thrift.NewTServerSocket(listen)

	if err != nil {
		return nil, err
	}
	server := thrift.NewTSimpleServer4(
		handler,
		transport,
		thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		prot,
	)

	return &TRpcServer{server, transport}, nil
}

func (t *TRpcServer) Listen() error {
	return t.socket.Listen()
}

func (t *TRpcServer) Serve() error {
	return t.server.Serve()
}

func (t *TRpcServer) Addr() net.Addr {
	return t.socket.Addr()
}

func (t *TRpcServer) Close() {
	t.server.Stop()
	t.socket.Close()
}

func (t *TRpcServer) GetClientTransport() (thrift.TTransport, error) {
	transport, err := thrift.NewTSocket(t.Addr().String())
	if err != nil {
		return nil, err
	}
	if err = transport.Open(); err != nil {
		return nil, err
	}

	return thrift.NewTFramedTransport(transport), nil
}
