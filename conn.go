package gohbase

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/kuhufu/gohbase/thrift/thrift2/hbase"
)

type conn struct {
	*hbase.THBaseServiceClient
	transport *thrift.TSocket
}

func newConn(addr string) (*conn, error) {
	transportFactory := thrift.NewTBufferedTransportFactory(1024 * 8)
	//protocolFactory := thrift.NewTCompactProtocolFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport, err := thrift.NewTSocket(addr)
	if err != nil {
		return nil, err
	}

	useTransport, err := transportFactory.GetTransport(transport)
	if err != nil {
		return nil, err
	}

	framedTransport := thrift.NewTFramedTransport(useTransport)
	client := hbase.NewTHBaseServiceClientFactory(framedTransport, protocolFactory)
	if err = useTransport.Open(); err != nil {
		return nil, err
	}

	return &conn{client, transport}, nil
}

// create Namespace
func (h *conn) Close() error {
	return h.transport.Close()
}
