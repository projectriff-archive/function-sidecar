package grpc_test

import (
	"net"
	"testing"

	"github.com/sk8sio/function-sidecar/pkg/dispatcher"
	grpc "github.com/sk8sio/function-sidecar/pkg/dispatcher/grpc"
	fntypes "github.com/sk8sio/function-sidecar/pkg/dispatcher/grpc/fntypes"
	function "github.com/sk8sio/function-sidecar/pkg/dispatcher/grpc/function"
	"golang.org/x/net/context"
	google_grpc "google.golang.org/grpc"
)

type testStringFunctionServer struct {
	callback func(context.Context, *fntypes.Request) (*fntypes.Reply, error)
}

func (s *testStringFunctionServer) Call(ctx context.Context, req *fntypes.Request) (*fntypes.Reply, error) {
	return s.callback(ctx, req)
}

func TestIntegrationWithGrpc(t *testing.T) {
	message := "hello"
	listen, listenErr := net.Listen("tcp", ":10382")
	if listenErr != nil {
		t.Fatal(listenErr)
	}
	grpcServer := google_grpc.NewServer()
	// TODO: don't use a callback, wrap the server in a channel
	callback := func(ctx context.Context, req *fntypes.Request) (*fntypes.Reply, error) {
		if req.Body != message {
			t.Fatalf("got unexpected message [%s]", req.Body)
		}
		return &fntypes.Reply{}, nil
	}
	function.RegisterStringFunctionServer(grpcServer, &testStringFunctionServer{callback: callback})
	go func() {
		grpcServer.Serve(listen)
	}()
	dispatcher := grpc.NewGrpcDispatcher(&dispatcher.NoOpTraceContext{})
	_, dispatchErr := dispatcher.Dispatch(message)
	if dispatchErr != nil {
		t.Fatal(dispatchErr)
	}
}
