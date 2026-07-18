package grpcapi

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestNamespaceRequiresAuthenticatedService(t *testing.T) {
	server := &Server{apiKey: "secret"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer secret",
		"x-onix-service", "content",
	))
	namespace, err := server.namespace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if namespace != "content" {
		t.Fatalf("namespace = %q, want content", namespace)
	}
}

func TestNamespaceRejectsWrongBearer(t *testing.T) {
	server := &Server{apiKey: "secret"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer wrong",
		"x-onix-service", "content",
	))
	_, err := server.namespace(ctx)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %s, want permission denied", status.Code(err))
	}
}

func TestNamespaceRejectsOwnerLikeServiceKey(t *testing.T) {
	server := &Server{apiKey: "secret"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer secret",
		"x-onix-service", "content:user-id",
	))
	_, err := server.namespace(ctx)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want invalid argument", status.Code(err))
	}
}
