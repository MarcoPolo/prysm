package helpers

import (
	"github.com/prysmaticlabs/prysm/beacon-chain/rpc/statefetcher"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PrepareStateFetchGRPCError returns an appropriate gRPC error based on the supplied argument.
// The argument error should be a result of fetching state.
func PrepareStateFetchGRPCError(err error) error {
	if stateNotFoundErr, ok := err.(*statefetcher.StateNotFoundError); ok {
		return status.Errorf(codes.NotFound, "State not found: %v", stateNotFoundErr)
	} else if parseErr, ok := err.(*statefetcher.StateIdParseError); ok {
		return status.Errorf(codes.InvalidArgument, "Invalid state ID: %v", parseErr)
	}
	return status.Errorf(codes.Internal, "Invalid state ID: %v", err)
}