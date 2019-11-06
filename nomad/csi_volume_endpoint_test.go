package nomad

import (
	"testing"

	msgpackrpc "github.com/hashicorp/net-rpc-msgpackrpc"
	"github.com/hashicorp/nomad/acl"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/testutil"
	"github.com/stretchr/testify/require"
)

func TestCSIVolumeEndpoint_Get(t *testing.T) {
	t.Parallel()
	s1 := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0 // Prevent automatic dequeue
	})
	defer s1.Shutdown()

	state := s1.fsm.State()

	validToken := mock.CreatePolicyAndToken(t, state, 1001, "test-valid",
		mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityCSIAccess}))
	// invalidToken := mock.CreatePolicyAndToken(t, state, 1003, "test-invalid",
	// 	mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityCSIAccess}))

	codec := rpcClient(t, s1)
	testutil.WaitForLeader(t, s1.RPC)

	// Create the volume
	vols := []*structs.CSIVolume{{
		ID:           "DEADBEEF-70AD-4672-9178-802BCA500C87",
		MaxClaim:     2,
		Driver:       "minnie",
		ModeWriteOne: false,
		ModeReadMany: true,
	}}
	err := state.CSIVolumeRegister(0, vols)
	require.NoError(t, err)

	// Create the register request
	req := &structs.CSIVolumeSingleRequest{
		ID: "DEADBEEF-70AD-4672-9178-802BCA500C87",
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			AuthToken: validToken.SecretID,
		},
	}

	var resp structs.CSIVolumeSingleResponse
	err = msgpackrpc.CallWithCodec(codec, "CSIVolume.GetVolume", req, &resp)
	require.NoError(t, err)
	require.NotEqual(t, 0, resp.Index)

	// MLM check props
}
