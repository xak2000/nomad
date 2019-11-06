package nomad

import (
	"testing"

	msgpackrpc "github.com/hashicorp/net-rpc-msgpackrpc"
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
	err := s1.fsm.State().CSIVolumeRegister(0, vols)
	require.NoError(t, err)

	// Create the register request
	req := &structs.CSIVolumeSingleRequest{
		ID: "DEADBEEF-70AD-4672-9178-802BCA500C87",
	}

	var resp structs.CSIVolumeSingleResponse
	if err := msgpackrpc.CallWithCodec(codec, "CSIVolume.GetVolume", req, &resp); err != nil {
		t.Fatalf("err: %v", err)
	}
	if resp.Index == 0 {
		t.Fatalf("bad index: %d", resp.Index)
	}

	// MLM check props
}
