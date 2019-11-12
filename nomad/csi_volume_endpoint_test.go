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
	srv := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0 // Prevent automatic dequeue
	})
	defer srv.Shutdown()
	testutil.WaitForLeader(t, srv.RPC)

	ns := structs.DefaultNamespace

	state := srv.fsm.State()
	state.BootstrapACLTokens(1, 0, mock.ACLManagementToken())
	srv.config.ACLEnabled = true
	policy := mock.NamespacePolicy(ns, "", []string{acl.NamespaceCapabilityCSIAccess})
	validToken := mock.CreatePolicyAndToken(t, state, 1001, "csi-access", policy)

	codec := rpcClient(t, srv)

	// Create the volume
	vols := []*structs.CSIVolume{{
		ID:           "DEADBEEF-70AD-4672-9178-802BCA500C87",
		Namespace:    ns,
		MaxClaim:     2,
		Driver:       "minnie",
		ModeWriteOne: false,
		ModeReadMany: true,
	}}
	err := state.CSIVolumeRegister(0, vols)
	require.NoError(t, err)

	// Create the register request
	req := &structs.CSIVolumeGetRequest{
		ID: "DEADBEEF-70AD-4672-9178-802BCA500C87",
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: ns,
			AuthToken: validToken.SecretID,
		},
	}

	var resp structs.CSIVolumeGetResponse
	err = msgpackrpc.CallWithCodec(codec, "CSIVolume.GetVolume", req, &resp)
	require.NoError(t, err)
	require.NotEqual(t, 0, resp.Index)
	require.Equal(t, vols[0].ID, resp.Volume.ID)
}

func TestCSIVolumeEndpoint_Put(t *testing.T) {
	t.Parallel()
	srv := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0 // Prevent automatic dequeue
	})
	defer srv.Shutdown()
	testutil.WaitForLeader(t, srv.RPC)

	ns := structs.DefaultNamespace

	state := srv.fsm.State()
	state.BootstrapACLTokens(1, 0, mock.ACLManagementToken())
	srv.config.ACLEnabled = true
	policy := mock.NamespacePolicy(ns, "", []string{acl.NamespaceCapabilityCSIAccess})
	validToken := mock.CreatePolicyAndToken(t, state, 1001, "csi-access", policy)

	codec := rpcClient(t, srv)

	// Create the volume
	vols := []*structs.CSIVolume{{
		ID:           "DEADBEEF-70AD-4672-9178-802BCA500C87",
		Namespace:    ns,
		MaxClaim:     2,
		Driver:       "minnie",
		ModeWriteOne: false,
		ModeReadMany: true,
	}}

	// Create the register request
	req := &structs.CSIVolumePutRequest{
		Volumes: vols,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: ns,
			AuthToken: validToken.SecretID,
		},
	}

	var resp structs.CSIVolumePutResponse
	err := msgpackrpc.CallWithCodec(codec, "CSIVolume.PutVolume", req, &resp)
	require.NoError(t, err)
	require.NotEqual(t, 0, resp.Index)
}

func TestCSIVolumeEndpoint_List(t *testing.T) {
	t.Parallel()
	srv := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0 // Prevent automatic dequeue
	})
	defer srv.Shutdown()
	testutil.WaitForLeader(t, srv.RPC)

	ns := structs.DefaultNamespace
	ms := "altNamespace"

	state := srv.fsm.State()
	state.BootstrapACLTokens(1, 0, mock.ACLManagementToken())
	srv.config.ACLEnabled = true

	policy := mock.NamespacePolicy(ns, "", []string{acl.NamespaceCapabilityCSIAccess})
	nsTok := mock.CreatePolicyAndToken(t, state, 1001, "csi-access", policy)
	codec := rpcClient(t, srv)

	// Create the volume
	vols := []*structs.CSIVolume{{
		ID:           "DEADBEEF-70AD-4672-9178-802BCA500C87",
		Namespace:    ns,
		MaxClaim:     2,
		Driver:       "minnie",
		ModeWriteOne: false,
		ModeReadMany: true,
	}, {
		ID:           "BAADF00D-70AD-4672-9178-802BCA500C87",
		Namespace:    ns,
		MaxClaim:     2,
		Driver:       "adam",
		ModeWriteOne: true,
		ModeReadMany: true,
	}, {
		ID:           "BEADCEED-70AD-4672-9178-802BCA500C87",
		Namespace:    ms,
		MaxClaim:     2,
		Driver:       "paddy",
		ModeWriteOne: true,
		ModeReadMany: true,
	}}
	err := state.CSIVolumeRegister(0, vols)
	require.NoError(t, err)

	var resp structs.CSIVolumeListResponse

	// Query all, ACL only allows ns
	req := &structs.CSIVolumeListRequest{
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			AuthToken: nsTok.SecretID,
		},
	}
	err = msgpackrpc.CallWithCodec(codec, "CSIVolume.List", req, &resp)
	require.NoError(t, err)
	require.NotEqual(t, 0, resp.Index)
	require.Equal(t, 2, len(resp.Volumes))
	ids := map[string]bool{vols[0].ID: true, vols[1].ID: true}
	for _, v := range resp.Volumes {
		delete(ids, v.ID)
	}
	require.Equal(t, 0, len(ids))

	// Query by Driver
	req = &structs.CSIVolumeListRequest{
		Driver: "adam",
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			AuthToken: nsTok.SecretID,
		},
	}
	err = msgpackrpc.CallWithCodec(codec, "CSIVolume.List", req, &resp)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Volumes))
	require.Equal(t, vols[1].ID, resp.Volumes[0].ID)

	// Query by Driver, ACL filters all results
	req = &structs.CSIVolumeListRequest{
		Driver: "paddy",
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			AuthToken: nsTok.SecretID,
		},
	}
	err = msgpackrpc.CallWithCodec(codec, "CSIVolume.List", req, &resp)
	require.NoError(t, err)
	require.Equal(t, 0, len(resp.Volumes))
}
