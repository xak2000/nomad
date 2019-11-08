package nomad

import (
	"fmt"
	"time"

	metrics "github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	memdb "github.com/hashicorp/go-memdb"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad/acl"
	"github.com/hashicorp/nomad/nomad/state"
	"github.com/hashicorp/nomad/nomad/structs"
)

// CSIVolume wraps the structs.CSIVolume with request data and server context
type CSIVolume struct {
	srv    *Server
	logger log.Logger

	// ctx provides context regarding the underlying connection
	ctx *RPCContext

	volume *structs.CSIVolume

	// updateFuture is used to wait for the pending batch update
	// to complete. This may be nil if no batch is pending.
	// updateFuture *structs.BatchFuture

	// updateTimer is the timer that will trigger the next batch
	// update, and may be nil if there is no batch pending.
	// updateTimer *time.Timer

	// updatesLock synchronizes access to the updates list,
	// the future and the timer.
	// updatesLock sync.Mutex
}

// endpoint forwards the request to the leader, validates ACLs, and starts metrics. If it
// returns a non-nil function, that function should be called with delay in the endpoint
func (srv *Server) endpoint(args *structs.QueryOptions, reply *structs.QueryMeta,
	forward string,
	aclCheck func(*acl.ACL) bool,
	metricsNames []string,
) (stop bool, delayThunk func(), _ error) {
	// Forward to the leader
	if done, err := srv.forward(forward, args, args, reply); done {
		return true, nil, err
	}

	// Enforce ACL Token
	// Lookup the token
	aclObj, err := srv.ResolveToken(args.AuthToken)
	if err != nil {
		// If ResolveToken had an unexpected error return that
		return true, nil, err
	}

	// Check the found token with the provided predicate
	if aclObj != nil && !aclCheck(aclObj) {
		return true, nil, structs.ErrPermissionDenied
	}

	// Fallback to treating the AuthToken as a Node.SecretID
	if aclObj == nil {
		ws := memdb.NewWatchSet()
		node, stateErr := srv.fsm.State().NodeBySecretID(ws, args.AuthToken)
		if stateErr != nil {
			// Return the original ResolveToken error with this err
			var merr multierror.Error
			merr.Errors = append(merr.Errors, err, stateErr)
			return false, nil, merr.ErrorOrNil()
		}

		if node == nil {
			return true, nil, structs.ErrTokenNotFound
		}
	}

	// Start metrics
	if len(metricsNames) > 0 {
		start := time.Now()
		return false,
			func() {
				metrics.MeasureSince(metricsNames, start)
			},
			nil
	}

	return false, nil, nil
}

// List replies with CSIVolumes, filtered by ACL access
func (v *CSIVolume) List(args *structs.CSIVolumeListRequest, reply *structs.CSIVolumeListResponse) error {
	aclOk := func(namespace string, aclObj *acl.ACL) bool {
		return aclObj.AllowNsOp(namespace, acl.NamespaceCapabilityCSIAccess)
	}

	stop, deferFn, err := v.srv.endpoint(&args.QueryOptions, &reply.QueryMeta, "CSIVolume.List",
		func(a *acl.ACL) bool { return aclOk(v.volume.Namespace, a) },
		[]string{"nomad", "volume", "list"})
	if stop || err != nil {
		return err
	}
	if deferFn != nil {
		defer deferFn()
	}

	aclObj, err := v.srv.ResolveToken(args.AuthToken)
	if err != nil {
		return fmt.Errorf("acl resolve token: %v", err)
	}

	// Setup the blocking query to get the list
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			// Capture all the nodes
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.CSIVolumesByDriver(ws, prefix)
			} else {
				iter, err = state.CSIVolumes(ws)
			}
			if err != nil {
				return err
			}

			// Collect results, filter by ACL access
			var vs []*structs.CSIVolume
			cache := map[string]bool{}

			for {
				raw := iter.Next()
				if raw == nil {
					break
				}
				vol := raw.(*structs.CSIVolume)

				// Cache acl checks, they're expensive
				allowed, ok := cache[vol.Namespace]
				if !ok {
					allowed = aclOk(vol.Namespace, aclObj)
					cache[vol.Namespace] = allowed
				}

				if allowed {
					vs = append(vs, vol)
				}
			}
			reply.Volumes = vs

			// Use the last index that affected the table
			index, err := state.Index("csi_volumes")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			v.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return v.srv.blockingRPC(&opts)
}

// GetVolume fetches detailed information about a specific volume
func (v *CSIVolume) GetVolume(args *structs.CSIVolumeSingleRequest, reply *structs.CSIVolumeSingleResponse) error {
	stop, deferFn, err := v.srv.endpoint(&args.QueryOptions, &reply.QueryMeta, "CSIVolume.GetVolume",
		func(aclObj *acl.ACL) bool {
			return aclObj.AllowNsOp(v.volume.Namespace, acl.NamespaceCapabilityCSIAccess)
		},
		[]string{"nomad", "volume", "get"})
	if stop || err != nil {
		return err
	}
	if deferFn != nil {
		defer deferFn()
	}

	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			vol, err := state.CSIVolumeByID(ws, args.ID)
			if err != nil {
				return err
			}

			reply.Volume = vol

			// Use the last index that affected the table
			index, err := state.Index("csi_volumes")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			v.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return v.srv.blockingRPC(&opts)
}
