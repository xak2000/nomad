package nomad

import (
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

// aclObj looks up the ACL token in the request and returns the acl.ACL object
// fallback to node secret ids
func (srv *Server) aclObj(args *structs.QueryOptions) (*acl.ACL, error) {
	// Lookup the token
	aclObj, err := srv.ResolveToken(args.AuthToken)
	if err != nil {
		// If ResolveToken had an unexpected error return that
		return nil, err
	}

	if aclObj == nil {
		ws := memdb.NewWatchSet()
		node, stateErr := srv.fsm.State().NodeBySecretID(ws, args.AuthToken)
		if stateErr != nil {
			// Return the original ResolveToken error with this err
			var merr multierror.Error
			merr.Errors = append(merr.Errors, err, stateErr)
			return nil, merr.ErrorOrNil()
		}

		if node == nil {
			return nil, structs.ErrTokenNotFound
		}
	}

	return aclObj, nil
}

// List replies with CSIVolumes, filtered by ACL access
func (v *CSIVolume) List(args *structs.CSIVolumeListRequest, reply *structs.CSIVolumeListResponse) error {
	if done, err := v.srv.forward("CSIVolume.List", args, args, reply); done {
		return err
	}

	aclObj, err := v.srv.aclObj(&args.QueryOptions)
	if err != nil {
		return err
	}

	metricsStart := time.Now()
	defer metrics.MeasureSince([]string{"nomad", "volume", "list"}, metricsStart)

	// Setup the blocking query to get the list
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			// Capture all the nodes
			var err error
			var iter memdb.ResultIterator
			if args.Driver != "" {
				iter, err = state.CSIVolumesByDriver(ws, args.Driver)
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

				if args.Namespace != "" && vol.Namespace != args.Namespace {
					continue
				}

				// Cache acl checks, QUESTION: are they expensive
				allowed, ok := cache[vol.Namespace]
				if !ok {
					allowed = aclObj.AllowNsOp(vol.Namespace, acl.NamespaceCapabilityCSIAccess)
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
func (v *CSIVolume) GetVolume(args *structs.CSIVolumeGetRequest, reply *structs.CSIVolumeGetResponse) error {
	if done, err := v.srv.forward("CSIVolume.GetVolume", args, args, reply); done {
		return err
	}

	aclObj, err := v.srv.aclObj(&args.QueryOptions)
	if err != nil {
		return err
	}

	metricsStart := time.Now()
	defer metrics.MeasureSince([]string{"nomad", "volume", "get"}, metricsStart)

	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			vol, err := state.CSIVolumeByID(ws, args.ID)
			if err != nil {
				return err
			}

			if !aclObj.AllowNsOp(vol.Namespace, acl.NamespaceCapabilityCSIAccess) {
				return structs.ErrPermissionDenied
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

// PutVolume registers a new volume
func (v *CSIVolume) PutVolume(args *structs.CSIVolumePutRequest, reply *structs.CSIVolumePutResponse) error {
	if done, err := v.srv.forward("CSIVolume.GetVolume", args, args, reply); done {
		return err
	}

	aclObj, err := v.srv.aclObj(&args.QueryOptions)
	if err != nil {
		return err
	}

	metricsStart := time.Now()
	defer metrics.MeasureSince([]string{"nomad", "volume", "get"}, metricsStart)

	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			// vol, err := state.CSIVolumeByID(ws, args.ID)
			// if err != nil {
			// 	return err
			// }

			if !aclObj.AllowNsOp(vol.Namespace, acl.NamespaceCapabilityCSIAccess) {
			// 	return structs.ErrPermissionDenied
			// }

			// reply.Volume = vol

			// // Use the last index that affected the table
			// index, err := state.Index("csi_volumes")
			// if err != nil {
			// 	return err
			// }
			// reply.Index = index

			// // Set the query response
			// v.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return v.srv.blockingRPC(&opts)
}
