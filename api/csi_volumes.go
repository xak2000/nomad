package api

import (
	"sort"

	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	NodeStatusInit  = "initializing"
	NodeStatusReady = "ready"
	NodeStatusDown  = "down"

	// NodeSchedulingEligible and Ineligible marks the node as eligible or not,
	// respectively, for receiving allocations. This is orthoginal to the node
	// status being ready.
	NodeSchedulingEligible   = "eligible"
	NodeSchedulingIneligible = "ineligible"
)

// Nodes is used to query node-related API endpoints
type CSIVolumes struct {
	client *Client
}

// CSIVolumes returns a handle on the volume endpoints.
func (c *Client) CSIVolumes() *CSIVolumes {
	return &CSIVolumes{client: c}
}

// List is used to list out all of the volumes
func (n *CSIVolumes) List(q *QueryOptions) ([]*structs.CSIVolume, *QueryMeta, error) {
	var resp CSIVolumeIndexSort
	qm, err := n.client.query("/v1/csi/volumes", &resp, q)
	if err != nil {
		return nil, nil, err
	}

	sort.Sort(resp)
	return resp, qm, nil
}

// func (n *CSIVolumes) PrefixList(prefix string) ([]*NodeListStub, *QueryMeta, error) {
// 	return n.List(&QueryOptions{Prefix: prefix})
// }

// Info is used to query a specific node by its ID.
func (n *CSIVolumes) Info(nodeID string, q *QueryOptions) (*Node, *QueryMeta, error) {
	var resp Node
	qm, err := n.client.query("/v1/node/"+nodeID, &resp, q)
	if err != nil {
		return nil, nil, err
	}
	return &resp, qm, nil
}
