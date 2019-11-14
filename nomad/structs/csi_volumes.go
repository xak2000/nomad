package structs

import (
	"fmt"
	"strings"
)

const (
	VolumeTypeCSI = "csi"
)

type CSIVolume struct {
	ID            string
	Driver        string
	Namespace     string
	Claim         int
	MaxClaim      int
	ModeReadMany  bool
	ModeWriteOne  bool
	Topology      map[string]string
	CreatedIndex  uint64
	ModifiedIndex uint64
}

func (v *CSIVolume) CanMountReadOnly() bool {
	if v.Claim < v.MaxClaim {
		return true
	}
	return false
}

func (v *CSIVolume) CanMountWritable() bool {
	if v.Claim < 1 && v.ModeWriteOne {
		return true
	}
	return false
}

// Equality by value
func (v *CSIVolume) Equal(o *CSIVolume) bool {
	if o == nil {
		return false
	}

	if v.ID == o.ID &&
		v.Driver == o.Driver &&
		v.Namespace == o.Namespace &&
		v.Claim == o.Claim &&
		v.MaxClaim == o.MaxClaim &&
		v.ModeReadMany == o.ModeReadMany &&
		v.ModeWriteOne == o.ModeWriteOne {

		if len(v.Topology) != len(o.Topology) {
			return false
		}

		for k, x := range v.Topology {
			if o.Topology[k] != x {
				return false
			}
		}

		for k, x := range o.Topology {
			if v.Topology[k] != x {
				return false
			}
		}
		return true
	}
	return false
}

// Validate validates the volume struct, returning all validation errors at once
func (v *CSIVolume) Validate() error {
	errs := []string{}

	if v.ID == "" {
		errs = append(errs, "missing volume id")
	}
	if v.Driver == "" {
		errs = append(errs, "missing driver")
	}
	if v.Namespace == "" {
		errs = append(errs, "missing namespace")
	}
	if v.MaxClaim == 0 {
		errs = append(errs, "missing max claim count")
	}
	if v.ModeReadMany == false && v.ModeWriteOne == false {
		errs = append(errs, "one mode must be set")
	}
	if len(v.Topology) == 0 {
		errs = append(errs, "missing topology")
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation: %s", strings.Join(errs, ", "))
	}
	return nil
}

// ========================================
// Request envelopes

type CSIVolumeRegisterRequest struct {
	Volumes []*CSIVolume
	WriteRequest
}

type CSIVolumeRegisterResponse struct {
	QueryMeta
}

type CSIVolumeDeregisterRequest struct {
	VolumeIDs []string
	WriteRequest
}

type CSIVolumeDeregisterResponse struct {
	QueryMeta
}

type CSIVolumeClaimRequest struct {
	VolumeIDs []string
	Claim     bool
	WriteRequest
}

type CSIVolumeListRequest struct {
	Driver string
	QueryOptions
}

type CSIVolumeListResponse struct {
	Volumes []*CSIVolume
	QueryMeta
}

type CSIVolumeGetRequest struct {
	ID string
	QueryOptions
}

type CSIVolumeGetResponse struct {
	Volume *CSIVolume
	QueryMeta
}

// ClientCSIVolumeConfig is used to configure access to host paths on a Nomad Client
type ClientCSIVolumeConfig struct {
	Name     string `hcl:",key"`
	Path     string `hcl:"path"`
	ReadOnly bool   `hcl:"read_only"`
}

func (p *ClientCSIVolumeConfig) Copy() *ClientCSIVolumeConfig {
	if p == nil {
		return nil
	}

	c := new(ClientCSIVolumeConfig)
	*c = *p
	return c
}

func CopyMapStringClientCSIVolumeConfig(m map[string]*ClientCSIVolumeConfig) map[string]*ClientCSIVolumeConfig {
	if m == nil {
		return nil
	}

	nm := make(map[string]*ClientCSIVolumeConfig, len(m))
	for k, v := range m {
		nm[k] = v.Copy()
	}

	return nm
}

func CopySliceClientCSIVolumeConfig(s []*ClientCSIVolumeConfig) []*ClientCSIVolumeConfig {
	l := len(s)
	if l == 0 {
		return nil
	}

	ns := make([]*ClientCSIVolumeConfig, l)
	for idx, cfg := range s {
		ns[idx] = cfg.Copy()
	}

	return ns
}

func CSIVolumeSliceMerge(a, b []*ClientCSIVolumeConfig) []*ClientCSIVolumeConfig {
	n := make([]*ClientCSIVolumeConfig, len(a))
	seenKeys := make(map[string]int, len(a))

	for i, config := range a {
		n[i] = config.Copy()
		seenKeys[config.Name] = i
	}

	for _, config := range b {
		if fIndex, ok := seenKeys[config.Name]; ok {
			n[fIndex] = config.Copy()
			continue
		}

		n = append(n, config.Copy())
	}

	return n
}

// VolumeMount represents the relationship between a destination path in a task
// and the task group volume that should be mounted there.
// VolumeMount is defined in volumes.go, and we can reuse it here
