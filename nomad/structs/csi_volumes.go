package structs

const (
	VolumeTypeCSI = "csi"
)

type CSIVolume struct {
	ID            string
	Driver        string
	Namespace     string
	Claim         int
	MaxClients    int
	ModeReadMany  bool
	ModeWriteOne  bool
	CreatedIndex  uint64
	ModifiedIndex uint64
}

func (v *CSIVolume) CanMountReadOnly() bool {
	if v.Claim < v.MaxClients {
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

// ========================================
// Request envelopes

type CSIVolumeRegisterRequest struct {
	Volumes []*CSIVolume
	WriteRequest
}

type CSIVolumeDeregisterRequest struct {
	VolumeIDs []string
	WriteRequest
}

type CSIVolumeClaimRequest struct {
	VolumeIDs []string
	Claim     bool
	WriteRequest
}

type CSIVolumeListRequest struct {
	QueryOptions
}

type CSIVolumeListResponse struct {
	Volumes []*CSIVolume
	QueryMeta
}

type CSIVolumeSingleRequest struct {
	ID string
	QueryOptions
}

type CSIVolumeSingleResponse struct {
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
