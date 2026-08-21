package config

import (
	"fmt"
	"sort"
)

// Diff is the change set between two config snapshots, shaped exactly around
// the actions listed in §12.2.
type Diff struct {
	AddedNodes      []string // nothing to do now; they arrive via handshake
	RemovedNodes    []string // drain, then disconnect; their ports close
	KeyChangedNodes []string // identity change ⇒ force disconnect

	AddedPorts   []int // start a listener if the node is online
	RemovedPorts []int // stop accepting, let in-flight conns finish

	// ChangedNodes need a `reload_config` push but no reconnect: channels
	// count, a remote address, an added/removed port, or a settings change.
	ChangedNodes []string

	SettingsChanged bool
}

func (d Diff) Empty() bool {
	return len(d.AddedNodes) == 0 && len(d.RemovedNodes) == 0 && len(d.KeyChangedNodes) == 0 &&
		len(d.AddedPorts) == 0 && len(d.RemovedPorts) == 0 && len(d.ChangedNodes) == 0
}

func (d Diff) String() string {
	return fmt.Sprintf("added_nodes=%v removed_nodes=%v key_changed=%v added_ports=%v removed_ports=%v reload_push=%v settings_changed=%v",
		d.AddedNodes, d.RemovedNodes, d.KeyChangedNodes, d.AddedPorts, d.RemovedPorts, d.ChangedNodes, d.SettingsChanged)
}

// DiffConfig computes old → new. A port whose `node` changed shows up as both
// a removed and an added port, which is exactly the "delete + add" handling
// the design asks for.
func DiffConfig(old, cur *Config) Diff {
	var d Diff
	if old == nil {
		for _, n := range cur.NodeOrder {
			d.AddedNodes = append(d.AddedNodes, n)
		}
		d.AddedPorts = cur.SortedPorts()
		d.SettingsChanged = true
		return d
	}

	d.SettingsChanged = old.Settings != cur.Settings

	for _, name := range old.NodeOrder {
		newNode, ok := cur.Nodes[name]
		if !ok {
			d.RemovedNodes = append(d.RemovedNodes, name)
			continue
		}
		if old.Nodes[name].Key != newNode.Key {
			d.KeyChangedNodes = append(d.KeyChangedNodes, name)
		}
	}
	for _, name := range cur.NodeOrder {
		if _, ok := old.Nodes[name]; !ok {
			d.AddedNodes = append(d.AddedNodes, name)
		}
	}

	for _, p := range old.SortedPorts() {
		np, ok := cur.Ports[p]
		if !ok || np.Node != old.Ports[p].Node {
			d.RemovedPorts = append(d.RemovedPorts, p)
		}
	}
	for _, p := range cur.SortedPorts() {
		op, ok := old.Ports[p]
		if !ok || op.Node != cur.Ports[p].Node {
			d.AddedPorts = append(d.AddedPorts, p)
		}
	}

	// Which surviving nodes need a config push?
	changed := map[string]bool{}
	for _, name := range cur.NodeOrder {
		if _, existed := old.Nodes[name]; !existed {
			continue // brand new: it will get its config at handshake time
		}
		if !old.NodeConfig(name).Equal(cur.NodeConfig(name)) {
			changed[name] = true
		}
	}
	// A key change means a forced disconnect, so a push would be pointless.
	for _, name := range d.KeyChangedNodes {
		delete(changed, name)
	}
	for name := range changed {
		d.ChangedNodes = append(d.ChangedNodes, name)
	}
	sort.Strings(d.ChangedNodes)
	return d
}
