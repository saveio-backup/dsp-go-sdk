package dsp

import "github.com/saveio/dsp-go-sdk/state"

type ModuleStateResp struct {
	Id    int
	Name  string
	State state.ModuleState
}

func (this *Dsp) GetModuleState() []*ModuleStateResp {
	var stateOfChain, stateOfFs, stateOfDNS, stateOfChannel state.ModuleState
	if this.chain != nil {
		stateOfChain = this.chain.State()
	}
	if this.Running() {
		if this.fs != nil {
			stateOfFs = this.fs.State()
		} else {
			stateOfFs = state.ModuleStateError
		}
		if this.dns != nil {
			stateOfDNS = this.dns.State()
		} else {
			stateOfDNS = state.ModuleStateError
		}
		if this.channel != nil {
			stateOfChannel = this.channel.State()
		} else {
			stateOfChannel = state.ModuleStateError
		}
	}
	if this.state.Get() == state.ModuleStateStarting {
		if stateOfFs == state.ModuleStateNone {
			stateOfFs = state.ModuleStateStarting
		}
		if stateOfDNS == state.ModuleStateNone {
			stateOfDNS = state.ModuleStateStarting
		}
		if stateOfChannel == state.ModuleStateNone {
			stateOfChannel = state.ModuleStateStarting
		}
	}

	moduleNames := []string{"dsp", "chain", "max", "scan", "pylons"}
	allStates := []state.ModuleState{this.State(), stateOfChain, stateOfFs, stateOfDNS, stateOfChannel}

	states := make([]*ModuleStateResp, 0)
	for idx, name := range moduleNames {
		states = append(states, &ModuleStateResp{
			Id:    idx,
			Name:  name,
			State: allStates[idx],
		})
	}

	return states
}
