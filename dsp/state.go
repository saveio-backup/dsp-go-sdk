package dsp

import "github.com/saveio/dsp-go-sdk/types/state"

type ModuleStateResp struct {
	Id    int
	Name  string
	State state.ModuleState
}

func (this *Dsp) GetModuleState() []*ModuleStateResp {
	var stateOfChain, stateOfFs, stateOfDNS, stateOfChannel state.ModuleState
	if this.Chain != nil {
		stateOfChain = this.Chain.State()
	}
	if this.Running() {
		if this.Fs != nil {
			stateOfFs = this.Fs.State()
		} else {
			stateOfFs = state.ModuleStateError
		}
		if this.DNS != nil {
			stateOfDNS = this.DNS.State()
		} else {
			stateOfDNS = state.ModuleStateError
		}
		if this.Channel != nil {
			stateOfChannel = this.Channel.State()
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
