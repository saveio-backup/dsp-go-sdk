package contract

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/common/log"
)

func (this *MicroPayment) GetFilterArgsForAllEventsFromChannel(chanID int, fromBlock, toBlock uint32) ([]map[string]interface{}, error) {
	toBlockUint := uint32(toBlock)

	currentH, _ := this.ChainSdk.GetCurrentBlockHeight()
	if toBlockUint > currentH {
		return nil, fmt.Errorf("toBlock bigger than currentBlockHeight:%d", currentH)
	} else if toBlockUint == 0 {
		toBlockUint = currentH
	} else if uint32(fromBlock) > toBlockUint {
		return nil, errors.New("fromBlock bigger than toBlock")
	}

	var eventRe = make([]map[string]interface{}, 0)
	for bc := uint32(fromBlock); bc <= toBlockUint; bc++ {
		raws, err := this.ChainSdk.GetSmartContractEventByBlock(bc)
		if err != nil {
			log.Errorf("get smart contract result by block err, msg:%s", err)
			return nil, err
		}
		if len(raws) == 0 {
			continue
		}
		for _, r := range raws {
			buf, err := json.Marshal(r)
			if err != nil {
				log.Errorf("json marshal result err:%s", err)
				return nil, err
			}
			result, err := utils.GetSmartContractEvent(buf)
			if err != nil {
				log.Errorf("GetSmartContractEvent[rawResult Unmarshal] err: %s", err)
				return nil, err
			}
			if result == nil {
				log.Errorf("rawResult Unmarshal return nil")
				continue
			}
			for _, notify := range result.Notify {
				if _, ok := notify.States.(map[string]interface{}); !ok {
					continue
				}
				eventRe = append(eventRe, notify.States.(map[string]interface{}))
			}
		}
	}
	return eventRe, nil
}

// GetAllFilterArgsForAllEventsFromChannel get all events from fromBlock to current block height
// return a slice of map[string]interface{}
func (this *MicroPayment) GetAllFilterArgsForAllEventsFromChannel(chanID int, fromBlock uint32) ([]map[string]interface{}, error) {
	height, err := this.ChainSdk.GetCurrentBlockHeight()
	if err != nil {
		return nil, err
	}
	return this.GetFilterArgsForAllEventsFromChannel(chanID, fromBlock, height)
}
