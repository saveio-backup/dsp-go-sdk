package chain

import (
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// RegisterNode. register node to chain
func (c *Chain) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	return c.client.RegisterNode(addr, volume, serviceTime)
}

// NodeExit. exit a fs node submit to chain
func (c *Chain) NodeExit() (string, error) {
	return c.client.NodeExit()
}

// QueryNode. query node information by wallet address
func (c *Chain) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	return c.client.QueryNode(walletAddr)
}

// UpdateNode. update node information
func (c *Chain) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	return c.client.UpdateNode(addr, volume, serviceTime)
}

// RegisterNode. register node to chain
func (c *Chain) NodeWithdrawProfit() (string, error) {
	return c.client.NodeWithdrawProfit()
}
