package common

// websocket action
const (
	WS_SUBSCRIBE_ACTION_BLOCK         = "Block"
	WS_SUBSCRIBE_ACTION_EVENT_NOTIFY  = "Notify"
	WS_SUBSCRIBE_ACTION_EVENT_LOG     = "Log"
	WS_SUBSCRIBE_ACTION_BLOCK_TX_HASH = "BlockTxHash"
)

// polling interval in second
const (
	POLL_TX_INTERVAL = 3
)
