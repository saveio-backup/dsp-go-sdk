package common

const (
	CHUNK_SIZE = 256 * 1024 // chunk size
)

const (
	MAX_GOROUTINES_IN_LOOP = 10
)

// timeout
const (
	TX_CONFIRM_TIMEOUT     = 60 // wait for tx confirmed timeout
	FILE_FETCH_ACK_TIMEOUT = 10 // wait for file fetch ack timeout
	BLOCK_FETCH_TIMEOUT    = 10 // fetch block and get block response timeout
	CHECK_PROVE_TIMEOUT    = 60 // client upload file and check prove timeout
)

type BlockStoreType int

const (
	BLOCK_STORE_TYPE_NORMAL BlockStoreType = iota
	BLOCK_STORE_TYPE_FILE
)
