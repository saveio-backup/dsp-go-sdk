package common

const (
	MESSAGE_VERSION = "1"
)

const (
	FILE_OP_NONE      = 0 // no operation
	FILE_OP_FETCH_ASK = 1 // client give a file to server
	FILE_OP_FETCH_ACK = 2 // server fetch file from client
	FILE_OP_FETCH_RDY = 3 // client is ready('rdy' as abbr.) to be fetched
	FILE_OP_DOWNLOAD  = 4 // client download file from server
	FILE_OP_BACKUP    = 5 // client back up file from server
	FILE_OP_DELETE    = 6 // client delete file of server
)

const (
	BLOCK_OP_NONE = 0 // no operation
	BLOCK_OP_GET  = 1 // get block
)

// message type
const (
	MSG_TYPE_BLOCK = "block"
	MSG_TYPE_FILE  = "file"
)

const (
	ASSET_NONE = 0
	ASSET_ONT  = 1
	ASSET_ONG  = 2
)

const (
	MAX_GOROUTINES_IN_LOOP = 10
)
