package common

const (
	MESSAGE_VERSION = "1"
)

const (
	FILE_OP_GIVE     = 1 // client give a file to server
	FILE_OP_FETCH    = 2 // server fetch file from client
	FILE_OP_DOWNLOAD = 3 // client download file from server
	FILE_OP_BACKUP   = 4 // client back up file from server
)

// message type
const (
	MSG_TYPE_BLOCK_REQ = "blockreq"
	MSG_TYPE_BLOCK     = "block"
	MSG_TYPE_FILE      = "file"
)

const (
	ASSET_NONE = 0
	ASSET_ONT  = 1
	ASSET_ONG  = 2
)
