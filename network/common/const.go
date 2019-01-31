package common

const (
	MESSAGE_VERSION = "1"
)

const (
	FILE_OP_NONE         = 0 // no operation
	FILE_OP_FETCH_ASK    = 1 // client give a file to server
	FILE_OP_FETCH_ACK    = 2 // server fetch file from client
	FILE_OP_FETCH_RDY    = 3 // client is ready('rdy' as abbr.) to be fetched
	FILE_OP_DOWNLOAD     = 4 // client download file from server
	FILE_OP_DOWNLOAD_ACK = 5 // client download file from server
	FILE_OP_BACKUP       = 6 // client back up file from server
	FILE_OP_BACKUP_ACK   = 7 // client back up file from server
	FILE_OP_DELETE       = 8 // client delete file of server
	FILE_OP_DELETE_ACK   = 9 // server delete file ack
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
	REQUEST_MSG_TIMEOUT    = 60 // 60s
)

const (
	MSG_ERROR_CODE_NONE             = 0     // success
	MSG_ERROR_CODE_PARAM_INVALID    = 50001 // param invalid
	MSG_ERROR_CODE_DOWNLOAD_REFUSED = 50002 // download refused
	MSG_ERROR_CODE_FILE_NOT_EXIST   = 50003 // file not found
	MSG_ERROR_CODE_TOO_MANY_TASKS   = 50004 // too many tasks
)

var MSG_ERROR_MSG = map[int32]string{
	MSG_ERROR_CODE_NONE:             "success",
	MSG_ERROR_CODE_PARAM_INVALID:    "invalid params",
	MSG_ERROR_CODE_DOWNLOAD_REFUSED: "download refused",
	MSG_ERROR_CODE_FILE_NOT_EXIST:   "file not found",
	MSG_ERROR_CODE_TOO_MANY_TASKS:   "too many tasks",
}
