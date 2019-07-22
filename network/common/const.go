package common

const (
	MESSAGE_VERSION = "1"
)

const (
	FILE_OP_NONE         = 0  // no operation
	FILE_OP_FETCH_ASK    = 1  // client ask peers to fetch file from self
	FILE_OP_FETCH_ACK    = 2  // peers will fetch file from client
	FILE_OP_FETCH_RDY    = 3  // client is ready('rdy' as abbr.) to be fetched
	FILE_OP_DOWNLOAD_ASK = 4  // client ask download file from peers
	FILE_OP_DOWNLOAD_ACK = 5  // peers send ack to client
	FILE_OP_DOWNLOAD     = 6  // client send download msg to chosen peers
	FILE_OP_DOWNLOAD_OK  = 7  // client send download msg to chosen peers
	FILE_OP_BACKUP       = 8  // client back up file from peers
	FILE_OP_BACKUP_ACK   = 9  // client back up file from peers
	FILE_OP_DELETE       = 10 // client delete file of peers
	FILE_OP_DELETE_ACK   = 11 // server delete file ack
)

const (
	BLOCK_OP_NONE = 0 // no operation
	BLOCK_OP_GET  = 1 // get block
)

// message type
const (
	MSG_TYPE_NONE    = "none"
	MSG_TYPE_BLOCK   = "block"
	MSG_TYPE_FILE    = "file"
	MSG_TYPE_PAYMENT = "payment"
)

const (
	MAX_GOROUTINES_IN_LOOP = 10
	REQUEST_MSG_TIMEOUT    = 60   // 60s
	MSG_OP_CODE            = 2000 // msg op code
)

const (
	MSG_ERROR_CODE_NONE                 = 0     // success
	MSG_ERROR_CODE_PARAM_INVALID        = 50001 // param invalid
	MSG_ERROR_CODE_DOWNLOAD_REFUSED     = 50002 // download refused
	MSG_ERROR_CODE_FILE_NOT_EXIST       = 50003 // file not found
	MSG_ERROR_CODE_TOO_MANY_TASKS       = 50004 // too many tasks
	MSG_ERROR_CODE_FILE_UNITPRICE_ERROR = 50005 // file unitprice error
	MSG_ERROR_CODE_TASK_EXIST           = 50006 // file unitprice error
	MSG_ERROR_INTERNAL_ERROR            = 50007 // internal err
)

var MSG_ERROR_MSG = map[int32]string{
	MSG_ERROR_CODE_NONE:                 "success",
	MSG_ERROR_CODE_PARAM_INVALID:        "invalid params",
	MSG_ERROR_CODE_DOWNLOAD_REFUSED:     "download refused",
	MSG_ERROR_CODE_FILE_NOT_EXIST:       "file not found",
	MSG_ERROR_CODE_TOO_MANY_TASKS:       "too many tasks",
	MSG_ERROR_CODE_FILE_UNITPRICE_ERROR: "file unitprice error",
	MSG_ERROR_CODE_TASK_EXIST:           "file task exists",
	MSG_ERROR_INTERNAL_ERROR:            "internal error",
}
