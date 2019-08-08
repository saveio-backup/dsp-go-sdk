package common

const (
	MESSAGE_VERSION = "1"
)

const (
	FILE_OP_NONE         = 0    // no operation
	FILE_OP_FETCH_ASK    = 1000 // client ask peers to fetch file from self
	FILE_OP_FETCH_ACK    = 1001 // peers will fetch file from client
	FILE_OP_FETCH_RDY    = 1002 // client is ready('rdy' as abbr.) to be fetched
	FILE_OP_FETCH_PAUSE  = 1003 // fetch pause client send to chosen peers
	FILE_OP_FETCH_RESUME = 1004 // fetch resume client send to chosen peers
	FILE_OP_FETCH_CANCEL = 1005 // fetch cancel msg

	FILE_OP_DOWNLOAD_ASK    = 2000 // client ask download file from peers
	FILE_OP_DOWNLOAD_ACK    = 2001 // peers send ack to client
	FILE_OP_DOWNLOAD        = 2003 // client send download msg to chosen peers
	FILE_OP_DOWNLOAD_OK     = 2004 // client send download msg to chosen peers
	FILE_OP_DOWNLOAD_CANCEL = 2005 // client send download cancel to chosen peers

	FILE_OP_BACKUP     = 3000 // client back up file from peers
	FILE_OP_BACKUP_ACK = 3001 // client back up file from peers

	FILE_OP_DELETE     = 4000 // client delete file of peers
	FILE_OP_DELETE_ACK = 4001 // server delete file ack
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
	REQUEST_MSG_TIMEOUT    = 10   // 60s
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
