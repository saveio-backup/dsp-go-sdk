package common

const (
	CHUNK_SIZE                  = 256 * 1024 // chunk size
	DOWNLOAD_FILE_TEMP_DIR_PATH = "./temp"   // download temp file path
)

// timeout
const (
	TX_CONFIRM_TIMEOUT           = 60 // wait for tx confirmed timeout
	FILE_FETCH_ACK_TIMEOUT       = 10 // wait for file fetch ack timeout
	BLOCK_FETCH_TIMEOUT          = 10 // fetch block and get block response timeout
	CHECK_PROVE_TIMEOUT          = 60 // client upload file and check prove timeout
	CHECK_CHANNEL_STATE_INTERVAL = 3  // check channel state interval
	WAIT_CHANNEL_CONNECT_TIMEOUT = 60 // wait for channel connected
)

const (
	PROTO_NODE_PREFIX = "Qm"
	RAW_NODE_PREFIX   = "zb"
)

const (
	MAX_TASKS_NUM                = 50  // max task number
	MAX_GOROUTINES_FOR_WORK_TASK = 8   // max goroutines for choose worker to do job
	BACKUP_FILE_DURATION         = 10  // 10s check
	MAX_EXPIRED_PROVE_TASK_NUM   = 100 // max backup tasks
)

const (
	FILE_UNIT_PRICE_TYPE_BYTE  = 1
	FILE_UNIT_PRICE_TYPE_KBYTE = 2
	FILE_UNIT_PRICE_TYPE_MBYTE = 3
	FILE_UNIT_PRICE_TYPE_GBYTE = 4
)
