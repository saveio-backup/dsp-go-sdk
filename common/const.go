package common

const (
	CHUNK_SIZE                  = 256 * 1024      // chunk size
	DOWNLOAD_FILE_TEMP_DIR_PATH = "./temp"        // download temp file path
	PROTO_NODE_FILE_HASH_LEN    = 46              // proto node file hash length
	RAW_NODE_FILE_HASH_LEN      = 49              // raw node file hash length
	FILE_URL_CUSTOM_HEADER      = "save://share/" // file url custom header

	FILE_DOWNLOAD_UNIT_PRICE = 1     // unit price for all file
	DSP_NETWORK_PROTOCOL     = "tcp" // default dsp network protocol
	TRACKER_PORT             = 6369  // tracker default port
	TRACKER_NETWORK_PROTOCOL = "udp" // tracker network protocol
)

// oni link
const (
	FILE_LINK_PREFIX       = "save-link://" // save link header
	FILE_LINK_HASH_KEY     = "hash"         // hash
	FILE_LINK_NAME_KEY     = "name"         // filename
	FILE_LINK_SIZE_KEY     = "size"         // size
	FILE_LINK_BLOCKNUM_KEY = "blocknum"     // block count
	FILE_LINK_TRACKERS_KEY = "tr"           // tr
)

// timeout
const (
	TX_CONFIRM_TIMEOUT                  = 60      // wait for tx confirmed timeout
	FILE_FETCH_ACK_TIMEOUT              = 60      // wait for file fetch ack timeout
	BLOCK_FETCH_TIMEOUT                 = 60      // fetch block and get block response timeout
	CHECK_PROVE_TIMEOUT                 = 60      // client upload file and check prove timeout
	CHECK_CHANNEL_STATE_INTERVAL        = 3       // check channel state interval
	CHECK_CHANNEL_CAN_TRANSFER_INTERVAL = 1       // check channel can transfer interval
	CHECK_CHANNEL_CAN_TRANSFER_TIMEOUT  = 30      // check channel can transfer interval
	WAIT_CHANNEL_CONNECT_TIMEOUT        = 10      // wait for channel connected
	FILE_DNS_TTL                        = 10 * 60 //10m
	// CHANNEL_TRANSFER_TIMEOUT     = 60      // 60s
	CHANNEL_TRANSFER_TIMEOUT = 20 // 60s
	CHANNEL_WITHDRAW_TIMEOUT = 60 // 60s
	P2P_REQ_TIMEOUT          = 10 // 10s
	P2P_BROADCAST_TIMEOUT    = 30 // 30s
	TRACKER_SERVICE_TIMEOUT  = 10 // 5s
)

const (
	PROTO_NODE_PREFIX = "Qm"
	RAW_NODE_PREFIX   = "zb"
)

const (
	MAX_TASKS_NUM                = 50  // max task number
	MAX_GOROUTINES_FOR_WORK_TASK = 8   // max goroutines for choose worker to do job
	BACKUP_FILE_DURATION         = 10  // 10s check
	MAX_EXPIRED_PROVE_TASK_NUM   = 10  // max backup tasks one time
	MAX_WORKER_FAILED_NUM        = 10  // max failed count from a worker
	MAX_DOWNLOAD_PEERS_NUM       = 100 // max peers for download file
	MAX_NETWORK_REQUEST_RETRY    = 5   // max network request retry
)

const (
	FILE_UNIT_PRICE_TYPE_BYTE  = 1
	FILE_UNIT_PRICE_TYPE_KBYTE = 2
	FILE_UNIT_PRICE_TYPE_MBYTE = 3
	FILE_UNIT_PRICE_TYPE_GBYTE = 4
)

const (
	ASSET_NONE = 0
	ASSET_USDT = 1
)
