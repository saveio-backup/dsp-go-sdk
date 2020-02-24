package common

import "github.com/saveio/carrier/network"

const (
	CHUNK_SIZE                             = 256 * 1024 // chunk size
	DOWNLOAD_FILE_TEMP_DIR_PATH            = "./temp"   // download temp file path
	PROTO_NODE_FILE_HASH_LEN               = 46         // proto node file hash length
	RAW_NODE_FILE_HASH_LEN                 = 49         // raw node file hash length
	FILE_URL_CUSTOM_HEADER_PROTOCOL        = "oni://"
	FILE_URL_CUSTOM_HEADER                 = FILE_URL_CUSTOM_HEADER_PROTOCOL + "share/" // file url custom header
	FILE_URL_CUSTOM_PLUGIN_HEADER_PROTOCOL = "dsp-plugin://"

	FILE_DOWNLOAD_UNIT_PRICE = 1     // unit price for all file
	DSP_NETWORK_PROTOCOL     = "tcp" // default dsp network protocol
	TRACKER_SVR_DEFAULT_PORT = 10340 // tracker default port
	TRACKER_NETWORK_PROTOCOL = "tcp" // tracker network protocol
	MILLISECOND_PER_SECOND   = 1000  // 1 Second = 1000 Millisecond

	TASK_STATE_CHECK_DURATION     = 1      // task state change check ticker duration
	TASK_PROGRESS_TICKER_DURATION = 2      // task ticker
	PROGRESS_SPEED_LEN            = 10     // progress speed len
	URL_LINK_VERSION              = "1.0"  // url link version
	ENCRYPTED_FILE_EXTENSION      = ".ept" // encrypted file extension
)

// oni link
const (
	FILE_LINK_PREFIX         = "oni-link://" // save link header
	FILE_LINK_HASH_KEY       = "hash"        // hash
	FILE_LINK_NAME_KEY       = "name"        // filename
	FILE_LINK_SIZE_KEY       = "size"        // size
	FILE_LINK_BLOCKNUM_KEY   = "blocknum"    // block count
	FILE_LINK_TRACKERS_KEY   = "tr"          // tr
	FILE_LINK_OWNER_KEY      = "owner"       // owner
	FILE_LINK_BLOCKSROOT_KEY = "blocksroot"  // blocksroot
)

// timeout
const (
	TX_CONFIRM_TIMEOUT                  = 60      // wait for tx confirmed timeout
	FILE_FETCH_ACK_TIMEOUT              = 60      // wait for file fetch ack timeout
	CHECK_PROVE_TIMEOUT                 = 60      // client upload file and check prove timeout
	CHECK_CHANNEL_STATE_INTERVAL        = 3       // check channel state interval
	CHECK_CHANNEL_CAN_TRANSFER_INTERVAL = 1       // check channel can transfer interval
	CHECK_CHANNEL_CAN_TRANSFER_TIMEOUT  = 30      // check channel can transfer interval
	WAIT_CHANNEL_CONNECT_TIMEOUT        = 6       // wait for channel connected
	FILE_DNS_TTL                        = 10 * 60 //10m
	CHANNEL_TRANSFER_TIMEOUT            = 20      // 60s
	CHANNEL_WITHDRAW_TIMEOUT            = 60      // 60s
	ACTOR_P2P_REQ_TIMEOUT               = 15      // 15s
	MAX_ACTOR_P2P_REQ_TIMEOUT           = 600     // max actor timeout 600s
	P2P_BROADCAST_TIMEOUT               = 10      // 30s
	P2P_REQUEST_WAIT_REPLY_TIMEOUT      = 20      // 30s
	TRACKER_SERVICE_TIMEOUT             = 15      // 5s
	WAIT_FOR_GENERATEBLOCK_TIMEOUT      = 10      // wait for generate timeout
	MEDIA_TRANSFER_TIMEOUT              = 20      // media transfer timeout

	NETWORK_STREAM_WRITE_TIMEOUT  = 8                                                                                                // network write stream timeout for 128KB (128KB/8s=16KB/s)
	DOWNLOAD_STREAM_WRITE_TIMEOUT = 2                                                                                                // download stream timeout 128KB/2s 64KB/s
	SHARE_BLOCKS_MIN_SPEED        = 100                                                                                              // share blocks min speed 100 KB/S
	SHARE_BLOCKS_TIMEOUT          = CHUNK_SIZE / 1024 * MAX_REQ_BLOCK_COUNT / SHARE_BLOCKS_MIN_SPEED                                 // timeout = DataSize / 100 KB/s = 81s
	DOWNLOAD_BLOCKFLIGHTS_TIMEOUT = (CHUNK_SIZE / network.PER_SEND_BLOCK_SIZE) * DOWNLOAD_STREAM_WRITE_TIMEOUT * MAX_REQ_BLOCK_COUNT // download block flights time out,  Notice: DOWNLOAD_BLOCKFLIGHTS_TIMEOUT <= DOWNLOAD_FILE_TIMEOUT / 2, 128s
	DOWNLOAD_FILE_TIMEOUT         = DOWNLOAD_BLOCKFLIGHTS_TIMEOUT                                                                    // download file time out, 128s
	ACTOR_MAX_P2P_REQ_TIMEOUT     = DOWNLOAD_FILE_TIMEOUT                                                                            // max p2p request timeout
)

const (
	PROTO_NODE_PREFIX = "Qm"
	RAW_NODE_PREFIX   = "zb"
)

const (
	MAX_TASKS_NUM                  = 50     // max task number
	MAX_GOROUTINES_FOR_WORK_TASK   = 8      // max goroutines for choose worker to do job
	BACKUP_FILE_DURATION           = 30     // 10s check
	DISPATCH_FILE_DURATION         = 300    // 10s check
	REMOVE_FILES_DURATION          = 10     // 10s for remove files check
	MAX_EXPIRED_PROVE_TASK_NUM     = 10     // max backup tasks one time
	MAX_WORKER_BLOCK_FAILED_NUM    = 1      // max failed count from a worker to request block
	MAX_WORKER_FILE_FAILED_NUM     = 10     // max failed count from a worker to request file
	MAX_DOWNLOAD_PEERS_NUM         = 50     // max peers for download file
	MAX_NETWORK_REQUEST_RETRY      = 4      // max network request retry
	MAX_BACKUP_FILE_FAILED         = 3      // max backup file failed times
	MAX_BACKUP_FILES_IN_QUEUE      = 10     // max backuping files
	MAX_TRACKERS_NUM               = 100    // max tracker num
	MAX_DNS_NUM                    = 15     // max dns num
	MAX_PUBLICADDR_CACHE_LEN       = 100    // cache len
	MAX_PROGRESS_CHANNEL_SIZE      = 100    // progress channel size
	MAX_SEND_BLOCK_RETRY           = 3      // max send block retry
	MAX_SAME_UPLOAD_BLOCK_NUM      = 3      // max enable upload same block for same node
	MAX_TRACKER_REQ_TIMEOUT_NUM    = 5      // max tracker request timeout num
	MAX_PUBLIC_IP_UPDATE_SECOND    = 5 * 60 // max update second to update peer host addr cache
	MAX_BLOCK_FETCHED_RETRY        = 3      // max uploading block be fetched retry
	MAX_REQ_BLOCK_COUNT            = 32     // max block count when request for download flights
	MIN_REQ_BLOCK_COUNT            = 16     // min block count when request for download flights
	MAX_SEND_BLOCK_COUNT           = 16     // max send block count for send flights
	MIN_SEND_BLOCK_COUNT           = 16     // min send block count for send flights
	MAX_START_PDP_RETRY            = 2      // max start pdp retry
	START_PDP_RETRY_DELAY          = 5      // delay pdp retry
	MAX_PEERCNT_FOR_DOWNLOAD       = 100    // max peer count (threads) for download file
	MIN_DOWNLOAD_QOS_LEN           = 3      // min download QoS size
	MAX_TRACKER_REQUEST_PARALLEL   = 10     // max concurrent tracker request num
	MAX_PEERS_NUM_GET_FROM_TRACKER = 1000   // max peer count get from tracker
	FILE_DOWNLOADED_INDEX_OFFSET   = 3      // check file is downloaded start point
	MAX_BLOCK_HEIGHT_DIFF          = 10     // max block height diff between peers
	MAX_DNS_NODE_FOR_PAY           = 4      // max dns node used to pay
)

// task
const (
	MAX_TASK_SESSION_NUM  = 100 // max task session num
	MAX_TASK_BLOCK_REQ    = 100 // max task request block
	MAX_TASK_BLOCK_NOTIFY = 100 // max task notify
)

// go routine
const (
	MAX_UPLOAD_ROUTINES = 15 // max upload go routines
	MAX_ASYNC_ROUTINES  = 15 // max request go routines
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
