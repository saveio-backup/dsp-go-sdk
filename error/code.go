package error

const (
	SUCCESS = 0
)

// sdk internal error
const (
	INVALID_PARAMS uint32 = iota + 40000
	INTERNAL_ERROR
	INVALID_ADDRESS
	CLOSE_DB_ERROR
)

// task error
const (
	NEW_TASK_FAILED uint32 = iota + 50000
	PREPARE_UPLOAD_ERROR
	TASK_INTERNAL_ERROR
	UPLOAD_TASK_EXIST
	SHARDING_FAIELD
	FILE_HAS_UPLOADED
	GET_STORAGE_NODES_FAILED
	ONLINE_NODES_NOT_ENOUGH
	PAY_FOR_STORE_FILE_FAILED
	SET_FILEINFO_DB_ERROR
	ADD_WHITELIST_ERROR // 50010
	GET_PDP_PARAMS_ERROR
	GET_ALL_BLOCK_ERROR
	SEARCH_RECEIVERS_FAILED
	RECEIVERS_NOT_ENOUGH
	RECEIVERS_REJECTED
	TASK_WAIT_TIMEOUT
	FILE_UPLOADED_CHECK_PDP_FAILED
	GET_SESSION_ID_FAILED
	FILE_UNIT_PRICE_ERROR
	TOO_MANY_TASKS // 50020
	FILE_NOT_FOUND_FROM_CHAIN
	DOWNLOAD_REFUSED
	CHAIN_ERROR
	TASK_PAUSE_ERROR
	GET_FILEINFO_FROM_DB_ERROR
	DOWNLOAD_FILEHASH_NOT_FOUND
	NO_CONNECTED_DNS
	NO_DOWNLOAD_SEED
	GET_DOWNLOAD_INFO_FAILED_FROM_PEERS
	PREPARE_CHANNEL_ERROR // 50030
	FILEINFO_NOT_EXIST
	PAY_UNPAID_BLOCK_FAILED
	CREATE_DOWNLOAD_FILE_FAILED
	GET_UNDOWNLOADED_BLOCK_FAILED
	DOWNLOAD_BLOCK_FAILED
	GET_FILE_STATE_ERROR
	WRITE_FILE_DATA_FAILED
	FS_PUT_BLOCK_FAILED
	ADD_GET_BLOCK_REQUEST_FAILED
	DECRYPT_FILE_FAILED // 50040
	RENAME_FILED_FAILED
	DOWNLOAD_FILE_TIMEOUT
	DOWNLOAD_FILE_RESUSED
	UNITPRICE_ERROR
	DOWNLOAD_TASK_EXIST
	DECRYPT_WRONG_PASSWORD
	DELETE_FILE_HASHES_EMPTY
	NO_FILE_NEED_DELETED
	DELETE_FILE_ACCESS_DENIED
	PDP_PRIVKEY_NOT_FOUND
	GET_SIMPLE_CHECKSUM_ERROR
	WRONG_TASK_TYPE
	FILE_IS_EXPIRED
	BLOCK_HAS_SENT
	REMOTE_PEER_DELETE_FILE
	NO_BLOCK_TO_DOWNLOAD
	PAY_BLOCK_TO_SELF
	TASK_NOT_EXIST
	NO_PRIVILEGE_TO_DOWNLOAD
	CHECK_FILE_FAILED
)

// delete file error
const (
	DELETE_FILE_FAILED uint32 = iota + 60000
	DELETE_FILE_TX_UNCONFIRMED
	DELETE_FILE_FILEINFO_EXISTS
)

// FS error
const (
	FS_INTERNAL_ERROR = iota + 70000
	FS_INIT_SERVICE_ERROR
	FS_CLOSE_ERROR
	FS_CREATE_DB_ERROR
	FS_DECODE_CID_ERROR
	FS_GET_ALL_CID_ERROR
	FS_GET_ALL_OFFSET_ERROR
	FS_DECODE_BLOCK_ERROR
	FS_GET_DAG_NODE_ERROR
	FS_PUT_DATA_ERROR
	FS_GET_DATA_ERROR
	FS_PIN_ROOT_ERROR
	FS_START_PDP_ERROR
	FS_DELETE_FILE_ERROR
	FS_DECRYPT_ERROR
	FS_ENCRYPT_ERROR
)

// Network error
const (
	NETWORK_INTERNAL_ERROR = iota + 80000
	NETWORK_TIMEOUT
	NETWORK_BROADCAST_ERROR
	NETWORK_CONNECT_ERROR
	NETWORK_CLOSE_ERROR
	NETWORK_REQ_ERROR
	NETWORK_SEND_ERROR
)

const (
	CHANNEL_INTERNAL_ERROR = iota + 90000
	CHANNEL_START_SERVICE_ERROR
	CHANNEL_SYNC_BLOCK_ERROR
	CHANNEL_START_INSTANCE_ERROR
	CHANNEL_CREATE_DB_ERROR
	CHANNEL_CREATE_ACTOR_ERROR
	CHANNEL_SET_GET_IP_CALLBACK_ERROR
	CHANNEL_SET_DB_ERROR
	CHANNEL_GET_DB_ERROR
	CHANNEL_SERVICE_NOT_START
	CHANNEL_OPEN_FAILED
	CHANNEL_DEPOSIT_FAILED
	CHANNEL_CHECK_TIMEOUT
	CHANNEL_MEDIA_TRANSFER_TIMEOUT
	CHANNEL_NOT_EXIST
)

const (
	DNS_INTERNAL_ERROR = iota + 100000
	DNS_NO_REGISTER_DNS
	DNS_GET_HOSTADDR_ERROR
	DNS_REG_ENDPOINT_ERROR
	DNS_REQ_TRACKER_ERROR
	DNS_PUSH_TRACKER_ERROR
)
