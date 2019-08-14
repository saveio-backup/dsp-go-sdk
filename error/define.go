package error

const (
	SUCCESS = 0
)

// sdk internal error
const (
	INVALID_PARAMS uint32 = iota + 40000
	INTERNAL_ERROR
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
	ADD_WHITELIST_ERROR
	GET_PDP_PARAMS_ERROR
	GET_ALL_BLOCK_ERROR
	SEARCH_RECEIVERS_FAILED
	RECEIVERS_NOT_ENOUGH
	RECEIVERS_REJECTED
	TASK_WAIT_TIMEOUT
	FILE_UPLOADED_CHECK_PDP_FAILED
	GET_SESSION_ID_FAILED
	FILE_UNIT_PRICE_ERROR
	TOO_MANY_TASKS
	FILE_NOT_FOUND_FROM_CHAIN
	DOWNLOAD_REFUSED
	CHAIN_ERROR
	TASK_PAUSE_ERROR
	GET_FILEINFO_FROM_DB_ERROR
	DOWNLOAD_FILEHASH_NOT_FOUND
	NO_CONNECTED_DNS
	NO_DOWNLOAD_SEED
	GET_DOWNLOAD_INFO_FAILED_FROM_PEERS
	PREPARE_CHANNEL_ERROR
	FILEINFO_NOT_EXIST
	PAY_UNPAID_BLOCK_FAILED
	CREATE_DOWNLOAD_FILE_FAILED
	GET_UNDOWNLOAD_BLOCK_FAILED
	DOWNLOAD_BLOCK_FAILED
	GET_FILE_STATE_ERROR
	WRITE_FILE_DATA_FAILED
	FS_PUT_BLOCK_FAILED
	ADD_GET_BLOCK_REQUEST_FAILED
	DECRYPT_FILE_FAILED
	RENAME_FILED_FAILED
	DOWNLOAD_FILE_TIMEOUT
	DOWNLOAD_FILE_RESUSED
	UNITPRICE_ERROR
	DOWNLOAD_TASK_EXIST
)

// delete file error
const (
	DELETE_FILE_FAILED uint32 = iota + 60000
	DELETE_FILE_TX_UNCONFIRMED
	DELETE_FILE_FILEINFO_EXISTS
)
