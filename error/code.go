package error

const (
	SUCCESS = 0
)

// sdk internal error
const (
	INVALID_PARAMS  uint32 = iota + 40000 // 40000
	INTERNAL_ERROR                        // 40001
	INVALID_ADDRESS                       // 40002
	CLOSE_DB_ERROR                        // 40003
)

// task error
const (
	NEW_TASK_FAILED                     uint32 = iota + 50000 // 50000
	PREPARE_UPLOAD_ERROR                                      // 50001
	TASK_INTERNAL_ERROR                                       // 50002
	UPLOAD_TASK_EXIST                                         // 50003
	SHARDING_FAIELD                                           // 50004
	FILE_HAS_UPLOADED                                         // 50005
	GET_STORAGE_NODES_FAILED                                  // 50006
	ONLINE_NODES_NOT_ENOUGH                                   // 50007
	PAY_FOR_STORE_FILE_FAILED                                 // 50008
	SET_FILEINFO_DB_ERROR                                     // 50009
	ADD_WHITELIST_ERROR                                       // 50010
	GET_PDP_PARAMS_ERROR                                      // 50011
	GET_ALL_BLOCK_ERROR                                       // 50012
	SEARCH_RECEIVERS_FAILED                                   // 50013
	RECEIVERS_NOT_ENOUGH                                      // 50014
	RECEIVERS_REJECTED                                        // 50015
	TASK_WAIT_TIMEOUT                                         // 50016
	FILE_UPLOADED_CHECK_PDP_FAILED                            // 50017
	GET_SESSION_ID_FAILED                                     // 50018
	FILE_UNIT_PRICE_ERROR                                     // 50019
	TOO_MANY_TASKS                                            // 50020
	FILE_NOT_FOUND_FROM_CHAIN                                 // 50021
	DOWNLOAD_REFUSED                                          // 50022
	CHAIN_ERROR                                               // 50023
	TASK_PAUSE_ERROR                                          // 50024
	GET_FILEINFO_FROM_DB_ERROR                                // 50025
	DOWNLOAD_FILEHASH_NOT_FOUND                               // 50026
	NO_CONNECTED_DNS                                          // 50027
	NO_DOWNLOAD_SEED                                          // 50028
	GET_DOWNLOAD_INFO_FAILED_FROM_PEERS                       // 50029
	PREPARE_CHANNEL_ERROR                                     // 50030
	FILEINFO_NOT_EXIST                                        // 50031
	PAY_UNPAID_BLOCK_FAILED                                   // 50032
	CREATE_DOWNLOAD_FILE_FAILED                               // 50033
	GET_UNDOWNLOADED_BLOCK_FAILED                             // 50034
	DOWNLOAD_BLOCK_FAILED                                     // 50035
	GET_FILE_STATE_ERROR                                      // 50036
	WRITE_FILE_DATA_FAILED                                    // 50037
	FS_PUT_BLOCK_FAILED                                       // 50038
	ADD_GET_BLOCK_REQUEST_FAILED                              // 50039
	DECRYPT_FILE_FAILED                                       // 50040
	RENAME_FILED_FAILED                                       // 50041
	DOWNLOAD_FILE_TIMEOUT                                     // 50042
	DOWNLOAD_FILE_RESUSED                                     // 50043
	UNITPRICE_ERROR                                           // 50044
	DOWNLOAD_TASK_EXIST                                       // 50045
	DECRYPT_WRONG_PASSWORD                                    // 50046
	DELETE_FILE_HASHES_EMPTY                                  // 50047
	NO_FILE_NEED_DELETED                                      // 50048
	DELETE_FILE_ACCESS_DENIED                                 // 50049
	PDP_PRIVKEY_NOT_FOUND                                     // 50050
	GET_SIMPLE_CHECKSUM_ERROR                                 // 50051
	WRONG_TASK_TYPE                                           // 50052
	FILE_IS_EXPIRED                                           // 50053
	BLOCK_HAS_SENT                                            // 50054
	REMOTE_PEER_DELETE_FILE                                   // 50055
	NO_BLOCK_TO_DOWNLOAD                                      // 50056
	PAY_BLOCK_TO_SELF                                         // 50057
	TASK_NOT_EXIST                                            // 50058
	NO_PRIVILEGE_TO_DOWNLOAD                                  // 50059
	CHECK_FILE_FAILED                                         // 50060
	EXIST_UNPAID_BLOCKS                                       // 50061
	BLOCK_NOT_FOUND                                           // 50062
	REFER_ID_NOT_FOUND                                        // 50063
	GET_TASK_PROPERTY_ERROR                                   // 50064
	ADD_FILE_UNPAID_ERROR                                     // 50065
)

// delete file error
const (
	DELETE_FILE_FAILED          uint32 = iota + 60000 // 60000
	DELETE_FILE_TX_UNCONFIRMED                        // 60001
	DELETE_FILE_FILEINFO_EXISTS                       // 60002
)

// FS error
const (
	FS_INTERNAL_ERROR       = iota + 70000 // 70000
	FS_INIT_SERVICE_ERROR                  // 70001
	FS_CLOSE_ERROR                         // 70002
	FS_CREATE_DB_ERROR                     // 70003
	FS_DECODE_CID_ERROR                    // 70004
	FS_GET_ALL_CID_ERROR                   // 70005
	FS_GET_ALL_OFFSET_ERROR                // 70006
	FS_DECODE_BLOCK_ERROR                  // 70007
	FS_GET_DAG_NODE_ERROR                  // 70008
	FS_PUT_DATA_ERROR                      // 70009
	FS_GET_DATA_ERROR                      // 70010
	FS_PIN_ROOT_ERROR                      // 70011
	FS_START_PDP_ERROR                     // 70012
	FS_DELETE_FILE_ERROR                   // 70013
	FS_DECRYPT_ERROR                       // 70014
	FS_ENCRYPT_ERROR                       // 70015
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
	DNS_INTERNAL_ERROR     = iota + 100000 // 100000
	DNS_NO_REGISTER_DNS                    // 100001
	DNS_GET_HOSTADDR_ERROR                 // 100002
	DNS_REG_ENDPOINT_ERROR                 // 100003
	DNS_REQ_TRACKER_ERROR                  // 100004
	DNS_PUSH_TRACKER_ERROR                 // 100005
	DNS_TRACKER_TIMEOUT                    // 100006
)
