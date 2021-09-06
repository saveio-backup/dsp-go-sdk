package error

const (
	SUCCESS = 0
)

// sdk internal error code
const (
	INVALID_PARAMS  uint32 = iota + 10000 // 10000
	INTERNAL_ERROR                        // 10001
	INVALID_ADDRESS                       // 10002
	CLOSE_DB_ERROR                        // 10003
)

// Network error code
const (
	NETWORK_INTERNAL_ERROR         = iota + 20000 // 20000
	NETWORK_TIMEOUT                               // 20001
	NETWORK_BROADCAST_ERROR                       // 20002
	NETWORK_CONNECT_ERROR                         // 20003
	NETWORK_CLOSE_ERROR                           // 20004
	NETWORK_REQ_ERROR                             // 20005
	NETWORK_SEND_ERROR                            // 20006
	NETWORK_CHECK_CONN_STATE_ERROR                // 20007
)

// CHAIN error code
const (
	CHAIN_INTERNAL_ERROR       = iota + 30000 // 30000
	CHAIN_ERROR                               // 30001
	CHAIN_POLL_CONFIRMED_ERROR                // 30002
)

// FS error code
const (
	FS_INTERNAL_ERROR       = iota + 40000 // 40000
	FS_INIT_SERVICE_ERROR                  // 40001
	FS_CLOSE_ERROR                         // 40002
	FS_CREATE_DB_ERROR                     // 40003
	FS_DECODE_CID_ERROR                    // 40004
	FS_GET_ALL_CID_ERROR                   // 40005
	FS_GET_ALL_OFFSET_ERROR                // 40006
	FS_DECODE_BLOCK_ERROR                  // 40007
	FS_GET_DAG_NODE_ERROR                  // 40008
	FS_PUT_DATA_ERROR                      // 40009
	FS_GET_DATA_ERROR                      // 40010
	FS_PIN_ROOT_ERROR                      // 40011
	FS_START_PDP_ERROR                     // 40012
	FS_DELETE_FILE_ERROR                   // 40013
	FS_DECRYPT_ERROR                       // 40014
	FS_ENCRYPT_ERROR                       // 40015
)

// Channel error code
const (
	CHANNEL_INTERNAL_ERROR              = iota + 50000 // 50000
	CHANNEL_START_SERVICE_ERROR                        // 50001
	CHANNEL_SYNC_BLOCK_ERROR                           // 50002
	CHANNEL_START_INSTANCE_ERROR                       // 50003
	CHANNEL_CREATE_DB_ERROR                            // 50004
	CHANNEL_CREATE_ACTOR_ERROR                         // 50005
	CHANNEL_SET_GET_IP_CALLBACK_ERROR                  // 50006
	CHANNEL_SET_DB_ERROR                               // 50007
	CHANNEL_GET_DB_ERROR                               // 50008
	CHANNEL_SERVICE_NOT_START                          // 50009
	CHANNEL_OPEN_FAILED                                // 50010
	CHANNEL_DEPOSIT_FAILED                             // 50011
	CHANNEL_CHECK_TIMEOUT                              // 50012
	CHANNEL_MEDIA_TRANSFER_TIMEOUT                     // 50013
	CHANNEL_NOT_EXIST                                  // 50014
	CHANNEL_CLOSE_FAILED                               // 50015
	CHANNEL_DIRECT_TRANSFER_TIMEOUT                    // 50016
	CHANNEL_GET_TOTAL_BALANCE_ERROR                    // 50017
	CHANNEL_GET_AVAILABLE_BALANCE_ERROR                // 50018
	CHANNEL_GET_TOTAL_WITHDRAWL_ERROR                  // 50019
	CHANNEL_GET_CURRENT_BALANCE_ERROR                  // 50020
	CHANNEL_WITHDRAW_FAILED                            // 50021
	CHANNEL_COOPERATIVE_SETTLE_ERROR                   // 50022
	CHANNEL_DIRECT_TRANSFER_ERROR                      // 50023
)

const (
	DNS_INTERNAL_ERROR     = iota + 60000 // 60000
	DNS_NO_REGISTER_DNS                   // 60001
	DNS_GET_HOSTADDR_ERROR                // 60002
	DNS_REG_ENDPOINT_ERROR                // 60003
	DNS_REQ_TRACKER_ERROR                 // 60004
	DNS_PUSH_TRACKER_ERROR                // 60005
	DNS_TRACKER_TIMEOUT                   // 60006
)

// task error
const (
	NEW_TASK_FAILED             uint32 = iota + 70000 // 70000
	TASK_INTERNAL_ERROR                               // 70001
	GET_STORAGE_NODES_FAILED                          // 70002
	ONLINE_NODES_NOT_ENOUGH                           // 70003
	SET_FILEINFO_DB_ERROR                             // 70004
	ADD_WHITELIST_ERROR                               // 70005
	GET_PDP_PARAMS_ERROR                              // 70006
	GET_ALL_BLOCK_ERROR                               // 70007
	TASK_WAIT_TIMEOUT                                 // 70008
	GET_SESSION_ID_FAILED                             // 70009
	FILE_UNIT_PRICE_ERROR                             // 70010
	TOO_MANY_TASKS                                    // 70011
	FILE_NOT_FOUND_FROM_CHAIN                         // 70012
	TASK_PAUSE_ERROR                                  // 70013
	GET_FILEINFO_FROM_DB_ERROR                        // 70014
	PREPARE_CHANNEL_ERROR                             // 70015
	FILEINFO_NOT_EXIST                                // 70016
	GET_FILE_STATE_ERROR                              // 70017
	UNITPRICE_ERROR                                   // 70018
	DELETE_FILE_HASHES_EMPTY                          // 70019
	NO_FILE_NEED_DELETED                              // 70020
	DELETE_FILE_ACCESS_DENIED                         // 70021
	WRONG_TASK_TYPE                                   // 70022
	FILE_IS_EXPIRED                                   // 70023
	BLOCK_HAS_SENT                                    // 70024
	PAY_BLOCK_TO_SELF                                 // 70025
	TASK_NOT_EXIST                                    // 70026
	BLOCK_NOT_FOUND                                   // 70027
	REFER_ID_NOT_FOUND                                // 70028
	GET_TASK_PROPERTY_ERROR                           // 70029
	SET_TASK_PROPERTY_ERROR                           // 70030
	DISPATCH_FILE_ERROR                               // 70031
	RECEIVE_ERROR_MSG                                 // 70032
	MISSING_FILE_BLOCKS_ROOT                          // 70033
	GET_HOST_ADDR_ERROR                               // 70034
	TASK_RESUME_ERROR                                 // 70035
	TASK_RETRY_ERROR                                  // 70036
	DELETE_FILE_FAILED                                // 70037
	DELETE_FILE_TX_UNCONFIRMED                        // 70038
	DELETE_FILE_FILEINFO_EXISTS                       // 70039
	POC_TASK_ERROR                                    // 70040
)

// Upload task error code
const (
	UPLOAD_TASK_INTERNAL_ERROR     = iota + 70100 // 70100
	PREPARE_UPLOAD_ERROR                          // 70101
	UPLOAD_TASK_EXIST                             // 70102
	SHARDING_FAIELD                               // 70103
	FILE_HAS_UPLOADED                             // 70104
	PAY_FOR_STORE_FILE_FAILED                     // 70105
	SEARCH_RECEIVERS_FAILED                       // 70106
	RECEIVERS_NOT_ENOUGH                          // 70107
	RECEIVERS_REJECTED                            // 70108
	FILE_UPLOADED_CHECK_PDP_FAILED                // 70109
	PDP_PRIVKEY_NOT_FOUND                         // 70110
	GET_SIMPLE_CHECKSUM_ERROR                     // 70111
	MISS_UPLOADED_FILE_TX                         // 70112
	CHECK_UPLOADED_TX_ERROR                       // 70113
	NO_PRIVILEGE_TO_UPLOAD                        // 70114
)

// Download task error code
const (
	DOWNLOAD_TASK_INTERNAL_ERROR        = iota + 70200 // 70200
	DOWNLOAD_REFUSED                                   // 70201
	DOWNLOAD_FILEHASH_NOT_FOUND                        // 70202
	NO_CONNECTED_DNS                                   // 70203
	NO_DOWNLOAD_SEED                                   // 70204
	GET_DOWNLOAD_INFO_FAILED_FROM_PEERS                // 70205
	PAY_UNPAID_BLOCK_FAILED                            // 70206
	CREATE_DOWNLOAD_FILE_FAILED                        // 70207
	GET_UNDOWNLOADED_BLOCK_FAILED                      // 70208
	DOWNLOAD_BLOCK_FAILED                              // 70209
	WRITE_FILE_DATA_FAILED                             // 70210
	FS_PUT_BLOCK_FAILED                                // 70211
	ADD_GET_BLOCK_REQUEST_FAILED                       // 70212
	DECRYPT_FILE_FAILED                                // 70213
	RENAME_FILED_FAILED                                // 70214
	DOWNLOAD_FILE_TIMEOUT                              // 70215
	DOWNLOAD_FILE_RESUSED                              // 70216
	DOWNLOAD_TASK_EXIST                                // 70217
	DECRYPT_WRONG_PASSWORD                             // 70218
	REMOTE_PEER_DELETE_FILE                            // 70219
	NO_BLOCK_TO_DOWNLOAD                               // 70220
	NO_PRIVILEGE_TO_DOWNLOAD                           // 70221
	CHECK_FILE_FAILED                                  // 70222
	EXIST_UNPAID_BLOCKS                                // 70223
	ADD_FILE_UNPAID_ERROR                              // 70224
)
