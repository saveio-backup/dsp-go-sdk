package store

import "fmt"

const (
	DB_PREFIX                    = "dsp_db"
	DB_VERSION                   = "v1"
	TASK_INFO_PREFIX             = DB_PREFIX + "_" + "task" + "_" + DB_VERSION
	TASKINFO_ID_PREFIX           = DB_PREFIX + "_" + "task_id_index" + "_" + DB_VERSION
	TASKINFO_COUNT               = DB_PREFIX + "_" + "task_id_count" + "_" + DB_VERSION
	TASK_INFO_ID_FILE_PREFIX     = DB_PREFIX + "_" + "task_file_id" + "_" + DB_VERSION
	SHARE_TO_PREFIX              = DB_PREFIX + "_" + "task_share_to" + "_" + DB_VERSION
	BLOCK_INFO_PREFIX            = DB_PREFIX + "_" + "task_block_info" + "_" + DB_VERSION
	TASK_PROGRESS_PREFIX         = DB_PREFIX + "_" + "task_progress" + "_" + DB_VERSION
	TASK_UNPAID_PREFIX           = DB_PREFIX + "_" + "task_unpaid" + "_" + DB_VERSION
	TASK_DOWNLOADED_PREFIX       = DB_PREFIX + "_" + "task_downloaded" + "_" + DB_VERSION
	TASK_DOWNLOADED_COUNT_PREFIX = DB_PREFIX + "_" + "task_downloaded_count" + "_" + DB_VERSION
	TASK_OPTIONS_PREFIX          = DB_PREFIX + "_" + "task_options" + "_" + DB_VERSION
	TASK_UPLOAD_UNDONE_PREFIX    = DB_PREFIX + "_" + "task_upload_undone" + "_" + DB_VERSION
	TASK_DOWNLOAD_UNDONE_PREFIX  = DB_PREFIX + "_" + "task_download_undone" + "_" + DB_VERSION
	TASK_SESSIONS_COUNT_PREFIX   = DB_PREFIX + "_" + "task_session_count" + "_" + DB_VERSION
	TASK_SESSIONS_PREFIX         = DB_PREFIX + "_" + "task_sessions" + "_" + DB_VERSION
)

const (
	PAYMENT_ID   = DB_PREFIX + "_" + "payment" + "_" + DB_VERSION
	CHANNEL_LIST = DB_PREFIX + "_" + "channel_list" + "_" + DB_VERSION
)

// TaskInfoKey. Key of task
func TaskInfoKey(id string) string {
	return fmt.Sprintf("[%s]%s", TASK_INFO_PREFIX, id)
}

// TaskIdIndexKey. Key of task id
func TaskIdIndexKey(index uint32) string {
	return fmt.Sprintf("[%s]%d", TASKINFO_ID_PREFIX, index)
}

// TaskCountKey. Key of task count
func TaskCountKey() string {
	return TASKINFO_COUNT
}

// FileShareToKey. Key of the file whom share to
func FileShareToKey(fileInfoId, receiver string) string {
	return fmt.Sprintf("[%s]%s_%s", SHARE_TO_PREFIX, fileInfoId, receiver)
}

// BlockInfoKey. Key of block info
func BlockInfoKey(fileInfoId string, index uint32, blockHashStr string) string {
	return fmt.Sprintf("[%s]%s_%d_%s", BLOCK_INFO_PREFIX, fileInfoId, index, blockHashStr)
}

// FileProgressKey. Key of progress
func FileProgressKey(fileInfoId, nodeHostAddr string) string {
	return fmt.Sprintf("[%s]%s_%s", TASK_PROGRESS_PREFIX, fileInfoId, nodeHostAddr)
}

// FileUnpaidKey. Key of unpaid asset
func FileUnpaidKey(fileInfoId, walletAddr string, asset int32) string {
	return fmt.Sprintf("[%s]%s_%s_%d", TASK_UNPAID_PREFIX, fileInfoId, walletAddr, asset)
}

// FileDownloadedCountKey. Key of downloaded file count
func FileDownloadedCountKey() string {
	return fmt.Sprintf("[%s]", TASK_DOWNLOADED_COUNT_PREFIX)
}

// FileDownloadedKey. Key of download block index
func FileDownloadedKey(index uint32) string {
	return fmt.Sprintf("[%s]%d", TASK_DOWNLOADED_PREFIX, index)
}

// FileOptionsKey. Key of task options
func FileOptionsKey(fileInfoId string) string {
	return fmt.Sprintf("[%s]%s", TASK_OPTIONS_PREFIX, fileInfoId)
}

// FileUploadUndoneKey. Key of undone upload task
func FileUploadUndoneKey() string {
	return fmt.Sprintf("[%s]", TASK_UPLOAD_UNDONE_PREFIX)
}

// FileDownloadUndoneKey. Key of undone download task
func FileDownloadUndoneKey() string {
	return fmt.Sprintf("[%s]", TASK_DOWNLOAD_UNDONE_PREFIX)
}

// FileSessionCountKey. Key of session total count
func FileSessionCountKey(fileInfoId string) string {
	return fmt.Sprintf("[%s]%s", TASK_SESSIONS_COUNT_PREFIX, fileInfoId)
}

// FileSessionKey. Key of session index
func FileSessionKey(fileInfoId string, index int) string {
	return fmt.Sprintf("[%s]%s_%d", TASK_SESSIONS_PREFIX, fileInfoId, index)
}

// TaskIdWithFile. Key of map task id with prefix and wallet
func TaskIdWithFile(prefix, walletAddress string, tp TaskType) string {
	return fmt.Sprintf("%s-%s-%d", prefix, walletAddress, tp)
}

// TaskInfoIdWithFile. Key of file to task id
func TaskInfoIdWithFile(key string) string {
	return fmt.Sprintf("[%s]%s", TASK_INFO_ID_FILE_PREFIX, key)
}

// PaymentKey. Key of payment
func PaymentKey(paymentId int32) string {
	return fmt.Sprintf("[%s]%d", PAYMENT_ID, paymentId)
}

// ChannelListKey. Key of channel list
func ChannelListKey(waleltAddr string) string {
	return fmt.Sprintf("[%s]%s", CHANNEL_LIST, waleltAddr)
}
