package store

import "fmt"

const (
	UNIT_PRICE_PREFIX            = "store_file_download_unit_price:"
	SHARE_TO_PREFIX              = "store_file_share_to:"
	BLOCK_INFO_PREFIX            = "store_file_block_info:"
	FILE_PROGRESS_PREFIX         = "store_file_progress:"
	FILE_UNPAID_PREFIX           = "store_file_unpaid:"
	FILE_DOWNLOADED_PREFIX       = "store_file_downloaded:"
	FILE_DOWNLOADED_COUNT_PREFIX = "store_file_downloaded_count2:"
	FILE_BLOCK_OFFSET_PREFIX     = "store_file_block_offset:"
	FILE_BLOCK_TAIL_PREFIX       = "store_file_block_tail:"
	FILE_OPTIONS_PREFIX          = "store_file_options:"
	FILE_UPLOAD_UNDONE_PREFIX    = "store_file_upload_undone:"
	FILE_DOWNLOAD_UNDONE_PREFIX  = "store_file_download_undone:"
)

func FileDownloadUnitPriceKey(fileInfoId string) string {
	return fmt.Sprintf("%s-%s", UNIT_PRICE_PREFIX, fileInfoId)
}

func FileShareToKey(fileInfoId, receiver string) string {
	return fmt.Sprintf("%s-%s-%s", SHARE_TO_PREFIX, fileInfoId, receiver)
}

func BlockInfoKey(fileInfoId string, index uint32, blockHashStr string) string {
	return fmt.Sprintf("%s-%s-%d-%s", BLOCK_INFO_PREFIX, fileInfoId, index, blockHashStr)
}

func FileProgressKey(fileInfoId, nodeHostAddr string) string {
	return fmt.Sprintf("%s-%s-%s", FILE_PROGRESS_PREFIX, fileInfoId, nodeHostAddr)
}

func FileUnpaidKey(fileInfoId, walletAddr string, asset int32) string {
	return fmt.Sprintf("%s-%s-%s-%d", FILE_UNPAID_PREFIX, fileInfoId, walletAddr, asset)
}

func FileDownloadedCountKey() string {
	return FILE_DOWNLOADED_COUNT_PREFIX
}

func FileDownloadedKey(index uint32) string {
	return fmt.Sprintf("%s-%d", FILE_DOWNLOADED_PREFIX, index)
}

func FileBlockTailKey(fileInfoId string, index uint32) string {
	return fmt.Sprintf("%s-%s-%d", FILE_BLOCK_TAIL_PREFIX, fileInfoId, index)
}

func FileOptionsKey(fileInfoId string) string {
	return fmt.Sprintf("%s-%s", FILE_OPTIONS_PREFIX, fileInfoId)
}

func FileUploadUndoneKey() string {
	return FILE_UPLOAD_UNDONE_PREFIX
}

func FileDownloadUndoneKey() string {
	return FILE_DOWNLOAD_UNDONE_PREFIX
}
