package base

import (
	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/core/chain"
	"github.com/saveio/dsp-go-sdk/core/channel"
	"github.com/saveio/dsp-go-sdk/core/dns"
	"github.com/saveio/dsp-go-sdk/core/fs"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
)

type ITaskMgr interface {
	Chain() *chain.Chain       // Chain module
	DNS() *dns.DNS             // dns
	Fs() *fs.Fs                // fs module
	Channel() *channel.Channel // channel module
	Config() *config.DspConfig // sdk config
	IsClient() bool            // check if is client mode
	IsWorkerBusy(taskId, walletAddr string, excludePhase int) bool
	EmitUploadResult(taskId string, result interface{}, sdkErr *sdkErr.Error)
	EmitDispatchResult(taskId string, result interface{}, sdkErr *sdkErr.Error)
	DeleteUploadTask(taskId string)
	PauseDuplicatedUploadTask(taskId, fileHashStr string) bool
	CleanUploadTask(taskId string) error
	GetDownloadTaskImpl(taskId string) interface{}
	IsDownloadTaskExist(taskId string) bool
	DeleteDownloadTask(taskId string)
	CleanDownloadTask(taskId string) error
	EmitDownloadResult(taskId string, ret interface{}, sdkErr *sdkErr.Error)
	DispatchTask(origTaskId, fileHashStr string)
	CleanDispatchTask(taskId string) error
}
