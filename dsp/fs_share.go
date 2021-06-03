package dsp

import (
	"github.com/saveio/dsp-go-sdk/task/types"
)

// RegShareNotificationChannel. register share channel
func (this *Dsp) RegShareNotificationChannel() {
	this.TaskMgr.RegShareNotification()
}

// ShareNotificationChannel.
func (this *Dsp) ShareNotificationChannel() chan *types.ShareNotification {
	return this.TaskMgr.ShareNotification()
}

// CloseShareNotificationChannel.
func (this *Dsp) CloseShareNotificationChannel() {
	this.TaskMgr.CloseShareNotification()
}
