package dsp

import (
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/oniChain/common/log"
	"github.com/oniio/oniDNS/tracker"
)

func (this *Dsp) PushToTrackers(hash string, trackerUrls []string, listenAddr string) error {
	index := strings.Index(listenAddr, "://")
	hostPort := listenAddr
	if index != -1 {
		hostPort = listenAddr[index+3:]
	}
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return err
	}
	netIp := net.ParseIP(host).To4()
	if netIp == nil {
		netIp = net.ParseIP(host).To16()
	}
	netPort, err := strconv.Atoi(port)
	if err != nil {
		return err
	}
	var hashBytes [46]byte
	copy(hashBytes[:], []byte(hash)[:])
	for _, trackerUrl := range trackerUrls {
		err := tracker.CompleteTorrent(hashBytes, trackerUrl, netIp, uint16(netPort))
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *Dsp) GetPeerFromTracker(hash string, trackerUrls []string) []string {
	var hashBytes [46]byte
	copy(hashBytes[:], []byte(hash)[:])

	peerAddrs := make([]string, 0)
	for _, trackerUrl := range trackerUrls {
		peers := tracker.GetTorrentPeers(hashBytes, trackerUrl, -1, 1)
		if len(peers) == 0 {
			continue
		}
		for _, p := range peers {
			addr := fmt.Sprintf("%s://%s:%d", this.Network.Protocol(), p.IP, p.Port)
			peerAddrs = append(peerAddrs, addr)
		}
		break
	}
	return peerAddrs
}

func (this *Dsp) StartSeedService() {
	tick := time.NewTicker(time.Duration(this.Config.SeedInterval) * time.Second)
	for {
		<-tick.C
		if len(this.Config.TrackerUrls) == 0 {
			continue
		}
		fileInfos, err := ioutil.ReadDir(this.Config.FsFileRoot)
		if err != nil || len(fileInfos) == 0 {
			continue
		}
		files := make([]string, 0)
		for _, info := range fileInfos {
			if info.IsDir() || !strings.HasPrefix(info.Name(), common.PROTO_NODE_PREFIX) {
				continue
			}
			files = append(files, info.Name())
		}
		if len(files) == 0 {
			continue
		}
		log.Debugf("push file to tracker %v", files)
		for _, fileHashStr := range files {
			this.PushToTrackers(fileHashStr, this.Config.TrackerUrls, this.Network.ListenAddr())
		}
	}
}
