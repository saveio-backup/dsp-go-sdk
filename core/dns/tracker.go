package dns

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/types/score"
	uAddr "github.com/saveio/dsp-go-sdk/utils/addr"
	"github.com/saveio/dsp-go-sdk/utils/async"
	"github.com/saveio/dsp-go-sdk/utils/format"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

// SetupTrackers. setup DNS tracker url list
func (d *DNS) SetupTrackers() error {
	ns, err := d.chain.GetAllDnsNodes()
	if err != nil {
		return sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	if len(ns) == 0 {
		return sdkErr.New(sdkErr.DNS_NO_REGISTER_DNS, "no dns nodes from chain")
	}
	if d.trackerUrls == nil {
		d.trackerUrls = make([]string, 0)
	} else {
		d.trackerUrls = d.trackerUrls[:0]
	}
	reserved := format.StringsToMap(d.reservedDNS)
	for _, v := range ns {
		_, ok := reserved[v.WalletAddr.ToBase58()]
		if len(reserved) > 0 && !ok {
			continue
		}
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		if len(d.trackerUrls) >= d.maxNodeNum {
			break
		}
		trackerUrl := fmt.Sprintf("%s://%s:%d", d.trackerProtocol, v.IP, consts.TRACKER_SVR_DEFAULT_PORT)
		d.trackerUrls = append(d.trackerUrls, trackerUrl)
	}
	d.trackerUrls = format.RemoveDuplicated(append(d.trackerUrls, d.reservedTrackers...))
	log.Debugf("d.trackerUrls %v", d.trackerUrls)
	return nil
}

func (d *DNS) GetTrackerList() []string {
	return d.trackerUrls
}

// GetPeerFromTracker. get peer host addr from trackers
// return: ["tcp://127.0.0.1:1234"]
func (d *DNS) GetPeerFromTracker(hash string) []string {
	selfAddr := client.P2PGetPublicAddr()
	log.Infof("selfAddr %v, hash %v", selfAddr, hash)
	protocol := selfAddr[:strings.Index(selfAddr, "://")]

	request := func(trackerUrl string, resp chan *trackerResp) {
		peers, err := client.P2PTorrentPeers([]byte(hash), trackerUrl)
		if err != nil || len(peers) == 0 {
			resp <- &trackerResp{
				err: fmt.Errorf("peers is empty, err: %s", err),
			}
			return
		}
		peerAddrs := make([]string, 0)
		peerMap := make(map[string]struct{}, 0)
		for _, addr := range peers {
			if !strings.Contains(addr, protocol) {
				addr = fmt.Sprintf("%s://%s", protocol, addr)
			}
			if addr == selfAddr {
				continue
			}
			if _, ok := peerMap[addr]; ok || len(peerMap) > consts.MAX_PEERS_NUM_GET_FROM_TRACKER {
				continue
			}
			peerMap[addr] = struct{}{}
			peerAddrs = append(peerAddrs, addr)
		}
		if len(peerAddrs) == 0 {
			resp <- &trackerResp{
				err: fmt.Errorf("peers is empty"),
			}
			return
		}
		resp <- &trackerResp{
			ret:  peerAddrs,
			stop: true,
		}
	}
	result := d.requestTrackers(request)
	if result == nil {
		return nil
	}
	peerAddrs, ok := result.([]string)
	if !ok {
		log.Errorf("convert result to peers failed")
		return nil
	}
	return peerAddrs
}

// PushToTrackers. Push torrent file hash to trackers
// hash: "..."
// listenAddr: "tcp://127.0.0.1:1234"
func (d *DNS) PushToTrackers(hash string, listenAddr string) error {
	host, netPort, err := uAddr.ParseHostAddr(listenAddr)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INTERNAL_ERROR, err)
	}
	if len(d.trackerUrls) == 0 {
		return sdkErr.New(sdkErr.NO_CONNECTED_DNS, "no online dns connected")
	}
	log.Debugf("pushing file %s to trackers, listen addr is %v", hash, listenAddr)
	request := func(trackerUrl string, resp chan *trackerResp) {
		err := client.P2PCompleteTorrent([]byte(hash), host, uint64(netPort), trackerUrl)
		if err == nil {
			log.Debugf("push file %s to tracker %s  with hostAddr  %s success", hash, trackerUrl, listenAddr)
		} else {
			log.Errorf("push file %s to tracker %s  with hostAddr  %s err %s", hash, trackerUrl, listenAddr, err)
		}
		resp <- &trackerResp{
			ret: trackerUrl,
			err: err,
		}
	}
	d.requestTrackers(request)
	return nil
}

// PushFilesToTrackers. batch push a amount of files to trackers
func (d *DNS) PushFilesToTrackers(files []string) {
	log.Debugf("pushing %d files to %d trackers", len(files), len(d.trackerUrls))
	if len(d.trackerUrls) == 0 || len(files) == 0 {
		return
	}
	req := func(files []interface{}, resp chan *async.RequestResponse) {
		if len(files) != 1 {
			resp <- &async.RequestResponse{
				Error: sdkErr.New(sdkErr.INVALID_PARAMS, "invalid arguments %v", files),
			}
			return
		}
		fileHashStr, ok := files[0].(string)
		if !ok {
			resp <- &async.RequestResponse{
				Error: sdkErr.New(sdkErr.INVALID_PARAMS, "invalid arguments %v", files),
			}
			return
		}
		err := d.PushToTrackers(fileHashStr, client.P2PGetPublicAddr())
		if err != nil {
			resp <- &async.RequestResponse{
				Error: sdkErr.NewWithError(sdkErr.DNS_PUSH_TRACKER_ERROR, err),
			}
			return
		} else {
			log.Debugf("push file %s to trackers success", fileHashStr)
		}
		resp <- &async.RequestResponse{}
	}
	reqArgs := make([][]interface{}, 0, len(files))
	for _, fileHashStr := range files {
		reqArgs = append(reqArgs, []interface{}{fileHashStr})
	}
	async.RequestWithArgs(req, reqArgs)
	log.Debugf("push %d files to %d trackers done", len(files), len(d.trackerUrls))
}

// requestTrackers. send a request to trackers parallely
func (d *DNS) requestTrackers(request func(trackerUrl string, trackerResp chan *trackerResp)) interface{} {
	req := func(trackers []interface{}, resp chan *async.RequestResponse) bool {
		if len(trackers) != 1 {
			resp <- &async.RequestResponse{
				Error: sdkErr.New(sdkErr.INTERNAL_ERROR, "invalid arguments %v", trackers),
			}
			return false
		}
		trackerUrl, ok := trackers[0].(string)
		if !ok {
			resp <- &async.RequestResponse{
				Error: sdkErr.New(sdkErr.INTERNAL_ERROR, "invalid arguments %v", trackers),
			}
			return false
		}

		done := make(chan *trackerResp, 1)
		log.Debugf("start request to tracker %s", trackerUrl)
		go request(trackerUrl, done)
		for {
			select {
			case ret, ok := <-done:
				if !ok || ret == nil || ret.err != nil {
					resp <- &async.RequestResponse{
						Result: trackerUrl,
						Error:  sdkErr.NewWithError(sdkErr.INTERNAL_ERROR, ret.err),
					}
					return false
				}
				resp <- &async.RequestResponse{
					Result: ret.ret,
				}
				return ret.stop
			case <-time.After(time.Duration(consts.TRACKER_SERVICE_TIMEOUT) * time.Second):
				log.Errorf("request to tracker %s timeout", trackerUrl)
				resp <- &async.RequestResponse{
					Result: trackerUrl,
					Error:  sdkErr.New(sdkErr.DNS_TRACKER_TIMEOUT, "request to tracker %s timeout", trackerUrl),
				}
				return false
			}
		}
	}
	reqArgs := make([][]interface{}, 0, len(d.trackerUrls))
	for _, u := range d.trackerUrls {
		reqArgs = append(reqArgs, []interface{}{u})
	}
	if len(reqArgs) == 0 {
		log.Warnf("no args for request")
		return nil
	}
	resp := async.RequestOneWithArgs(req, reqArgs)
	results := make([]interface{}, 0, len(reqArgs))
	for _, r := range resp {
		if r.Error == nil || r.Error.Code == sdkErr.SUCCESS {
			results = append(results, r.Result)
			continue
		}
		trackerUrl, ok := r.Result.(string)
		if !ok {
			continue
		}
		scoreVal, ok := d.trackerScore.Load(trackerUrl)
		var trackerScore *score.Score
		if !ok {
			trackerScore = score.NewScore(trackerUrl)
		} else {
			trackerScore, _ = scoreVal.(*score.Score)
		}
		if r.Error != nil && r.Error.HasErrorCode(sdkErr.DNS_TRACKER_TIMEOUT) {
			trackerScore.Timeout()
		} else {
			trackerScore.Fail()
		}
		log.Debugf("update tracker %s score is %s", trackerUrl, trackerScore)
		d.trackerScore.Store(trackerUrl, trackerScore)
		d.sortTrackers()
	}
	if len(results) > 0 {
		return results[0]
	}
	return nil
}

// GetExternalIP. get external ip of wallet from dns nodes
// return ["tcp://127.0.0.1:1234"], nil
func (d *DNS) GetExternalIP(walletAddr string) (string, error) {
	info, ok := d.publicAddrCache.Get(walletAddr)
	var oldHostAddr string
	if ok && info != nil {
		addrInfo, ok := info.(*types.NodeInfo)
		now := uTime.GetMilliSecTimestamp()
		if ok && addrInfo != nil && uint64(addrInfo.UpdatedAt+consts.MAX_PUBLIC_IP_UPDATE_SECOND*consts.MILLISECOND_PER_SECOND) > now && len(addrInfo.HostAddr) > 0 {
			log.Debugf("GetExternalIP %s addr %s from cache", walletAddr, addrInfo.HostAddr)
			return addrInfo.HostAddr, nil
		}
		if addrInfo != nil {
			oldHostAddr = addrInfo.HostAddr
			log.Debugf("wallet: %s, old host ip :%s, now :%d", walletAddr, addrInfo.HostAddr, addrInfo.UpdatedAt)
		}
	}
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		log.Errorf("address from b58 failed %s", err)
		return "", sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	if len(d.trackerUrls) == 0 {
		log.Warn("GetExternalIP no trackers")
	}
	request := func(url string, resp chan *trackerResp) {
		hostAddr, err := client.P2PGetEndpointAddr(address, url)
		log.Debugf("ReqEndPoint hostAddr url: %s, address %s, hostaddr:%s", url, address.ToBase58(), string(hostAddr))
		if err != nil {
			resp <- &trackerResp{
				err: err,
			}
			return
		}
		if len(string(hostAddr)) == 0 || !uAddr.IsHostAddrValid(string(hostAddr)) {
			resp <- &trackerResp{
				err: fmt.Errorf("invalid hostAddr %s", hostAddr),
			}
			return
		}
		hostAddrStr := uAddr.ConvertToFullHostAddr(d.channelProtocol, string(hostAddr))
		log.Debugf("GetExternalIP %s :%v from %s", walletAddr, string(hostAddrStr), url)
		if strings.Index(hostAddrStr, "0.0.0.0:0") != -1 {
			resp <- &trackerResp{
				err: fmt.Errorf("invalid hostAddr %s", hostAddr),
			}
			return
		}
		resp <- &trackerResp{
			ret:  hostAddrStr,
			stop: true,
		}
	}
	result := d.requestTrackers(request)
	log.Debugf("get external ip wallet %s, result %v", walletAddr, result)
	if result == nil {
		if len(oldHostAddr) > 0 {
			return oldHostAddr, nil
		}
		hostAddrFromChain, err := d.GetDNSHostAddrFromChain(walletAddr)
		if err != nil {
			return "", sdkErr.New(sdkErr.DNS_REQ_TRACKER_ERROR, "request tracker result is nil : %v", result)
		}
		log.Debugf("get host addr %s of %s from chain", walletAddr, hostAddrFromChain)
		result = hostAddrFromChain
	}
	hostAddrStr, ok := result.(string)
	if !ok {
		if len(oldHostAddr) > 0 {
			return oldHostAddr, nil
		}
		log.Errorf("convert result to string failed: %v", result)
		return "", sdkErr.New(sdkErr.INTERNAL_ERROR, "convert result to string failed: %v", result)
	}
	d.publicAddrCache.Add(walletAddr, &types.NodeInfo{
		HostAddr:  hostAddrStr,
		UpdatedAt: uTime.GetMilliSecTimestamp(),
	})
	return hostAddrStr, nil
}

// sortTrackers. sort trackers by their score
func (d *DNS) sortTrackers() {
	scores := make(score.Scores, 0)
	for _, u := range d.trackerUrls {
		scoreVal, ok := d.trackerScore.Load(u)
		s := score.NewScore(u)
		if ok {
			s = scoreVal.(*score.Score)
		}
		scores = append(scores, *s)
	}
	sort.Sort(sort.Reverse(scores))
	newTrackers := make([]string, 0)
	for _, s := range scores {
		newTrackers = append(newTrackers, s.Id())
	}
	d.trackerUrls = newTrackers
	log.Debugf("after sort, new track urls %v", d.trackerUrls)
}
