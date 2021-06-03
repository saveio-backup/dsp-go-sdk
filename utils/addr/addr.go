package addr

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/saveio/themis/common/log"
)

// ConvertToFullHostAddr. format full host address with protocol and host addr
func ConvertToFullHostAddr(protocol, hostAddr string) string {
	if len(protocol) == 0 {
		log.Warnf("full host addr no protocol: %s", protocol)
		protocol = "tcp"
	}
	if strings.Index(hostAddr, protocol) != -1 {
		return hostAddr
	}
	return fmt.Sprintf("%s://%s", protocol, hostAddr)
}

// ConvertToFullHostAddrWithPort. format full host address with protocol and host addr
func ConvertToFullHostAddrWithPort(protocol, hostAddr string, port interface{}) string {
	if len(protocol) == 0 {
		log.Warnf("full host addr no protocol: %s", protocol)
		protocol = "tcp"
	}
	if strings.Index(hostAddr, protocol) != -1 {
		return fmt.Sprintf("%s:%v", hostAddr, port)
	}
	return fmt.Sprintf("%s://%s:%v", protocol, hostAddr, port)
}

// IsHostAddrValid. is host addr valid
func IsHostAddrValid(hostAddr string) bool {
	host := hostAddr
	if strings.Index(host, "0.0.0.0:0") != -1 {
		return false
	}
	protocolIdx := strings.Index(hostAddr, "://")
	if protocolIdx != -1 {
		host = hostAddr[protocolIdx+3:]
	}
	portIndex := strings.Index(host, ":")
	if portIndex != -1 {
		host = host[:portIndex]
	}
	ip := net.ParseIP(host)
	log.Debugf("hostAddr %s, host %s, ip %v", hostAddr, host, ip)
	if ip != nil {
		return true
	}
	return false
}

// ParseHostAddr. parse host addr to get host and port
// e.g tcp://127.0.0.1:8080 return 127.0.0.1, 8080
// e.g 127.0.0.1:8080 return 127.0.0.1, 8080
func ParseHostAddr(hostAddr string) (string, int, error) {
	index := strings.Index(hostAddr, "://")

	// hostPort=ip:port
	hostPort := hostAddr
	if index != -1 {
		hostPort = hostAddr[index+3:]
	}
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", 0, err
	}
	netPort, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, err
	}
	return host, netPort, nil
}

// GetProtocolOfHostAddr. get protocol of a host address
func GetProtocolOfHostAddr(hostAddr string) string {
	protocolIdx := strings.Index(hostAddr, "://")
	if protocolIdx == -1 {
		return ""
	}
	return hostAddr[:protocolIdx]
}
