package vega

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strconv"

	crand "crypto/rand"
)

// Lovely borrowed from consul

/*
 * Contains an entry for each private block:
 * 10.0.0.0/8
 * 172.16.0.0/12
 * 192.168/16
 */
var privateBlocks []*net.IPNet

var randSrc rand.Source
var randGen *rand.Rand

func init() {
	// Add each private block
	privateBlocks = make([]*net.IPNet, 3)
	_, block, err := net.ParseCIDR("10.0.0.0/8")
	if err != nil {
		panic(fmt.Sprintf("Bad cidr. Got %v", err))
	}
	privateBlocks[0] = block

	_, block, err = net.ParseCIDR("172.16.0.0/12")
	if err != nil {
		panic(fmt.Sprintf("Bad cidr. Got %v", err))
	}
	privateBlocks[1] = block

	_, block, err = net.ParseCIDR("192.168.0.0/16")
	if err != nil {
		panic(fmt.Sprintf("Bad cidr. Got %v", err))
	}
	privateBlocks[2] = block

	var n int64
	binary.Read(crand.Reader, binary.BigEndian, &n)

	randSrc = rand.NewSource(n)

	randGen = rand.New(randSrc)
}

// Returns if the given IP is in a private block
func isPrivateIP(ip_str string) bool {
	ip := net.ParseIP(ip_str)
	for _, priv := range privateBlocks {
		if priv.Contains(ip) {
			return true
		}
	}
	return false
}

// GetPrivateIP is used to return the first private IP address
// associated with an interface on the machine
func GetPrivateIP() (net.IP, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("Failed to get interface addresses: %v", err)
	}

	// Find private IPv4 address
	for _, rawAddr := range addresses {
		var ip net.IP
		switch addr := rawAddr.(type) {
		case *net.IPAddr:
			ip = addr.IP
		case *net.IPNet:
			ip = addr.IP
		default:
			continue
		}

		if ip.To4() == nil {
			continue
		}
		if !isPrivateIP(ip.String()) {
			continue
		}

		return ip, nil
	}

	return nil, fmt.Errorf("No private IP address found")
}

// runtimeStats is used to return various runtime information
func runtimeStats() map[string]string {
	return map[string]string{
		"os":         runtime.GOOS,
		"arch":       runtime.GOARCH,
		"version":    runtime.Version(),
		"max_procs":  strconv.FormatInt(int64(runtime.GOMAXPROCS(0)), 10),
		"goroutines": strconv.FormatInt(int64(runtime.NumGoroutine()), 10),
		"cpu_count":  strconv.FormatInt(int64(runtime.NumCPU()), 10),
	}
}

// generateUUID is used to generate a random UUID
func generateUUID() string {
	uuid := make([]byte, 16)

	for i := 0; i < 16; i += 8 {
		binary.BigEndian.PutUint64(uuid[i:i+8], uint64(randGen.Int63()))
	}

	// if _, err := rand.Read(uuid); err != nil {
	// panic(fmt.Errorf("failed to read random bytes: %v", err))
	// }

	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant is 10

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		uuid[0:4],
		uuid[4:6],
		uuid[6:8],
		uuid[8:10],
		uuid[10:16])
}

// generateUUID is used to generate a random UUID
func generateUUIDSecure() string {
	uuid := make([]byte, 16)

	if _, err := crand.Read(uuid); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant is 10

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		uuid[0:4],
		uuid[4:6],
		uuid[6:8],
		uuid[8:10],
		uuid[10:16])
}

func RandomQueue() string {
	return "gen-" + generateUUID()
}

func RandomID() string {
	return "m" + generateUUID()
}
