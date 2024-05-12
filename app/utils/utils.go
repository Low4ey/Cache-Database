package utils

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

var (
	rdb         = make(map[string]string)
	setRdbMutex sync.Mutex
	getRdbMutex sync.Mutex
	delMu       sync.Mutex
)

func extract_Command(key []string) (command string) {
	if len(key) > 2 {
		return strings.ToLower(key[2])
	} else {
		return "PING"
	}
}
func handleEcho(key []string) (result string) {
	result = "+" + key[4] + "\r\n"
	return result
}

func get_exp_timer(key []string) (time int) {
	time, _ = strconv.Atoi(key[10])
	return time
}

func get_empty_rdb(EMPTY_RDB_HEX string) string {
	bytes, err := hex.DecodeString(EMPTY_RDB_HEX)
	if err != nil {
		fmt.Println("Error parsing hex")
		return ""
	}
	return fmt.Sprintf("$%d\r\n%s", len(bytes), bytes)
}

func handleSet(commands []string) {
	key, value := commands[4], commands[6]
	setRdbMutex.Lock()
	rdb[key] = value
	defer setRdbMutex.Unlock()
	if len(commands) > 8 {
		exp_time := get_exp_timer(commands)
		timer := time.After(time.Duration(exp_time) * time.Millisecond)
		go func() {
			<-timer
			delMu.Lock()
			delete(rdb, key)
			delMu.Unlock()
		}()
	}
}

func handleInfo(nodeInfo models.NodeInfo) string {
	res := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n", nodeInfo.Role, nodeInfo.MasterReplid, nodeInfo.MasterReplOffset)
	result := fmt.Sprintf("$%d\r\n%s\r\n", len(res), res)
	return result
}

func handleGet(command []string) string {
	var response string
	key := command[4]
	getRdbMutex.Lock()
	value := rdb[key]
	defer getRdbMutex.Unlock()
	if len(value) > 1 {
		response = "+" + value + "\r\n"
		return response
	} else {
		response = "$-1\r\n"
		return response
	}
}

func handlePsync(nodeInfo models.NodeInfo) (string, string) {
	res := fmt.Sprintf("+FULLRESYNC %s %d\r\n", nodeInfo.MasterReplid, nodeInfo.MasterReplOffset)
	file := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	empty_rdb := get_empty_rdb(file)
	return res, empty_rdb
}

func handleConfig(nodeInfo models.NodeInfo, command []string) string {
	var res string
	if command[6] == "dir" {
		res = fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(nodeInfo.Dir), nodeInfo.Dir)
	} else {
		res = fmt.Sprintf("*2\r\n$3\r\ndbfilename\r\n$%d\r\n%s\r\n", len(nodeInfo.Rdbfilename), nodeInfo.Rdbfilename)
	}
	return res
}

func getKey() string {
	result := "\r\n"
	var res string
	count := 0
	for key := range rdb {
		res = key
		temp := fmt.Sprintf("$%d\r\n%s\r\n", len(res), res)
		result = result + temp
		count = count + 1
	}
	result = "*" + fmt.Sprintf("%d", count) + result
	return result
}
