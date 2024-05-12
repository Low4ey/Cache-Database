package utils

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

func HandleClient(conn net.Conn, nodeInfo models.NodeInfo, replicas map[int]net.Conn) {
	defer conn.Close()
	for {
		pong := []byte("+PONG\r\n")
		buf := make([]byte, 1024)
		res, err := conn.Read(buf)
		if err != nil {
			conn.Write([]byte(err.Error()))
		}
		result_string := strings.Split(string(buf[:res]), "\r\n")
		command := extract_Command(result_string)
		if command == "echo" {
			result := handleEcho(result_string)
			conn.Write([]byte(result))
		} else if command == "set" {
			handleSet(result_string)
			ok := "+OK\r\n"
			if nodeInfo.Role == "master" {
				conn.Write([]byte(ok))
			}
			for _, replica := range replicas {
				_, err := replica.Write([]byte(string(buf[:res])))
				if err != nil {
					fmt.Print(err)
				}
				time.Sleep(time.Millisecond * 20)
			}
		} else if command == "get" {
			result := handleGet(result_string)
			conn.Write([]byte(result))
		} else if command == "ping" {
			conn.Write(pong)
		} else if command == "info" {
			result := handleInfo(nodeInfo)
			conn.Write([]byte(result))
		} else if command == "replconf" {
			replicas[len(replicas)] = conn
			conn.Write([]byte("+OK\r\n"))
		} else if command == "psync" {
			result1, result2 := handlePsync(nodeInfo)
			conn.Write([]byte(result1))
			conn.Write([]byte(result2))
		} else if command == "config" {
			result := handleConfig(nodeInfo, result_string)
			fmt.Print(result)
			conn.Write([]byte(result))
		} else if command == "keys" {
			result := getKey()
			conn.Write([]byte(result))
		} else {
			conn.Write([]byte("-invalidcommand\r\n"))
		}

	}
}

func HandleReplica(masterConnectionString string, nodeInfo models.NodeInfo, nodePort string) {

	conn, err := net.Dial("tcp", masterConnectionString)

	if err != nil {

		fmt.Println("Failed to connect to master")

		os.Exit(1)

	}
	defer conn.Close()
	conn.Write([]byte("*1\r\n$4\r\nping\r\n"))
	conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%s\r\n", nodePort)))
	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	for {
		buf := make([]byte, 1024)
		res, err := conn.Read(buf)
		if err != nil {
			conn.Write([]byte(err.Error()))
		}
		command := strings.Split(string(buf[:res]), "\r\n")
		fmt.Print(res)
		for i := 2; i < len(command); i++ {
			if command[i] == "SET" {
				rdb[command[i+2]] = command[i+4]
			} else if command[i] == "GET" {
				value := rdb[command[i+2]]
				response := "+" + value + "\r\n"
				conn.Write([]byte(response))
			} else if command[i] == "REPLCONF" {
				fmt.Print("HERE")
				response := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
				conn.Write([]byte(response))
			}
		}
	}
}
