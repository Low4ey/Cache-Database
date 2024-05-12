package main

import (
	"flag"
	"fmt"
	"log"

	// Uncomment this block to pass the first stage
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/models"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func main() {
	replicas := map[int]net.Conn{}
	var port = flag.Int("port", 6379, "The port number to listen on")
	var isReplica = flag.Bool("replicaof", false, "Running in Replica mod")
	var dir = flag.String("dir", "", "The directory where RDB files are stored")
	var rdbfilename = flag.String("dbfilename", "", "The name of the RDB file")
	flag.Parse()
	if *dir != "" {
		err := utils.ReadRdbFile(*dir, *rdbfilename)

		if err != nil {
			fmt.Println("Could not read RDB file:", err)

		}
	}
	nodeInfo := models.NodeInfo{
		Role:             "master",
		MasterReplid:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		MasterReplOffset: 0,
		MasterHost:       "",
		MasterPort:       "",
		NodePort:         *port,
		Dir:              *dir,
		Rdbfilename:      *rdbfilename,
	}
	if *isReplica {
		nodeInfo.Role = "slave"
		nodeInfo.MasterHost = flag.Args()[0]
		nodeInfo.MasterPort = flag.Args()[1]
		masterConnectionString := fmt.Sprintf("%s:%s", nodeInfo.MasterHost, nodeInfo.MasterPort)
		go utils.HandleReplica(masterConnectionString, nodeInfo, fmt.Sprintf("%d", nodeInfo.NodePort))
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go utils.HandleClient(conn, nodeInfo, replicas)
	}
}
