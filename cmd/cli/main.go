package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/richardktran/lsm-tree-go-my-way/cmd/cli/internal/constant"
	"github.com/richardktran/lsm-tree-go-my-way/cmd/cli/internal/server"
	"github.com/richardktran/lsm-tree-go-my-way/cmd/cli/internal/store"
)

const (
	Host = "127.0.0.1"
	Port = "6969"
)

func main() {
	hostPort := fmt.Sprintf("%s:%s", Host, Port)
	store := store.NewStore()
	svr := server.NewServer(store, hostPort)

	go svr.StartServer()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("SLM-Tree Go My Way CLI (type QUIT to exit)")
	fmt.Printf("%s> ", hostPort)
	for scanner.Scan() {
		cmd := scanner.Text()
		parts := strings.Fields(cmd)

		if len(parts) == 0 {
			fmt.Printf("%s> ", hostPort)
			continue
		}

		if strings.ToUpper(cmd) == constant.QUIT {
			break
		}

		conn, err := net.Dial("tcp", hostPort)
		if err != nil {
			fmt.Println("Failed to connect to server: ", err)
			return
		}

		// Send command to server
		fmt.Fprintf(conn, "%s\n", cmd)

		response, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Println(response)
		conn.Close()

		fmt.Printf("%s> ", hostPort)
	}

}
