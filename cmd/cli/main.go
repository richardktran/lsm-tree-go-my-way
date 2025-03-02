package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/constant"
	"github.com/richardktran/lsm-tree-go-my-way/internal/server"
	"github.com/richardktran/lsm-tree-go-my-way/internal/store/lsmtree"
)

const (
	Host = "127.0.0.1"
	Port = "6969"
)

func main() {
	hostPort := net.JoinHostPort(Host, Port)

	appConfig := config.Config{
		MemTableSizeThreshold: 60, // bytes
		SSTableBlockSize:      40, // bytes
		RootDataDir:           "./data",
	}

	dirConfig := config.DirectoryConfig{
		WALDir: "wal",
	}

	initDirs(appConfig.RootDataDir, &dirConfig)

	store := lsmtree.NewStore(appConfig, dirConfig)

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
		fmt.Print(response)
		conn.Close()

		fmt.Printf("%s> ", hostPort)
	}

}

func initDirs(rootDir string, dirConfig *config.DirectoryConfig) {
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		os.Mkdir(rootDir, os.ModePerm)
	}

	dirConfig.WALDir = fmt.Sprintf("%s/%s", rootDir, dirConfig.WALDir)
	if _, err := os.Stat(dirConfig.WALDir); os.IsNotExist(err) {
		os.Mkdir(dirConfig.WALDir, os.ModePerm)
	}
}
