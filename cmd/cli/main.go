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
	appConfig := config.Config{
		Host:                  Host,
		Port:                  Port,
		MemTableSizeThreshold: 30, // bytes
		SSTableBlockSize:      20, // bytes
		SparseWALBufferSize:   2,  // records
		RootDataDir:           "./data",
	}

	dirConfig := config.DirectoryConfig{
		WALDir:         "wal",
		SSTableDir:     "sstables",
		SparseIndexDir: "sstables/indexes",
	}

	initDirs(appConfig.RootDataDir, &dirConfig)

	store := lsmtree.NewStore(&appConfig, &dirConfig)
	defer store.Close()

	hostPort := net.JoinHostPort(appConfig.Host, appConfig.Port)

	svr := server.NewServer(store, hostPort)

	go svr.StartServer()

	startCLI(hostPort)
}

// StartCLI starts the CLI for the user to interact with the server
// Listen the server response and print it to the console
func startCLI(hostPort string) {
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

// initDirs adds the root directory to the beginning of all the directories in the DirectoryConfig
// and creates the directories if they do not exist.
func initDirs(rootDir string, dirConfig *config.DirectoryConfig) {
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		os.Mkdir(rootDir, os.ModePerm)
	}

	dirs := []*string{
		&dirConfig.WALDir,
		&dirConfig.SSTableDir,
		&dirConfig.SparseIndexDir,
	}
	for _, dir := range dirs {
		*dir = fmt.Sprintf("%s/%s", rootDir, *dir)
		if _, err := os.Stat(*dir); os.IsNotExist(err) {
			os.Mkdir(*dir, os.ModePerm)
		}
	}
}
