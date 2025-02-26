package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/richardktran/lsm-tree-go-my-way/cmd/cli/internal/constant"
	"github.com/richardktran/lsm-tree-go-my-way/cmd/cli/internal/store"
)

type Server struct {
	store *store.Store
	host  string
}

func NewServer(store *store.Store, host string) *Server {
	return &Server{
		store: store,
		host:  host,
	}
}

func (s *Server) StartServer() {
	listener, err := net.Listen("tcp", s.host)
	if err != nil {
		log.Fatal("Failed to start server: ", err)
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Failed to accepting connection: ", err)
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		fullCmd := scanner.Text()
		parts := strings.Fields(fullCmd)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])
		switch cmd {
		case constant.GET:
			s.handleGet(conn, parts)
		case constant.SET:
			s.handleSet(conn, parts)
		case constant.DEL:
			s.handleDelete(conn, parts)
		default:
			fmt.Fprintf(conn, "Unknown command: %s\n", cmd)
		}
	}
}

func (s *Server) handleGet(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		fmt.Fprintf(conn, "ERROR: %s command requires exactly 1 argument\n", constant.GET)
	}
	val, exists := s.store.Get(parts[1])
	if !exists {
		fmt.Fprintf(conn, "(nil)\n")
		return
	}

	fmt.Fprintf(conn, "%s\n", val)
}

func (s *Server) handleSet(conn net.Conn, parts []string) {
	if len(parts) != 3 {
		fmt.Fprintf(conn, "ERROR: %s command requires exactly 2 arguments\n", constant.SET)
		return
	}

	s.store.Set(parts[1], parts[2])
	fmt.Fprintf(conn, "OK\n")
}

func (s *Server) handleDelete(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		fmt.Fprintf(conn, "ERROR: %s command requires exactly 1 argument\n", constant.DEL)
		return
	}

	s.store.Delete(parts[1])
	fmt.Fprintf(conn, "OK\n")
}
