package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type DroneCommand struct {
	Action   string `json:"action"`
	Target   string `json:"target"`
	BrokerID string `json:"broker_id"`
}

type Drone struct {
	ID     string
	Status string // "LIVRE", "OCUPADO", "RETORNANDO_BASE"
	Mutex  sync.Mutex
}

var (
	base_id     string
	base_drones []*Drone
)

func main() {
	// Captura configurações via argumentos ou variáveis de ambiente (útil pro Docker)
	if len(os.Args) < 3 {
		fmt.Println("Uso: go run drone_base.go <ID_DA_BASE> <PORTA> <QTD_DRONES>")
		return
	}

	base_id = os.Args[1]
	port := os.Args[2]
	num_drones, _ := strconv.Atoi(os.Args[3])

	// Inicializa a frota de forma escalável
	for i := 1; i <= num_drones; i++ {
		base_drones = append(base_drones, &Drone{
			ID:     fmt.Sprintf("%s-Drone-%02d", base_id, i),
			Status: "LIVRE",
		})
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	fmt.Printf("[%s] Operacional na porta %s com %d drones.\n", base_id, port, num_drones)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleBrokerRequest(conn)
	}
}

func handleBrokerRequest(conn net.Conn) {
	defer conn.Close()
	var cmd DroneCommand
	json.NewDecoder(conn).Decode(&cmd)

	if cmd.Action == "DISPATCH" {
		droneLivre := findFreeDrone()
		if droneLivre != nil {
			go executeMission(droneLivre, cmd)
			json.NewEncoder(conn).Encode(map[string]string{
				"status":   "ACCEPTED",
				"drone_id": droneLivre.ID,
				"base_id":  base_id,
			})
		} else {
			// A base avisa que não tem drones disponíveis no momento
			json.NewEncoder(conn).Encode(map[string]string{
				"status":  "REJECTED_NO_DRONES",
				"base_id": base_id,
			})
		}
	} else if cmd.Action == "STATUS" {
		for _, d := range base_drones {
			if d.ID == cmd.Target {
				d.Mutex.Lock()
				status := d.Status
				d.Mutex.Unlock()
				json.NewEncoder(conn).Encode(map[string]string{"status": status})
				return
			}
		}
	}
}

func findFreeDrone() *Drone {
	for _, d := range base_drones {
		d.Mutex.Lock()
		if d.Status == "LIVRE" {
			d.Status = "OCUPADO"
			d.Mutex.Unlock()
			return d
		}
		d.Mutex.Unlock()
	}
	return nil
}

func executeMission(d *Drone, cmd DroneCommand) {
	fmt.Printf("[%s] Decolando para %s a pedido de %s\n", d.ID, cmd.Target, cmd.BrokerID)
	time.Sleep(10 * time.Second) // Simula voo e atendimento

	d.Mutex.Lock()
	d.Status = "LIVRE" // Simplificado para focar na alocação
	d.Mutex.Unlock()
	fmt.Printf("[%s] Retornou e está LIVRE.\n", d.ID)
}
