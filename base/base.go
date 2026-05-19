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

type DroneStatus struct {
	Status string `json:"status"`
}

type Drone struct {
	ID            string
	Status        string // "LIVRE", "OCUPADO", "RETORNANDO_BASE"
	LastHeartbeat time.Time
	Mutex         sync.Mutex
}

var (
	base_id     string
	base_drones []*Drone
	droneMutex  sync.RWMutex
)

func main() {
	base_id = os.Args[1]
	port := os.Args[2]
	num_drones, _ := strconv.Atoi(os.Args[3])

	// Inicializa a frota de forma escalável
	for i := 1; i <= num_drones; i++ {
		base_drones = append(base_drones, &Drone{
			ID:            fmt.Sprintf("Drone_%02d", i),
			Status:        "LIVRE",
			LastHeartbeat: time.Now(),
		})
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	fmt.Printf("[%s] Operacional com %d drones.\n", base_id, num_drones)

	// Inicia verificação periódica de drones
	go startDroneHealthCheck()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleBrokerRequest(conn)
	}
}

// startDroneHealthCheck verifica periodicamente a saúde dos drones
func startDroneHealthCheck() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		droneMutex.RLock()
		for _, d := range base_drones {
			d.Mutex.Lock()
			// Se um drone está OCUPADO por mais de 2 minutos, considerar timeout
			if d.Status == "OCUPADO" && time.Since(d.LastHeartbeat) > 2*time.Minute {
				fmt.Printf("[%s] ⚠️ TIMEOUT: %s está ocupado há muito tempo. Retornando a LIVRE.\n", base_id, d.ID)
				d.Status = "LIVRE"
			}
			d.Mutex.Unlock()
		}
		droneMutex.RUnlock()
	}
}

func handleBrokerRequest(conn net.Conn) {
	defer conn.Close()
	var cmd DroneCommand
	if err := json.NewDecoder(conn).Decode(&cmd); err != nil {
		fmt.Printf("[%s] Erro ao decodificar comando: %v\n", base_id, err)
		return
	}

	if cmd.Action == "DISPATCH" {
		droneLivre := findFreeDrone()
		if droneLivre != nil {
			// Drone alocado com sucesso
			go executeMission(droneLivre, cmd)
			json.NewEncoder(conn).Encode(map[string]string{
				"status":   "ACCEPTED",
				"drone_id": droneLivre.ID,
				"base_id":  base_id,
			})
			fmt.Printf("[%s] ✅ DISPATCH ACEITO: %s para setor %s (broker: %s)\n",
				base_id, droneLivre.ID, cmd.Target, cmd.BrokerID)
		} else {
			// A base avisa que não tem drones disponíveis no momento
			json.NewEncoder(conn).Encode(map[string]string{
				"status":  "REJECTED_NO_DRONES",
				"base_id": base_id,
			})
			fmt.Printf("[%s] ❌ DISPATCH REJEITADO: Sem drones disponíveis\n", base_id)
		}

	} else if cmd.Action == "STATUS" {
		droneMutex.RLock()
		for _, d := range base_drones {
			if d.ID == cmd.Target {
				d.Mutex.Lock()
				status := d.Status
				d.Mutex.Unlock()
				droneMutex.RUnlock()
				json.NewEncoder(conn).Encode(map[string]string{"status": status})
				return
			}
		}
		droneMutex.RUnlock()
		// Drone não encontrado
		json.NewEncoder(conn).Encode(map[string]string{"status": "NOT_FOUND"})
	}
}

func findFreeDrone() *Drone {
	droneMutex.Lock()
	defer droneMutex.Unlock()

	for _, d := range base_drones {
		d.Mutex.Lock()
		if d.Status == "LIVRE" {
			d.Status = "OCUPADO"
			d.LastHeartbeat = time.Now()
			d.Mutex.Unlock()
			return d
		}
		d.Mutex.Unlock()
	}
	return nil
}

func executeMission(d *Drone, cmd DroneCommand) {
	fmt.Printf("[%s] 🚁 %s decolando para %s a pedido de %s\n",
		base_id, d.ID, cmd.Target, cmd.BrokerID)

	// Simula voo e atendimento (10 segundos)
	time.Sleep(10 * time.Second)

	d.Mutex.Lock()
	d.Status = "LIVRE"
	d.LastHeartbeat = time.Now()
	d.Mutex.Unlock()

	fmt.Printf("[%s] ✅ %s retornou e está LIVRE.\n", base_id, d.ID)
}
