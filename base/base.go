package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// Estrutura genérica para comandos recebidos de Brokers ou de Drones autónomos
type GenericCommand struct {
	Action   string `json:"action"`    // "DISPATCH", "STATUS", "REGISTER", "MISSION_COMPLETE", "HEARTBEAT"
	Target   string `json:"target"`    // Setor alvo (para Broker) ou Drone ID (para STATUS)
	BrokerID string `json:"broker_id"` // ID do Broker solicitante
	DroneID  string `json:"drone_id"`  // ID do Drone (usado no REGISTRO)
}

type Drone struct {
	ID            string
	Status        string // "LIVRE", "OCUPADO"
	Conn          net.Conn
	LastHeartbeat time.Time
	Mutex         sync.Mutex
}

var (
	base_id     string
	base_drones = make(map[string]*Drone)
	droneMutex  sync.RWMutex
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Uso: ./base <BASE_ID> <PORT>")
		return
	}

	base_id = os.Args[1]
	port := os.Args[2]

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	fmt.Printf("[%s] Base operacional. Aguardando registo de drones e pedidos de brokers...\n", base_id)

	// Inicia verificação periódica de saúde dos drones ativos
	go startDroneHealthCheck()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleIncomingConnection(conn)
	}
}

// startDroneHealthCheck monitoriza se os drones continuam vivos na rede
func startDroneHealthCheck() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		droneMutex.Lock()
		now := time.Now()
		for id, d := range base_drones {
			d.Mutex.Lock()
			// Se o container do drone não enviar sinal por mais de 15 segundos, assume queda
			if now.Sub(d.LastHeartbeat) > 15*time.Second {
				fmt.Printf("[%s] ⚠️ TIMEOUT: %s perdeu ligação (Heartbeat expirado). Removendo da frota.\n", base_id, d.ID)
				d.Conn.Close()
				delete(base_drones, id)
			}
			d.Mutex.Unlock()
		}
		droneMutex.Unlock()
	}
}

func handleIncomingConnection(conn net.Conn) {
	var cmd GenericCommand
	// Decodifica a primeira mensagem recebida para identificar a origem (Drone ou Broker)
	if err := json.NewDecoder(conn).Decode(&cmd); err != nil {
		fmt.Printf("[%s] Erro ao decodificar mensagem inicial: %v\n", base_id, err)
		conn.Close()
		return
	}

	switch cmd.Action {
	case "REGISTER":
		// Conexão persistente com o drone: o socket DEVE continuar aberto
		handleDroneConnection(conn, cmd.DroneID)

	case "DISPATCH":
		defer conn.Close()
		handleBrokerDispatch(conn, cmd)

	case "STATUS":
		defer conn.Close()
		handleBrokerStatus(conn, cmd)

	default:
		conn.Close()
	}
}

// handleDroneConnection gere o ciclo de vida e mensagens vindas do socket do Drone
func handleDroneConnection(conn net.Conn, droneID string) {
	d := &Drone{
		ID:            droneID,
		Status:        "LIVRE",
		Conn:          conn,
		LastHeartbeat: time.Now(),
	}

	droneMutex.Lock()
	// Substitui conexões antigas caso o mesmo drone reinicie com o mesmo ID
	if oldDrone, exists := base_drones[droneID]; exists {
		oldDrone.Conn.Close()
	}
	base_drones[droneID] = d
	droneMutex.Unlock()

	fmt.Printf("[%s] ➕ NOVO DRONE CONECTADO E REGISTADO: %s\n", base_id, droneID)

	decoder := json.NewDecoder(conn)
	for {
		var update GenericCommand
		if err := decoder.Decode(&update); err != nil {
			// Ligação com o container do drone quebrou
			fmt.Printf("[%s] ❌ CONEXÃO PERDIDA: O drone %s desconectou-se da rede.\n", base_id, droneID)
			droneMutex.Lock()
			if current, exists := base_drones[droneID]; exists && current.Conn == conn {
				delete(base_drones, droneID)
			}
			droneMutex.Unlock()
			conn.Close()
			break
		}

		d.Mutex.Lock()
		d.LastHeartbeat = time.Now() // Atualiza timestamp de atividade

		if update.Action == "HEARTBEAT" {
			// Apenas mantém o drone ativo no HealthCheck
		} else if update.Action == "MISSION_COMPLETE" {
			d.Status = "LIVRE"
			fmt.Printf("[%s] ✅ %s concluiu o atendimento com sucesso e voltou a ficar LIVRE.\n", base_id, droneID)
		}
		d.Mutex.Unlock()
	}
}

func handleBrokerDispatch(conn net.Conn, cmd GenericCommand) {
	droneLivre := findFreeDrone()
	if droneLivre != nil {
		// Envia a ordem de voo de forma assíncrona ao drone correspondente através do socket ativo
		go executeMission(droneLivre, cmd)

		json.NewEncoder(conn).Encode(map[string]string{
			"status":   "ACCEPTED",
			"drone_id": droneLivre.ID,
			"base_id":  base_id,
		})
		fmt.Printf("[%s] ✅ DISPATCH ACEITO: %s alocado para o setor %s (broker: %s)\n",
			base_id, droneLivre.ID, cmd.Target, cmd.BrokerID)
	} else {
		json.NewEncoder(conn).Encode(map[string]string{
			"status":  "REJECTED_NO_DRONES",
			"base_id": base_id,
		})
		fmt.Printf("[%s] ❌ DISPATCH REJEITADO: Frota local totalmente ocupada\n", base_id)
	}
}

func handleBrokerStatus(conn net.Conn, cmd GenericCommand) {
	droneMutex.RLock()
	d, exists := base_drones[cmd.Target]
	droneMutex.RUnlock()

	if exists {
		d.Mutex.Lock()
		status := d.Status
		d.Mutex.Unlock()
		json.NewEncoder(conn).Encode(map[string]string{"status": status})
	} else {
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

func executeMission(d *Drone, cmd GenericCommand) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	fmt.Printf("[%s] 🚁 Despachando comando de missão via TCP para %s (Alvo: %s)\n", base_id, d.ID, cmd.Target)

	payload := map[string]string{
		"action": "FLY",
		"target": cmd.Target,
	}

	// Escreve a instrução diretamente no buffer de rede do drone conectado
	if err := json.NewEncoder(d.Conn).Encode(payload); err != nil {
		fmt.Printf("[%s] ⚠️ Falha crítica ao enviar payload para %s: %v. Retornando a LIVRE.\n", base_id, d.ID, err)
		d.Status = "LIVRE"
	}
}
