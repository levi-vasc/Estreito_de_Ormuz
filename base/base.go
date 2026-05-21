package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// Define o contrato do payload JSON utilizado para o roteamento
// de mensagens inter-serviços (Brokers, Bases e Drones).
type GenericCommand struct {
	Action   string `json:"action"`
	Target   string `json:"target"`
	BrokerID string `json:"broker_id"`
	DroneID  string `json:"drone_id"`
}

// Encapsula o estado de alocação, a conexão TCP bidirecional persistente e
// os primitivos de sincronização para garantir acesso aos atributos da unidade.
type Drone struct {
	ID            string
	Status        string
	Conn          net.Conn
	LastHeartbeat time.Time
	Mutex         sync.Mutex
}

var (
	base_id      string
	base_drones  = make(map[string]*Drone)
	droneMutex   sync.RWMutex
	droneCounter int
)

// Inicializa a Base Operacional, realizando o bind do servidor TCP na porta especificada,
// e inicia as rotinas independentes de varredura de health check e o listener multiplexador.
func main() {
	base_id = os.Args[1]
	port := os.Args[2]

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	fmt.Printf("[%s] Base operacional na porta %s. Aguardando conexões...\n", base_id, port)

	go startDroneHealthCheck()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleIncomingConnection(conn)
	}
}

// Executa uma rotina de monitoramento periódico para identificar e
// fechar sockets pendentes ("zombie connections") de drones que excederam o timeout.
func startDroneHealthCheck() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		droneMutex.Lock()
		now := time.Now()
		for id, d := range base_drones {
			d.Mutex.Lock()
			if now.Sub(d.LastHeartbeat) > 15*time.Second {
				fmt.Printf("[%s] ⚠️ TIMEOUT: %s perdeu ligação. Removendo da frota.\n", base_id, d.ID)
				d.Conn.Close()
				delete(base_drones, id)
			}
			d.Mutex.Unlock()
		}
		droneMutex.Unlock()
	}
}

// Decodifica a mensagem TCP inicial e atua como um roteador de chamadas
// com base na propriedade "Action", delegando o socket para o handler apropriado.
func handleIncomingConnection(conn net.Conn) {
	var cmd GenericCommand
	if err := json.NewDecoder(conn).Decode(&cmd); err != nil {
		fmt.Printf("[%s] Erro ao decodificar mensagem inicial: %v\n", base_id, err)
		conn.Close()
		return
	}

	switch cmd.Action {
	case "REGISTER":
		droneMutex.Lock()
		droneCounter++
		assignedID := fmt.Sprintf("Drone_%d", droneCounter)
		droneMutex.Unlock()

		json.NewEncoder(conn).Encode(map[string]string{
			"action":   "REGISTER_ACK",
			"drone_id": assignedID,
		})

		handleDroneConnection(conn, assignedID)

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

// Assume o controle de um socket persistente de um drone recém-registrado,
// processando atualizações de estado assíncronas (heartbeats e confirmações de término de missão).
func handleDroneConnection(conn net.Conn, droneID string) {
	d := &Drone{
		ID:            droneID,
		Status:        "LIVRE",
		Conn:          conn,
		LastHeartbeat: time.Now(),
	}

	droneMutex.Lock()
	if oldDrone, exists := base_drones[droneID]; exists {
		oldDrone.Conn.Close()
	}
	base_drones[droneID] = d
	droneMutex.Unlock()

	fmt.Printf("[%s] ➕ NOVO DRONE REGISTADO: %s\n", base_id, droneID)

	decoder := json.NewDecoder(conn)
	for {
		var update GenericCommand
		if err := decoder.Decode(&update); err != nil {
			fmt.Printf("[%s] ❌ CONEXÃO PERDIDA: %s desconectou-se.\n", base_id, droneID)
			droneMutex.Lock()
			if current, exists := base_drones[droneID]; exists && current.Conn == conn {
				delete(base_drones, droneID)
			}
			droneMutex.Unlock()
			conn.Close()
			break
		}

		d.Mutex.Lock()
		d.LastHeartbeat = time.Now()

		if update.Action == "HEARTBEAT" {
		} else if update.Action == "MISSION_COMPLETE" {
			d.Status = "LIVRE"
			fmt.Printf("[%s] ✅ %s concluiu a missão e está LIVRE.\n", base_id, droneID)
		}
		d.Mutex.Unlock()
	}
}

// Responde à requisição síncrona do Broker para despacho. Em caso de sucesso
// na alocação, delega a instrução do drone para uma goroutine separada.
func handleBrokerDispatch(conn net.Conn, cmd GenericCommand) {
	droneLivre := findFreeDrone()
	if droneLivre != nil {
		go executeMission(droneLivre, cmd)

		json.NewEncoder(conn).Encode(map[string]string{
			"status":   "ACCEPTED",
			"drone_id": droneLivre.ID,
			"base_id":  base_id,
		})
		fmt.Printf("[%s] ✅ DISPATCH ACEITO: %s alocado para %s (Broker: %s)\n", base_id, droneLivre.ID, cmd.Target, cmd.BrokerID)
	} else {
		json.NewEncoder(conn).Encode(map[string]string{
			"status":  "REJECTED_NO_DRONES",
			"base_id": base_id,
		})
		fmt.Printf("[%s] ❌ DISPATCH REJEITADO: Nenhum drone livre\n", base_id)
	}
}

// Consulta de forma thread-safe (via RLock) o mapa de registros da frota
// e retorna via TCP o status operacional de um drone específico requisitado pelo Broker.
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

// Varre o registro de drones em busca de uma unidade com status "LIVRE",
// aplicando travamento mútuo (Lock) sobre a estrutura para executar a transição atômica para "OCUPADO".
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

// Serializa o payload de voo e o transmite através do socket TCP mantido com o drone.
// Caso ocorra uma falha de rede na transmissão, o status do drone passa por rollback para "LIVRE".
func executeMission(d *Drone, cmd GenericCommand) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	fmt.Printf("[%s] 🚁 Despachando %s para alvo %s\n", base_id, d.ID, cmd.Target)

	payload := map[string]string{
		"action": "FLY",
		"target": cmd.Target,
	}

	if err := json.NewEncoder(d.Conn).Encode(payload); err != nil {
		fmt.Printf("[%s] ⚠️ Falha ao enviar comando para %s: %v. Devolvendo a LIVRE.\n", base_id, d.ID, err)
		d.Status = "LIVRE"
	}
}
