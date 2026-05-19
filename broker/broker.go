package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Message struct {
	Type     string `json:"type"`
	SenderID string `json:"sender_id"`
	Clock    int    `json:"clock"`
	Priority int    `json:"priority"`
}

type LocalEvent struct {
	Priority int
	SectorID string
}

type Broker struct {
	ID    string
	Port  string
	Peers map[string]string
	Bases []string

	OrderCounter int
	CounterMutex sync.Mutex
	P2PClock     int

	State        string
	StateMutex   sync.Mutex
	PendingAcks  map[string]bool
	PendingQueue []Message
	AckCondition *sync.Cond

	CurrentReq Message

	SensorQueue chan LocalEvent

	ActiveDrones []string

	// TABELA GLOBAL: Agora mapeia pelo ID único do pedido (Clock) para evitar o bug do RELEASE
	GlobalRequests map[int]Message
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: ./broker <ID> <PORTA_P2P> <PORTA_SENSOR>")
		return
	}

	id := os.Args[1]
	p2pPort := os.Args[2]
	sensorPort := os.Args[3]

	peers := map[string]string{}
	if peersEnv := os.Getenv("PEERS"); peersEnv != "" {
		for _, entry := range strings.Split(peersEnv, ",") {
			parts := strings.SplitN(entry, "|", 2)
			if len(parts) == 2 {
				peers[parts[0]] = parts[1]
				fmt.Printf("[%s] Peer registrado: %s -> %s\n", id, parts[0], parts[1])
			}
		}
	}

	bases := []string{}
	if basesEnv := os.Getenv("BASES"); basesEnv != "" {
		bases = strings.Split(basesEnv, ",")
		fmt.Printf("[%s] Bases registradas: %v\n", id, bases)
	}

	broker := NewBroker(id, p2pPort, peers, bases)

	go broker.startStatusLogger()
	go broker.startHealthCheck()
	go broker.startQueueProcessor()

	go broker.startSensorServer(sensorPort)
	broker.startP2PServer()
}

func NewBroker(id, port string, peers map[string]string, bases []string) *Broker {
	b := &Broker{
		ID:             id,
		Port:           port,
		Peers:          peers,
		Bases:          bases,
		State:          "IDLE",
		PendingAcks:    make(map[string]bool),
		SensorQueue:    make(chan LocalEvent, 100),
		ActiveDrones:   []string{},
		GlobalRequests: make(map[int]Message),
		OrderCounter:   0,
		P2PClock:       0,
	}
	b.AckCondition = sync.NewCond(&b.StateMutex)
	return b
}

func (b *Broker) updateClocks(received int) {
	b.CounterMutex.Lock()
	if received > b.OrderCounter {
		b.OrderCounter = received
	}
	if received > b.P2PClock {
		b.P2PClock = received
	}
	b.OrderCounter++
	b.P2PClock++
	b.CounterMutex.Unlock()
}

func (b *Broker) getNextOrderID() int {
	b.CounterMutex.Lock()
	b.OrderCounter++
	id := b.OrderCounter
	b.P2PClock++
	b.CounterMutex.Unlock()
	return id
}

func (b *Broker) getNextP2PClock() int {
	b.CounterMutex.Lock()
	b.P2PClock++
	c := b.P2PClock
	b.CounterMutex.Unlock()
	return c
}

func (b *Broker) startHealthCheck() {
	ticker := time.NewTicker(3 * time.Second)
	knownDead := make(map[string]bool)

	for range ticker.C {
		for id, addr := range b.Peers {
			conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
			if err != nil {
				if !knownDead[id] {
					knownDead[id] = true
					fmt.Printf("\n[%s] ⚠️ ALERTA: Broker %s caiu! Ajustando rede...\n", b.ID, id)
				}
				b.StateMutex.Lock()
				if _, waiting := b.PendingAcks[id]; waiting {
					delete(b.PendingAcks, id)
					if len(b.PendingAcks) == 0 {
						b.AckCondition.Broadcast()
					}
				}

				// Removemos todos os pedidos do broker que caiu da tela global
				for clockKey, req := range b.GlobalRequests {
					if req.SenderID == id {
						delete(b.GlobalRequests, clockKey)
					}
				}
				b.StateMutex.Unlock()
			} else {
				if knownDead[id] {
					knownDead[id] = false
					fmt.Printf("\n[%s] ♻️ RECUPERAÇÃO: Broker %s reconectado!\n", b.ID, id)
				}
				conn.Close()
			}
		}
	}
}

func (b *Broker) startStatusLogger() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		b.StateMutex.Lock()
		state := b.State

		var allRequests []Message
		if b.State == "WAITING" || b.State == "IN_CS" {
			allRequests = append(allRequests, b.CurrentReq)
		}
		for _, req := range b.GlobalRequests {
			allRequests = append(allRequests, req)
		}

		activeDronesCopy := make([]string, len(b.ActiveDrones))
		copy(activeDronesCopy, b.ActiveDrones)
		b.StateMutex.Unlock()

		sort.Slice(allRequests, func(i, j int) bool {
			if allRequests[i].Clock != allRequests[j].Clock {
				return allRequests[i].Clock < allRequests[j].Clock
			}
			return allRequests[i].SenderID < allRequests[j].SenderID
		})

		status := "🛏️  Ocioso"
		if state == "IN_CS" {
			status = "🔐 NA REGIÃO CRÍTICA (Alocando drone com as bases)"
		} else if state == "WAITING" {
			status = "⏳ AGUARDANDO PERMISSÃO NA REDE"
		}

		droneStr := "❌ Nenhum drone neste broker no momento."
		if len(activeDronesCopy) > 0 {
			droneStr = "🚁 " + strings.Join(activeDronesCopy, ", ")
		}

		fmt.Printf("\n================ STATUS %s ================\n", b.ID)
		fmt.Printf("Status de Concorrência: %s\n", status)
		fmt.Printf("Drones Gerenciados Aqui: %s\n", droneStr)

		fmt.Printf("\n--- Fila Global de Requisições Ativas (Top 5) ---\n")
		if len(allRequests) == 0 {
			fmt.Printf("Fila vazia (Nenhuma ocorrência ativa na rede).\n")
		} else {
			limit := 5
			if len(allRequests) < limit {
				limit = len(allRequests)
			}
			for i := 0; i < limit; i++ {
				msg := allRequests[i]
				fmt.Printf("Pedido %d | Prioridade %d | %s\n", msg.Clock, msg.Priority, msg.SenderID)
			}
		}
		fmt.Printf("===================================================\n\n")
	}
}

func (b *Broker) startP2PServer() {
	ln, _ := net.Listen("tcp", ":"+b.Port)
	for {
		conn, _ := ln.Accept()
		go b.handleP2PConnection(conn)
	}
}

func (b *Broker) handleP2PConnection(conn net.Conn) {
	defer conn.Close()
	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}

	b.updateClocks(msg.Clock)

	b.StateMutex.Lock()
	switch msg.Type {
	case "ACK":
		delete(b.PendingAcks, msg.SenderID)
		if len(b.PendingAcks) == 0 {
			b.AckCondition.Broadcast()
		}
	case "REQ_DRONE":
		b.GlobalRequests[msg.Clock] = msg // Salva pelo Clock (Número do Pedido)
		b.handleRequest(msg)
	case "RELEASE":
		delete(b.GlobalRequests, msg.Clock) // Apaga exatamente o Pedido que finalizou
	}
	b.StateMutex.Unlock()
}

func (b *Broker) handleRequest(incomingReq Message) {
	myPriority := b.CurrentReq.Priority

	needsToWait := b.State == "IN_CS" || (b.State == "WAITING" &&
		(myPriority > incomingReq.Priority ||
			(myPriority == incomingReq.Priority && b.CurrentReq.Clock < incomingReq.Clock) ||
			(myPriority == incomingReq.Priority && b.CurrentReq.Clock == incomingReq.Clock && b.ID < incomingReq.SenderID)))

	if needsToWait {
		b.PendingQueue = append(b.PendingQueue, incomingReq)
	} else {
		b.sendDirectMessage(incomingReq.SenderID, "ACK", b.CurrentReq.Priority, incomingReq.Clock)
	}
}

func (b *Broker) startSensorServer(port string) {
	ln, _ := net.Listen("tcp", ":"+port)
	for {
		conn, _ := ln.Accept()
		go b.receiveSensorData(conn)
	}
}

func (b *Broker) receiveSensorData(conn net.Conn) {
	defer conn.Close()
	var sensorEvt map[string]interface{}
	json.NewDecoder(conn).Decode(&sensorEvt)

	priority := int(sensorEvt["priority"].(float64))
	sectorID := sensorEvt["sector_id"].(string)

	b.SensorQueue <- LocalEvent{Priority: priority, SectorID: sectorID}
}

func (b *Broker) startQueueProcessor() {
	for evt := range b.SensorQueue {
		b.executeDistributedExclusion(evt)
	}
}

func (b *Broker) executeDistributedExclusion(evt LocalEvent) {
	b.StateMutex.Lock()
	b.State = "WAITING"

	b.PendingAcks = make(map[string]bool)
	for id := range b.Peers {
		b.PendingAcks[id] = true
	}

	b.CurrentReq = Message{
		Type:     "REQ_DRONE",
		SenderID: b.ID,
		Clock:    b.getNextOrderID(),
		Priority: evt.Priority,
	}
	b.StateMutex.Unlock()

	for id := range b.Peers {
		if !b.sendDirectMessage(id, "REQ_DRONE", evt.Priority, b.CurrentReq.Clock) {
			b.StateMutex.Lock()
			delete(b.PendingAcks, id)
			b.StateMutex.Unlock()
		}
	}

	b.StateMutex.Lock()
	for len(b.PendingAcks) > 0 {
		b.AckCondition.Wait()
	}
	b.State = "IN_CS"
	b.StateMutex.Unlock()

	// Recebe o ID da base também
	baseAddr, rawDroneID, baseID := b.requestDroneToBases(evt.SectorID)

	// Formata o nome visual único
	uniqueDroneName := fmt.Sprintf("%s (%s)", rawDroneID, baseID)

	b.StateMutex.Lock()
	b.ActiveDrones = append(b.ActiveDrones, uniqueDroneName)
	b.StateMutex.Unlock()

	missionClock := b.CurrentReq.Clock // Salva o ID exato desta missão

	b.releaseSection()

	// Envia os dados crus para a rede, mas o nome único para o logger
	go b.waitForDroneReturnAndNotify(baseAddr, rawDroneID, uniqueDroneName, missionClock)
}

func (b *Broker) requestDroneToBases(targetSector string) (string, string, string) {
	for {
		for _, baseAddr := range b.Bases {
			conn, err := net.DialTimeout("tcp", baseAddr, 2*time.Second)
			if err != nil {
				continue
			}

			json.NewEncoder(conn).Encode(map[string]string{
				"action":    "DISPATCH",
				"target":    targetSector,
				"broker_id": b.ID,
			})

			var resp map[string]string
			json.NewDecoder(conn).Decode(&resp)
			conn.Close()

			if resp["status"] == "ACCEPTED" {
				droneID := resp["drone_id"]
				baseID := resp["base_id"]
				return baseAddr, droneID, baseID
			}
		}
		time.Sleep(3 * time.Second)
	}
}

func (b *Broker) waitForDroneReturnAndNotify(baseAddr, rawDroneID, uniqueDroneName string, missionClock int) {
	for {
		time.Sleep(2 * time.Second)
		conn, err := net.DialTimeout("tcp", baseAddr, 2*time.Second)
		if err != nil {
			continue
		}

		json.NewEncoder(conn).Encode(map[string]string{
			"action": "STATUS",
			"target": rawDroneID, // A base ainda atende pelo ID original
		})

		var resp map[string]string
		json.NewDecoder(conn).Decode(&resp)
		conn.Close()

		if resp["status"] == "LIVRE" {
			fmt.Printf("[%s] ✅ %s retornou à base e está LIVRE.\n", b.ID, uniqueDroneName)

			b.StateMutex.Lock()
			for i, d := range b.ActiveDrones {
				if d == uniqueDroneName {
					b.ActiveDrones = append(b.ActiveDrones[:i], b.ActiveDrones[i+1:]...)
					break
				}
			}
			b.StateMutex.Unlock()

			// Emite o sinal de RELEASE especificando o relógio (Pedido) que deve ser apagado
			for id := range b.Peers {
				b.sendDirectMessage(id, "RELEASE", 0, missionClock)
			}
			return
		}
	}
}

func (b *Broker) releaseSection() {
	b.StateMutex.Lock()
	defer b.StateMutex.Unlock()

	b.State = "IDLE"
	for _, pReq := range b.PendingQueue {
		b.sendDirectMessage(pReq.SenderID, "ACK", 0, b.getNextP2PClock())
	}
	b.PendingQueue = []Message{}
}

func (b *Broker) sendDirectMessage(targetID, msgType string, priority int, clockValue int) bool {
	addr := b.Peers[targetID]
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(Message{
		Type:     msgType,
		SenderID: b.ID,
		Clock:    clockValue,
		Priority: priority,
	})

	return true
}
