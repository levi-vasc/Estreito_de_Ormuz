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

// Isola os contadores lógicos da aplicação, garantindo
// operações atômicas no P2P e missões.
type LogicalClocks struct {
	P2PClock     int
	MissionClock int
	mutex        sync.Mutex
}

// Define o contrato do protocolo P2P para a negociação
// de acesso distribuído aos recursos (exclusão mútua).
type Message struct {
	Type      string `json:"type"`
	SenderID  string `json:"sender_id"`
	Clock     int    `json:"clock"`
	MissionID int    `json:"mission_id"`
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
}

// Encapsula as requisições de alocação derivadas dos sensores.
type LocalEvent struct {
	Priority int
	SectorID string
}

// Mantém o contexto operacional e os metadados de
// uma operação delegada a um drone específico.
type MissionState struct {
	MissionID    int
	BrokerID     string
	DroneID      string
	BaseID       string
	BaseAddr     string
	Priority     int
	SectorID     string // ADICIONADO: Guardar o setor original para permitir re-enfileiramento em caso de falha
	StartTime    time.Time
	DroneTimeout time.Duration
}

// Orquestra o estado distribuído, mantendo conexões P2P, a máquina de estados
// de exclusão mútua e a comunicação downstream com as bases e sensores.
type Broker struct {
	ID     string
	Port   string
	Peers  map[string]string
	Bases  []string
	Clocks *LogicalClocks

	State        string
	StateMutex   sync.Mutex
	PendingAcks  map[string]bool
	PendingQueue []Message
	AckCondition *sync.Cond

	CurrentReq Message

	GlobalRequests map[string]Message
	RequestsMutex  sync.Mutex

	ActiveMissions map[int]*MissionState
	MissionsMutex  sync.RWMutex

	SensorQueue chan LocalEvent

	PeerLastSeen map[string]time.Time
	PeerMutex    sync.RWMutex

	DroneWaitTimeout time.Duration
}

// Inicializa a topologia de rede do nó a partir de variáveis de ambiente
// e lança as goroutines responsáveis pelas rotinas assíncronas do broker.
func main() {
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
	go broker.startMissionMonitor()

	go broker.startSensorServer(sensorPort)
	broker.startP2PServer()
}

// Sincroniza o relógio lógico local com base em eventos da rede,
// assumindo o maior valor entre o relógio atual e o recebido.
func (lc *LogicalClocks) UpdateP2P(received int) int {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	if received > lc.P2PClock {
		lc.P2PClock = received
	}
	lc.P2PClock++
	return lc.P2PClock
}

// Retorna a posição atual do relógio lógico de forma thread-safe.
func (lc *LogicalClocks) GetP2PClock() int {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	return lc.P2PClock
}

// Aloca em memória um novo nó orquestrador, inicializando os canais,
// primitivas de sincronização e preenchendo as dependências topológicas iniciais.
func NewBroker(id, port string, peers map[string]string, bases []string) *Broker {
	b := &Broker{
		ID:               id,
		Port:             port,
		Peers:            peers,
		Bases:            bases,
		Clocks:           &LogicalClocks{P2PClock: 0, MissionClock: 0},
		State:            "IDLE",
		PendingAcks:      make(map[string]bool),
		PendingQueue:     []Message{},
		GlobalRequests:   make(map[string]Message),
		ActiveMissions:   make(map[int]*MissionState),
		SensorQueue:      make(chan LocalEvent, 100),
		PeerLastSeen:     make(map[string]time.Time),
		DroneWaitTimeout: 30 * time.Second,
	}

	for peerID := range peers {
		b.PeerLastSeen[peerID] = time.Now()
	}

	b.AckCondition = sync.NewCond(&b.StateMutex)
	return b
}

// Implementa um mecanismo de heartbeat ativo para monitoramento de peers.
func (b *Broker) startHealthCheck() {
	ticker := time.NewTicker(5 * time.Second)
	knownDead := make(map[string]bool)
	const PEER_TIMEOUT = 15 * time.Second

	for range ticker.C {
		for id, addr := range b.Peers {
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				b.PeerMutex.Lock()
				lastSeen := b.PeerLastSeen[id]
				b.PeerMutex.Unlock()

				timeSinceLastSeen := time.Since(lastSeen)

				if timeSinceLastSeen > PEER_TIMEOUT {
					if !knownDead[id] {
						knownDead[id] = true
						fmt.Printf("\n[%s] 💀 BROKER MORTO: %s (timeout: %v)\n", b.ID, id, timeSinceLastSeen)

						b.StateMutex.Lock()
						if _, waiting := b.PendingAcks[id]; waiting {
							delete(b.PendingAcks, id)
							if len(b.PendingAcks) == 0 {
								b.AckCondition.Broadcast()
							}
						}
						b.StateMutex.Unlock()

						b.RequestsMutex.Lock()
						for key, req := range b.GlobalRequests {
							if req.SenderID == id {
								delete(b.GlobalRequests, key)
							}
						}
						b.RequestsMutex.Unlock()
					}
				}
			} else {
				if knownDead[id] {
					knownDead[id] = false
					fmt.Printf("\n[%s] ♻️ BROKER RECUPERADO: %s\n", b.ID, id)
				}
				b.PeerMutex.Lock()
				b.PeerLastSeen[id] = time.Now()
				b.PeerMutex.Unlock()
				conn.Close()
			}
		}
	}
}

// Varre periodicamente o estado das missões ativas e emite eventos
// de expiração (timeout). Se o drone caiu, reinserimos o pedido na fila global/local.
func (b *Broker) startMissionMonitor() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		b.MissionsMutex.Lock()
		now := time.Now()

		for missionID, mission := range b.ActiveMissions {
			elapsed := now.Sub(mission.StartTime)
			if elapsed > mission.DroneTimeout {
				fmt.Printf("[%s] ⚠️ TIMEOUT DE MISSÃO: Pedido %d (drone %s não voltou em %v)\n",
					b.ID, missionID, mission.DroneID, mission.DroneTimeout)

				if mission.BrokerID == b.ID {
					fmt.Printf("[%s] ♻️ RE-ENFILEIRANDO MISSÃO FALHADA: Pedido %d no Setor %s\n", b.ID, missionID, mission.SectorID)
					b.SensorQueue <- LocalEvent{Priority: mission.Priority, SectorID: mission.SectorID}
				}

				delete(b.ActiveMissions, missionID)

				b.RequestsMutex.Lock()
				reqKey := fmt.Sprintf("%s-%d", mission.BrokerID, missionID)
				delete(b.GlobalRequests, reqKey)
				b.RequestsMutex.Unlock()

				go func(mID int) {
					for peerID := range b.Peers {
						b.sendDirectMessage(peerID, "RELEASE", 0, 0, mID)
					}
				}(missionID)
			}
		}
		b.MissionsMutex.Unlock()
	}
}

// Processa as métricas internas do nó, ordena a fila global e exibe o estado.
func (b *Broker) startStatusLogger() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		b.StateMutex.Lock()
		state := b.State

		var allRequests []Message
		if b.State == "WAITING" || b.State == "IN_CS" {
			allRequests = append(allRequests, b.CurrentReq)
		}

		b.RequestsMutex.Lock()
		for _, req := range b.GlobalRequests {
			allRequests = append(allRequests, req)
		}
		b.RequestsMutex.Unlock()

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

		b.MissionsMutex.RLock()

		var drones []string
		for _, m := range b.ActiveMissions {
			drones = append(drones, fmt.Sprintf("🚁 %s (%s) [Pedido: %d]", m.DroneID, m.BaseID, m.MissionID))
		}
		b.MissionsMutex.RUnlock()

		droneStr := "Nenhum"
		if len(drones) > 0 {
			droneStr = strings.Join(drones, ", ")
		}

		fmt.Printf("\n================ STATUS %s ================\n", b.ID)
		fmt.Printf("Estado: %s\n", status)
		fmt.Printf("Drones no Setor: %s\n", droneStr)

		if len(allRequests) > 0 {
			fmt.Printf("\n--- Top 5 Requisições ---\n")
			limit := 5
			if len(allRequests) < limit {
				limit = len(allRequests)
			}
			for i := 0; i < limit; i++ {
				msg := allRequests[i]
				fmt.Printf("Pedido %d | Prioridade %d | %s\n",
					msg.MissionID, msg.Priority, msg.SenderID)
			}
		}
		fmt.Printf("===============================================\n\n")
	}
}

func (b *Broker) startP2PServer() {
	ln, err := net.Listen("tcp", ":"+b.Port)
	if err != nil {
		fmt.Printf("[%s] Erro ao iniciar servidor P2P: %v\n", b.ID, err)
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go b.handleP2PConnection(conn)
	}
}

func (b *Broker) handleP2PConnection(conn net.Conn) {
	defer conn.Close()
	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}

	b.Clocks.UpdateP2P(msg.Clock)

	b.StateMutex.Lock()
	defer b.StateMutex.Unlock()

	switch msg.Type {
	case "ACK":
		delete(b.PendingAcks, msg.SenderID)
		if len(b.PendingAcks) == 0 {
			b.AckCondition.Broadcast()
		}

	case "REQ_DRONE":
		b.RequestsMutex.Lock()
		reqKey := fmt.Sprintf("%s-%d", msg.SenderID, msg.MissionID)
		b.GlobalRequests[reqKey] = msg
		b.RequestsMutex.Unlock()
		b.handleRequest(msg)

	case "RELEASE":
		b.RequestsMutex.Lock()
		reqKey := fmt.Sprintf("%s-%d", msg.SenderID, msg.MissionID)
		delete(b.GlobalRequests, reqKey)
		b.RequestsMutex.Unlock()
	}
}

func (b *Broker) handleRequest(incomingReq Message) {
	myPriority := b.CurrentReq.Priority

	iHavePriority := b.State == "IN_CS" || (b.State == "WAITING" &&

		(myPriority < incomingReq.Priority ||
			(myPriority == incomingReq.Priority && b.CurrentReq.Clock < incomingReq.Clock) ||
			(myPriority == incomingReq.Priority && b.CurrentReq.Clock == incomingReq.Clock && b.ID < incomingReq.SenderID)))
	if iHavePriority {
		b.PendingQueue = append(b.PendingQueue, incomingReq)
	} else {
		ackClock := b.Clocks.UpdateP2P(0)
		b.sendDirectMessage(incomingReq.SenderID, "ACK", b.CurrentReq.Priority, ackClock, 0)
	}

}

func (b *Broker) startSensorServer(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("[%s] Erro ao iniciar servidor de sensores: %v\n", b.ID, err)
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go b.receiveSensorData(conn)
	}
}

func (b *Broker) receiveSensorData(conn net.Conn) {
	defer conn.Close()
	var sensorEvt map[string]interface{}
	if err := json.NewDecoder(conn).Decode(&sensorEvt); err != nil {
		return
	}

	priority := int(sensorEvt["priority"].(float64))
	sectorID := sensorEvt["sector_id"].(string)

	json.NewEncoder(conn).Encode(map[string]string{"status": "RECEIVED"})

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

	msgClock := b.Clocks.GetP2PClock()
	missionID := msgClock

	b.CurrentReq = Message{
		Type:      "REQ_DRONE",
		SenderID:  b.ID,
		Clock:     msgClock,
		MissionID: missionID,
		Priority:  evt.Priority,
		Timestamp: time.Now().Unix(),
	}

	b.RequestsMutex.Lock()
	reqKey := fmt.Sprintf("%s-%d", b.ID, missionID)
	b.GlobalRequests[reqKey] = b.CurrentReq
	b.RequestsMutex.Unlock()

	b.StateMutex.Unlock()

	for id := range b.Peers {
		if !b.sendDirectMessage(id, "REQ_DRONE", evt.Priority, msgClock, missionID) {
			b.StateMutex.Lock()
			delete(b.PendingAcks, id)
			if len(b.PendingAcks) == 0 {
				b.AckCondition.Broadcast()
			}
			b.StateMutex.Unlock()
		}
	}

	b.StateMutex.Lock()
	for len(b.PendingAcks) > 0 {
		b.AckCondition.Wait()
	}
	b.State = "IN_CS"
	b.StateMutex.Unlock()

	baseAddr, rawDroneID, baseID := b.requestDroneToBases(evt.SectorID)

	if baseAddr == "" {
		fmt.Printf("[%s] ❌ Falha ao alocar drone para pedido %d\n", b.ID, missionID)
		b.releaseSection(missionID)
		return
	}

	uniqueDroneName := fmt.Sprintf("%s (%s)", rawDroneID, baseID)

	b.MissionsMutex.Lock()
	b.ActiveMissions[missionID] = &MissionState{
		MissionID:    missionID,
		BrokerID:     b.ID,
		DroneID:      rawDroneID,
		BaseID:       baseID,
		BaseAddr:     baseAddr,
		Priority:     evt.Priority,
		SectorID:     evt.SectorID, // ADICIONADO: Guardado para o caso de falha posterior
		StartTime:    time.Now(),
		DroneTimeout: b.DroneWaitTimeout,
	}
	b.MissionsMutex.Unlock()

	b.releaseSection(missionID)

	go b.waitForDroneReturnAndNotify(baseAddr, rawDroneID, uniqueDroneName, missionID, evt.SectorID)
}

func (b *Broker) requestDroneToBases(targetSector string) (string, string, string) {
	maxRetries := 10
	retryCount := 0

	for retryCount < maxRetries {
		for _, baseAddr := range b.Bases {
			conn, err := net.DialTimeout("tcp", baseAddr, 3*time.Second)
			if err != nil {
				continue
			}

			json.NewEncoder(conn).Encode(map[string]string{
				"action":    "DISPATCH",
				"target":    targetSector,
				"broker_id": b.ID,
			})

			var resp map[string]string
			err = json.NewDecoder(conn).Decode(&resp)
			conn.Close()

			if err == nil && resp["status"] == "ACCEPTED" {
				droneID := resp["drone_id"]
				baseID := resp["base_id"]
				return baseAddr, droneID, baseID
			}
		}

		retryCount++
		if retryCount < maxRetries {
			time.Sleep(2 * time.Second)
		}
	}

	return "", "", ""
}

func (b *Broker) waitForDroneReturnAndNotify(baseAddr, rawDroneID, uniqueDroneName string, missionID int, targetSector string) {
	maxRetries := 60
	retryCount := 0

	for retryCount < maxRetries {
		time.Sleep(2 * time.Second)

		// ADICIONADO: Se o monitor assíncrono já limpou por timeout, encerra imediatamente esta goroutine
		b.MissionsMutex.RLock()
		_, exists := b.ActiveMissions[missionID]
		b.MissionsMutex.RUnlock()
		if !exists {
			return
		}

		conn, err := net.DialTimeout("tcp", baseAddr, 2*time.Second)
		if err != nil {
			retryCount++
			continue
		}

		json.NewEncoder(conn).Encode(map[string]string{
			"action": "STATUS",
			"target": rawDroneID,
		})

		var resp map[string]string
		err = json.NewDecoder(conn).Decode(&resp)
		conn.Close()

		if err == nil && resp["status"] == "LIVRE" {
			fmt.Printf("\n[%s] O %s concluiu o atendimento e SAIU DO SETOR.\n", b.ID, uniqueDroneName)

			b.MissionsMutex.Lock()
			delete(b.ActiveMissions, missionID)
			b.MissionsMutex.Unlock()

			b.RequestsMutex.Lock()
			reqKey := fmt.Sprintf("%s-%d", b.ID, missionID)
			delete(b.GlobalRequests, reqKey)
			b.RequestsMutex.Unlock()

			releaseClock := b.Clocks.UpdateP2P(0)
			for id := range b.Peers {
				b.sendDirectMessage(id, "RELEASE", 0, releaseClock, missionID)
			}
			return
		}

		retryCount++
	}

	fmt.Printf("[%s] 💀 TIMEOUT NO POLLING: Pedido %d (%s) não retornou após %d tentativas\n",
		b.ID, missionID, uniqueDroneName, maxRetries)

	b.MissionsMutex.Lock()
	mission, exists := b.ActiveMissions[missionID]
	if exists {
		if mission.BrokerID == b.ID {
			b.SensorQueue <- LocalEvent{Priority: mission.Priority, SectorID: mission.SectorID}
		}
		delete(b.ActiveMissions, missionID)
	}
	b.MissionsMutex.Unlock()

	b.RequestsMutex.Lock()
	reqKey := fmt.Sprintf("%s-%d", b.ID, missionID)
	delete(b.GlobalRequests, reqKey)
	b.RequestsMutex.Unlock()

	releaseClock := b.Clocks.UpdateP2P(0)
	for id := range b.Peers {
		b.sendDirectMessage(id, "RELEASE", 0, releaseClock, missionID)
	}
}

func (b *Broker) releaseSection(missionID int) {
	b.StateMutex.Lock()
	defer b.StateMutex.Unlock()

	b.State = "IDLE"

	for _, pReq := range b.PendingQueue {
		ackClock := b.Clocks.UpdateP2P(0)
		b.sendDirectMessage(pReq.SenderID, "ACK", 0, ackClock, 0)
	}
	b.PendingQueue = []Message{}
}

func (b *Broker) sendDirectMessage(targetID, msgType string, priority int, clockValue int, missionID int) bool {
	addr := b.Peers[targetID]
	if addr == "" {
		return false
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(Message{
		Type:      msgType,
		SenderID:  b.ID,
		Clock:     clockValue,
		MissionID: missionID,
		Priority:  priority,
		Timestamp: time.Now().Unix(),
	})

	return true
}
