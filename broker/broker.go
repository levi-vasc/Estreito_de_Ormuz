package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

<<<<<<< Updated upstream
// Estrutura de Mensagens para comunicação P2P entre Brokers
type Message struct {
	Type     string `json:"type"`      // "REQ_DRONE", "ACK"
	SenderID string `json:"sender_id"` // ID do Broker (ex: "Setor_A")
	Clock    int    `json:"clock"`     // Relógio de Lamport
	Priority int    `json:"priority"`  // Prioridade da ocorrência (1-3)
=======
// Namespaces de relógio separados para evitar colisão
type LogicalClocks struct {
	P2PClock     int // Para mensagens P2P (REQ, ACK, RELEASE)
	MissionClock int // Para IDs únicos de missão
	mutex        sync.Mutex
}

func (lc *LogicalClocks) UpdateP2P(received int) int {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	if received > lc.P2PClock {
		lc.P2PClock = received
	}
	lc.P2PClock++
	return lc.P2PClock
}

func (lc *LogicalClocks) NextMissionID() int {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	lc.MissionClock++
	return lc.MissionClock
}

func (lc *LogicalClocks) GetP2PClock() int {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	return lc.P2PClock
}

type Message struct {
	Type      string `json:"type"`
	SenderID  string `json:"sender_id"`
	Clock     int    `json:"clock"`
	MissionID int    `json:"mission_id"` // Novo: identificador único de missão
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
}

type LocalEvent struct {
	Priority int
	SectorID string
>>>>>>> Stashed changes
}

type MissionState struct {
	MissionID    int
	BrokerID     string
	DroneID      string
	BaseID       string
	BaseAddr     string
	Priority     int
	StartTime    time.Time
	DroneTimeout time.Duration
}

type Broker struct {
<<<<<<< Updated upstream
	ID    string
	Port  string
	Peers map[string]string // ID -> IP:Porta dos outros Brokers
	Bases []string          // Lista de IP:Porta das Bases de Drones

	// Estado de Lamport e Concorrência
	LamportClock int
	ClockMutex   sync.Mutex

	// Estado do Algoritmo de Ricart-Agrawala
	State        string // "IDLE", "WAITING", "IN_CS"
=======
	ID     string
	Port   string
	Peers  map[string]string
	Bases  []string
	Clocks *LogicalClocks

	// Estado de exclusão mútua
	State        string
>>>>>>> Stashed changes
	StateMutex   sync.Mutex
	AcksReceived int
	PendingQueue []Message
	AckCondition *sync.Cond // Para bloquear o processo até receber todos os ACKs

<<<<<<< Updated upstream
	CurrentReq Message // Armazena o pedido que o broker está tentando processar
=======
	CurrentReq Message

	// Tabela global de requisições por MissionID (não mais pelo Clock!)
	GlobalRequests map[int]Message
	RequestsMutex  sync.Mutex

	// Estado de missões ativas
	ActiveMissions map[int]*MissionState
	MissionsMutex  sync.RWMutex

	SensorQueue chan LocalEvent

	// Rastreamento de saúde dos peers
	PeerLastSeen map[string]time.Time
	PeerMutex    sync.RWMutex

	// Timeout para esperar by drone (padrão: 30 segundos)
	DroneWaitTimeout time.Duration
>>>>>>> Stashed changes
}

func main() {
	id := os.Args[1]
	p2pPort := os.Args[2]
	sensorPort := os.Args[3]

	// Configuração da rede (isso deve vir de um arquivo ou ENV no Docker)
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

<<<<<<< Updated upstream
=======
	go broker.startStatusLogger()
	go broker.startHealthCheck()
	go broker.startQueueProcessor()
	go broker.startMissionMonitor()

>>>>>>> Stashed changes
	go broker.startSensorServer(sensorPort)
	broker.startP2PServer()
}

func NewBroker(id, port string, peers map[string]string, bases []string) *Broker {
	b := &Broker{
<<<<<<< Updated upstream
		ID:    id,
		Port:  port,
		Peers: peers,
		Bases: bases,
		State: "IDLE",
=======
		ID:               id,
		Port:             port,
		Peers:            peers,
		Bases:            bases,
		Clocks:           &LogicalClocks{P2PClock: 0, MissionClock: 0},
		State:            "IDLE",
		PendingAcks:      make(map[string]bool),
		PendingQueue:     []Message{},
		GlobalRequests:   make(map[int]Message),
		ActiveMissions:   make(map[int]*MissionState),
		SensorQueue:      make(chan LocalEvent, 100),
		PeerLastSeen:     make(map[string]time.Time),
		DroneWaitTimeout: 30 * time.Second,
>>>>>>> Stashed changes
	}

	// Inicializa PeerLastSeen
	for peerID := range peers {
		b.PeerLastSeen[peerID] = time.Now()
	}

	b.AckCondition = sync.NewCond(&b.StateMutex)
	return b
}

<<<<<<< Updated upstream
// --- LOGICA DE RELÓGIO LOGICO ---

func (b *Broker) updateClock(received int) {
	b.ClockMutex.Lock()
	if received > b.LamportClock {
		b.LamportClock = received
	}
	b.LamportClock++
	b.ClockMutex.Unlock()
}

func (b *Broker) getNextClock() int {
	b.ClockMutex.Lock()
	b.LamportClock++
	c := b.LamportClock
	b.ClockMutex.Unlock()
	return c
}

// --- SERVIDORES ---
=======
func (b *Broker) startHealthCheck() {
	ticker := time.NewTicker(5 * time.Second) // Aumentado para 5 segundos
	knownDead := make(map[string]bool)
	const PEER_TIMEOUT = 15 * time.Second // Timeout de 15 segundos antes de considerar morto

	for range ticker.C {
		for id, addr := range b.Peers {
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				b.PeerMutex.Lock()
				lastSeen := b.PeerLastSeen[id]
				b.PeerMutex.Unlock()

				timeSinceLastSeen := time.Since(lastSeen)

				// Só considera morto se passou o timeout
				if timeSinceLastSeen > PEER_TIMEOUT {
					if !knownDead[id] {
						knownDead[id] = true
						fmt.Printf("\n[%s] 💀 BROKER MORTO: %s (timeout: %v)\n",
							b.ID, id, timeSinceLastSeen)

						// Remove de ACKs pendentes
						b.StateMutex.Lock()
						if _, waiting := b.PendingAcks[id]; waiting {
							delete(b.PendingAcks, id)
							if len(b.PendingAcks) == 0 {
								b.AckCondition.Broadcast()
							}
						}
						b.StateMutex.Unlock()

						// Remove requisições deste broker, mas NÃO remove missões ativas!
						b.RequestsMutex.Lock()
						for missionID, req := range b.GlobalRequests {
							if req.SenderID == id {
								delete(b.GlobalRequests, missionID)
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

// startMissionMonitor verifica timeouts de missões
func (b *Broker) startMissionMonitor() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		b.MissionsMutex.Lock()
		now := time.Now()

		for missionID, mission := range b.ActiveMissions {
			elapsed := now.Sub(mission.StartTime)
			if elapsed > mission.DroneTimeout {
				fmt.Printf("[%s] ⚠️ TIMEOUT DE MISSÃO: MissionID=%d (drone %s não voltou em %v)\n",
					b.ID, missionID, mission.DroneID, mission.DroneTimeout)

				// Remove a missão como ativa
				delete(b.ActiveMissions, missionID)

				// Limpa do GlobalRequests
				b.RequestsMutex.Lock()
				delete(b.GlobalRequests, missionID)
				b.RequestsMutex.Unlock()

				// Emite RELEASE mesmo assim para limpar outras brokers
				go func(missionID int) {
					for peerID := range b.Peers {
						b.sendDirectMessage(peerID, "RELEASE", 0, 0, missionID)
					}
				}(missionID)
			}
		}
		b.MissionsMutex.Unlock()
	}
}

func (b *Broker) startStatusLogger() {
	ticker := time.NewTicker(15 * time.Second)
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

		// --- NOVO: Montando a variável droneStr ---
		var drones []string
		for _, m := range b.ActiveMissions {
			// Formata no estilo: 🚁 Drone_01 (Base_01)
			drones = append(drones, fmt.Sprintf("🚁 %s (%s)", m.DroneID, m.BaseID))
		}
		b.MissionsMutex.RUnlock()

		droneStr := "Nenhum"
		if len(drones) > 0 {
			droneStr = strings.Join(drones, ", ")
		}
		// ------------------------------------------

		fmt.Printf("\n================ STATUS %s ================\n", b.ID)
		fmt.Printf("Estado: %s\n", status)
		fmt.Printf("Drones no Setor: %s\n", droneStr) // Informação adicionada no log

		if len(allRequests) > 0 {
			fmt.Printf("\n--- Top 5 Requisições ---\n")
			limit := 5
			if len(allRequests) < limit {
				limit = len(allRequests)
			}
			for i := 0; i < limit; i++ {
				msg := allRequests[i]
				fmt.Printf("Pedido (MissionID: %d) | Prioridade %d | %s\n",
					msg.MissionID, msg.Priority, msg.SenderID)
			}
		}
		fmt.Printf("===============================================\n\n")
	}
}
>>>>>>> Stashed changes

// Inicia o servidor P2P para falar com outros Brokers
func (b *Broker) startP2PServer() {
<<<<<<< Updated upstream
	ln, _ := net.Listen("tcp", ":"+b.Port)
	fmt.Printf("[%s] Ouvindo rede P2P na porta %s\n", b.ID, b.Port)
=======
	ln, err := net.Listen("tcp", ":"+b.Port)
	if err != nil {
		fmt.Printf("[%s] Erro ao iniciar servidor P2P: %v\n", b.ID, err)
		return
	}
	defer ln.Close()

>>>>>>> Stashed changes
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
	json.NewDecoder(conn).Decode(&msg)

<<<<<<< Updated upstream
	b.updateClock(msg.Clock)
=======
	// Atualiza relógio P2P (namespace separado!)
	b.Clocks.UpdateP2P(msg.Clock)
>>>>>>> Stashed changes

	b.StateMutex.Lock()
	defer b.StateMutex.Unlock()

<<<<<<< Updated upstream
	if msg.Type == "ACK" {
		b.AcksReceived++
		fmt.Printf("[%s] ACK recebido de %s (%d/%d)\n", b.ID, msg.SenderID, b.AcksReceived, len(b.Peers))
		b.AckCondition.Broadcast() // Acorda a thread que está esperando ACKs
	} else if msg.Type == "REQ_DRONE" {
		b.handleRequest(msg)
=======
	switch msg.Type {
	case "ACK":
		delete(b.PendingAcks, msg.SenderID)
		if len(b.PendingAcks) == 0 {
			b.AckCondition.Broadcast()
		}

	case "REQ_DRONE":
		b.RequestsMutex.Lock()
		b.GlobalRequests[msg.MissionID] = msg // Usa MissionID, não Clock!
		b.RequestsMutex.Unlock()
		b.handleRequest(msg)

	case "RELEASE":
		b.RequestsMutex.Lock()
		delete(b.GlobalRequests, msg.MissionID) // Apaga por MissionID exato
		b.RequestsMutex.Unlock()
>>>>>>> Stashed changes
	}
}

func (b *Broker) handleRequest(incomingReq Message) {
	// Regra de Ricart-Agrawala para decidir se envia ACK ou enfileira
	myPriority := b.CurrentReq.Priority

	// Eu mando ACK se:
	// 1. Eu não quero o drone (IDLE)
	// 2. O outro é mais prioritário que eu
	// 3. Empate de prioridade, mas o relógio dele é menor
	// 4. Empate de tudo, mas o ID dele é menor (desempate final)

	needsToWait := b.State == "IN_CS" || (b.State == "WAITING" &&
		(myPriority > incomingReq.Priority ||
			(myPriority == incomingReq.Priority && b.CurrentReq.Clock < incomingReq.Clock) ||
			(myPriority == incomingReq.Priority && b.CurrentReq.Clock == incomingReq.Clock && b.ID < incomingReq.SenderID)))

	if needsToWait {
		b.PendingQueue = append(b.PendingQueue, incomingReq)
		fmt.Printf("[%s] Pedido de %s EM PENDÊNCIA.\n", b.ID, incomingReq.SenderID)
	} else {
<<<<<<< Updated upstream
		b.sendDirectMessage(incomingReq.SenderID, "ACK")
=======
		ackClock := b.Clocks.UpdateP2P(0)
		b.sendDirectMessage(incomingReq.SenderID, "ACK", b.CurrentReq.Priority, ackClock, 0)
>>>>>>> Stashed changes
	}
}

// Servidor para Sensores Locais
func (b *Broker) startSensorServer(port string) {
<<<<<<< Updated upstream
	ln, _ := net.Listen("tcp", ":"+port)
	fmt.Printf("[%s] Ouvindo Sensores na porta %s\n", b.ID, port)
	for {
		conn, _ := ln.Accept()
		go b.processSensorData(conn)
=======
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
>>>>>>> Stashed changes
	}
}

func (b *Broker) processSensorData(conn net.Conn) {
	defer conn.Close()
	var sensorEvt map[string]interface{}
	if err := json.NewDecoder(conn).Decode(&sensorEvt); err != nil {
		return
	}

	priority := int(sensorEvt["priority"].(float64))
	fmt.Printf("\n[%s] !!! OCORRÊNCIA PRIORIDADE %d !!!\n", b.ID, priority)

<<<<<<< Updated upstream
	b.StateMutex.Lock()
	b.State = "WAITING"
	b.AcksReceived = 0
	b.CurrentReq = Message{
		Type:     "REQ_DRONE",
		SenderID: b.ID,
		Clock:    b.getNextClock(),
		Priority: priority,
=======
	// Envia ACK de recebimento para o sensor
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

	// NOVO: Usa MissionID em vez de Clock para missões
	missionID := b.Clocks.NextMissionID()
	msgClock := b.Clocks.GetP2PClock()

	b.CurrentReq = Message{
		Type:      "REQ_DRONE",
		SenderID:  b.ID,
		Clock:     msgClock,
		MissionID: missionID,
		Priority:  evt.Priority,
		Timestamp: time.Now().Unix(),
>>>>>>> Stashed changes
	}

	b.RequestsMutex.Lock()
	b.GlobalRequests[missionID] = b.CurrentReq
	b.RequestsMutex.Unlock()

	b.StateMutex.Unlock()

<<<<<<< Updated upstream
	if len(b.Peers) == 0 {
		// Sem peers, entra direto
		b.StateMutex.Lock()
		b.State = "IN_CS"
		b.StateMutex.Unlock()
	} else {
		// Broadcast e espera ACKs — só uma vez
		for id := range b.Peers {
			b.sendDirectMessage(id, "REQ_DRONE")
=======
	// Envia requisição para todos os peers
	for id := range b.Peers {
		if !b.sendDirectMessage(id, "REQ_DRONE", evt.Priority, msgClock, missionID) {
			b.StateMutex.Lock()
			delete(b.PendingAcks, id)
			if len(b.PendingAcks) == 0 {
				b.AckCondition.Broadcast()
			}
			b.StateMutex.Unlock()
>>>>>>> Stashed changes
		}
		b.StateMutex.Lock()
		for b.AcksReceived < len(b.Peers) {
			b.AckCondition.Wait()
		}
		b.State = "IN_CS"
		b.StateMutex.Unlock()
	}

<<<<<<< Updated upstream
	fmt.Printf("[%s] Permissão concedida. Contatando bases...\n", b.ID)
	b.requestDroneToBases()
	b.releaseSection()
}

// --- COMUNICAÇÃO COM BASES E OUTROS ---

func (b *Broker) requestDroneToBases() {
	success := false
	for !success {
=======
	// Aguarda todos os ACKs
	b.StateMutex.Lock()
	for len(b.PendingAcks) > 0 {
		b.AckCondition.Wait()
	}
	b.State = "IN_CS"
	b.StateMutex.Unlock()

	// NOVO: Alocação de drone agora é ATÔMICA com a entrada em CS
	baseAddr, rawDroneID, baseID := b.requestDroneToBases(evt.SectorID)

	if baseAddr == "" {
		// Falhou ao alocar drone, libera CS imediatamente
		fmt.Printf("[%s] ❌ Falha ao alocar drone para missão %d\n", b.ID, missionID)
		b.releaseSection(missionID)
		return
	}

	uniqueDroneName := fmt.Sprintf("%s (%s)", rawDroneID, baseID)

	// Registra missão ativa
	b.MissionsMutex.Lock()
	b.ActiveMissions[missionID] = &MissionState{
		MissionID:    missionID,
		BrokerID:     b.ID,
		DroneID:      rawDroneID,
		BaseID:       baseID,
		BaseAddr:     baseAddr,
		Priority:     evt.Priority,
		StartTime:    time.Now(),
		DroneTimeout: b.DroneWaitTimeout,
	}
	b.MissionsMutex.Unlock()

	// Sai da CS imediatamente (antes de esperar drone voltar)
	b.releaseSection(missionID)

	// Monitora retorno do drone (fora de CS)
	go b.waitForDroneReturnAndNotify(baseAddr, rawDroneID, uniqueDroneName, missionID)
}

func (b *Broker) requestDroneToBases(targetSector string) (string, string, string) {
	maxRetries := 10
	retryCount := 0

	for retryCount < maxRetries {
>>>>>>> Stashed changes
		for _, baseAddr := range b.Bases {
			conn, err := net.DialTimeout("tcp", baseAddr, 3*time.Second)
			if err != nil {
				continue
			}

			json.NewEncoder(conn).Encode(map[string]string{
				"action":    "DISPATCH",
				"target":    b.ID,
				"broker_id": b.ID,
			})

			var resp map[string]string
			err = json.NewDecoder(conn).Decode(&resp)
			conn.Close()

			if err == nil && resp["status"] == "ACCEPTED" {
				droneID := resp["drone_id"]
				fmt.Printf("[%s] %s alocado pela %s!\n", b.ID, resp["drone_id"], resp["base_id"])
				b.waitForDroneReturn(baseAddr, droneID)
				success = true
				break
			}
		}
<<<<<<< Updated upstream
		if !success {
			fmt.Printf("[%s] Sem drones em nenhuma base. Re-tentando...\n", b.ID)
			time.Sleep(3 * time.Second)
=======

		retryCount++
		if retryCount < maxRetries {
			time.Sleep(2 * time.Second)
>>>>>>> Stashed changes
		}
	}

	return "", "", "" // Falhou após retries
}

<<<<<<< Updated upstream
func (b *Broker) waitForDroneReturn(baseAddr, droneID string) {
	for {
=======
func (b *Broker) waitForDroneReturnAndNotify(baseAddr, rawDroneID, uniqueDroneName string, missionID int) {
	maxRetries := 60 // 60 tentativas de 2 segundos = 2 minutos máximo
	retryCount := 0

	for retryCount < maxRetries {
>>>>>>> Stashed changes
		time.Sleep(2 * time.Second)
		conn, err := net.DialTimeout("tcp", baseAddr, 2*time.Second)
		if err != nil {
			retryCount++
			continue
		}

		json.NewEncoder(conn).Encode(map[string]string{
			"action": "STATUS",
<<<<<<< Updated upstream
			"target": droneID,
=======
			"target": rawDroneID,
>>>>>>> Stashed changes
		})

		var resp map[string]string
		err = json.NewDecoder(conn).Decode(&resp)
		conn.Close()

<<<<<<< Updated upstream
		if resp["status"] == "LIVRE" {
			fmt.Printf("[%s] Drone %s retornou.\n", b.ID, droneID)
=======
		if err == nil && resp["status"] == "LIVRE" {
			b.MissionsMutex.Lock()
			delete(b.ActiveMissions, missionID)
			b.MissionsMutex.Unlock()

			// Emite RELEASE para todos os peers
			releaseClock := b.Clocks.UpdateP2P(0)
			for id := range b.Peers {
				b.sendDirectMessage(id, "RELEASE", 0, releaseClock, missionID)
			}
>>>>>>> Stashed changes
			return
		}

		retryCount++
	}

	// Timeout esperando drone voltar
	fmt.Printf("[%s] 💀 TIMEOUT: MissionID %d (%s) não retornou após %d tentativas\n",
		b.ID, missionID, uniqueDroneName, maxRetries)

	b.MissionsMutex.Lock()
	delete(b.ActiveMissions, missionID)
	b.MissionsMutex.Unlock()

	// Mesmo assim emite RELEASE
	releaseClock := b.Clocks.UpdateP2P(0)
	for id := range b.Peers {
		b.sendDirectMessage(id, "RELEASE", 0, releaseClock, missionID)
	}
}

func (b *Broker) releaseSection(missionID int) {
	b.StateMutex.Lock()
	defer b.StateMutex.Unlock()

	b.State = "IDLE"
<<<<<<< Updated upstream
	fmt.Printf("[%s] Liberando recursos e notificando fila...\n", b.ID)

	for _, pReq := range b.PendingQueue {
		b.sendDirectMessage(pReq.SenderID, "ACK")
=======

	// Processa fila de requisições pendentes
	for _, pReq := range b.PendingQueue {
		ackClock := b.Clocks.UpdateP2P(0)
		b.sendDirectMessage(pReq.SenderID, "ACK", 0, ackClock, 0)
>>>>>>> Stashed changes
	}
	b.PendingQueue = []Message{}
	b.CurrentReq = Message{}
}

<<<<<<< Updated upstream
func (b *Broker) sendDirectMessage(targetID, msgType string) {
=======
func (b *Broker) sendDirectMessage(targetID, msgType string, priority int, clockValue int, missionID int) bool {
>>>>>>> Stashed changes
	addr := b.Peers[targetID]
	if addr == "" {
		return false // Peer não existe
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(Message{
<<<<<<< Updated upstream
		Type:     msgType,
		SenderID: b.ID,
		Clock:    b.getNextClock(),
		Priority: b.CurrentReq.Priority,
=======
		Type:      msgType,
		SenderID:  b.ID,
		Clock:     clockValue,
		MissionID: missionID,
		Priority:  priority,
		Timestamp: time.Now().Unix(),
>>>>>>> Stashed changes
	})
}
