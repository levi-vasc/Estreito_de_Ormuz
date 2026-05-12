package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// Estrutura de Mensagens para comunicação P2P entre Brokers
type Message struct {
	Type     string `json:"type"`      // "REQ_DRONE", "ACK"
	SenderID string `json:"sender_id"` // ID do Broker (ex: "Setor_A")
	Clock    int    `json:"clock"`     // Relógio de Lamport
	Priority int    `json:"priority"`  // Prioridade da ocorrência (1-3)
}

type Broker struct {
	ID    string
	Port  string
	Peers map[string]string // ID -> IP:Porta dos outros Brokers
	Bases []string          // Lista de IP:Porta das Bases de Drones

	// Estado de Lamport e Concorrência
	LamportClock int
	ClockMutex   sync.Mutex

	// Estado do Algoritmo de Ricart-Agrawala
	State        string // "IDLE", "WAITING", "IN_CS"
	StateMutex   sync.Mutex
	AcksReceived int
	PendingQueue []Message
	AckCondition *sync.Cond // Para bloquear o processo até receber todos os ACKs

	CurrentReq Message // Armazena o pedido que o broker está tentando processar
}

func main() {
	// Exemplo de execução. Em produção, use Flags ou ENV vars.
	// go run broker.go Setor_A 5001 (5001 p2p, 6001 sensores)
	id := os.Args[1]
	p2pPort := os.Args[2]
	sensorPort := os.Args[3]

	// Configuração da rede (isso deve vir de um arquivo ou ENV no Docker)
	peers := map[string]string{
		"Setor_B": "localhost:5002",
		"Setor_C": "localhost:5003",
	}
	bases := []string{"localhost:6000", "localhost:6001"}

	broker := NewBroker(id, p2pPort, peers, bases)

	go broker.startSensorServer(sensorPort)
	broker.startP2PServer()
}

func NewBroker(id, port string, peers map[string]string, bases []string) *Broker {
	b := &Broker{
		ID:    id,
		Port:  port,
		Peers: peers,
		Bases: bases,
		State: "IDLE",
	}
	b.AckCondition = sync.NewCond(&b.StateMutex)
	return b
}

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

// Inicia o servidor P2P para falar com outros Brokers
func (b *Broker) startP2PServer() {
	ln, _ := net.Listen("tcp", ":"+b.Port)
	fmt.Printf("[Broker %s] Ouvindo rede P2P na porta %s\n", b.ID, b.Port)
	for {
		conn, _ := ln.Accept()
		go b.handleP2PConnection(conn)
	}
}

func (b *Broker) handleP2PConnection(conn net.Conn) {
	defer conn.Close()
	var msg Message
	json.NewDecoder(conn).Decode(&msg)

	b.updateClock(msg.Clock)

	b.StateMutex.Lock()
	defer b.StateMutex.Unlock()

	if msg.Type == "ACK" {
		b.AcksReceived++
		fmt.Printf("[Broker %s] ACK recebido de %s (%d/%d)\n", b.ID, msg.SenderID, b.AcksReceived, len(b.Peers))
		b.AckCondition.Broadcast() // Acorda a thread que está esperando ACKs
	} else if msg.Type == "REQ_DRONE" {
		b.handleRequest(msg)
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
		fmt.Printf("[Broker %s] Pedido de %s ENFILEIRADO.\n", b.ID, incomingReq.SenderID)
	} else {
		b.sendDirectMessage(incomingReq.SenderID, "ACK")
	}
}

// Servidor para Sensores Locais
func (b *Broker) startSensorServer(port string) {
	ln, _ := net.Listen("tcp", ":"+port)
	fmt.Printf("[Broker %s] Ouvindo Sensores na porta %s\n", b.ID, port)
	for {
		conn, _ := ln.Accept()
		go b.processSensorData(conn)
	}
}

func (b *Broker) processSensorData(conn net.Conn) {
	defer conn.Close()
	var sensorEvt map[string]interface{}
	json.NewDecoder(conn).Decode(&sensorEvt)

	priority := int(sensorEvt["priority"].(float64))
	fmt.Printf("\n[Broker %s] !!! OCORRÊNCIA PRIORIDADE %d !!!\n", b.ID, priority)

	// INÍCIO DO PROTOCOLO DE EXCLUSÃO MÚTUA
	b.StateMutex.Lock()
	b.State = "WAITING"
	b.AcksReceived = 0
	b.CurrentReq = Message{
		Type:     "REQ_DRONE",
		SenderID: b.ID,
		Clock:    b.getNextClock(),
		Priority: priority,
	}
	b.StateMutex.Unlock()

	// Broadcast do pedido para todos os vizinhos
	for id := range b.Peers {
		b.sendDirectMessage(id, "REQ_DRONE")
	}

	// Espera até ter todos os ACKs
	b.StateMutex.Lock()
	for b.AcksReceived < len(b.Peers) {
		b.AckCondition.Wait()
	}
	b.State = "IN_CS" // Entrou na Seção Crítica
	b.StateMutex.Unlock()

	// SEÇÃO CRÍTICA: Tentar alocar o drone nas bases
	fmt.Printf("[Broker %s] Permissão concedida. Contatando bases...\n", b.ID)
	b.requestDroneToBases()

	// SAÍDA DA SEÇÃO CRÍTICA
	b.releaseSection()
}

// --- COMUNICAÇÃO COM BASES E OUTROS ---

func (b *Broker) requestDroneToBases() {
	success := false
	for !success {
		for _, baseAddr := range b.Bases {
			conn, err := net.DialTimeout("tcp", baseAddr, 2*time.Second)
			if err != nil {
				continue
			}

			json.NewEncoder(conn).Encode(map[string]string{
				"action":    "DISPATCH",
				"target":    b.ID,
				"broker_id": b.ID,
			})

			var resp map[string]string
			json.NewDecoder(conn).Decode(&resp)
			conn.Close()

			if resp["status"] == "ACCEPTED" {
				fmt.Printf("[Broker %s] Drone %s alocado pela base %s!\n", b.ID, resp["drone_id"], resp["base_id"])
				success = true
				break
			}
		}
		if !success {
			fmt.Printf("[Broker %s] Sem drones em nenhuma base. Re-tentando...\n", b.ID)
			time.Sleep(3 * time.Second)
		}
	}
}

func (b *Broker) releaseSection() {
	b.StateMutex.Lock()
	defer b.StateMutex.Unlock()

	b.State = "IDLE"
	fmt.Printf("[Broker %s] Liberando recursos e notificando fila...\n", b.ID)

	for _, pReq := range b.PendingQueue {
		b.sendDirectMessage(pReq.SenderID, "ACK")
	}
	b.PendingQueue = []Message{}
	b.CurrentReq = Message{}
}

func (b *Broker) sendDirectMessage(targetID, msgType string) {
	addr := b.Peers[targetID]
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(Message{
		Type:     msgType,
		SenderID: b.ID,
		Clock:    b.getNextClock(),
		Priority: b.CurrentReq.Priority,
	})
}
