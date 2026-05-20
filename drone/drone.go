package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

// Command mapeia a estrutura JSON esperada pela base (GenericCommand no base.go)
type Command struct {
	Action  string `json:"action"`
	Target  string `json:"target"`
	DroneID string `json:"drone_id"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: ./drone <BASE_ADDR:PORT>")
		return
	}

	rand.Seed(time.Now().UnixNano())
	baseAddr := os.Args[1]

	// Gera um ID único combinando o hostname do container Docker e um número aleatório
	hostname, _ := os.Hostname()
	droneID := fmt.Sprintf("Drone_%s_%03d", hostname, rand.Intn(1000))

	fmt.Printf("[%s] Iniciando sistemas. Tentando conectar à base em %s...\n", droneID, baseAddr)

	// Estabelece conexão persistente com a base
	conn, err := net.Dial("tcp", baseAddr)
	if err != nil {
		fmt.Printf("[%s] ❌ Falha ao conectar à base: %v\n", droneID, err)
		os.Exit(1) // Encerra para que o Docker reinicie o container
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	// 1. Fase de Registro
	regCmd := Command{
		Action:  "REGISTER",
		DroneID: droneID,
	}
	if err := encoder.Encode(regCmd); err != nil {
		fmt.Printf("[%s] ❌ Erro ao enviar registro: %v\n", droneID, err)
		os.Exit(1)
	}
	fmt.Printf("[%s] ✅ Registrado com sucesso na base!\n", droneID)

	// 2. Inicia o envio periódico de Heartbeats (sinais de vida)
	go startHeartbeat(encoder, droneID)

	// 3. Loop principal: ouvindo comandos da Base
	for {
		var incomingCmd Command
		if err := decoder.Decode(&incomingCmd); err != nil {
			fmt.Printf("[%s] ❌ Conexão com a base perdida: %v\n", droneID, err)
			os.Exit(1) // O container morre e o Docker cuida de subir um novo
		}

		if incomingCmd.Action == "FLY" {
			executeFlight(encoder, droneID, incomingCmd.Target)
		}
	}
}

// startHeartbeat avisa a base a cada 5 segundos que este drone continua online
func startHeartbeat(encoder *json.Encoder, droneID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		beatCmd := Command{
			Action:  "HEARTBEAT",
			DroneID: droneID,
		}
		// Se der erro ao enviar, ignoramos aqui, pois o loop principal vai capturar a queda de conexão no Decode()
		encoder.Encode(beatCmd)
	}
}

// executeFlight simula a missão e avisa a base quando terminar
func executeFlight(encoder *json.Encoder, droneID string, targetSector string) {
	fmt.Printf("\n[%s] 🚁 RECEBEU ORDEM DE VOO!\n", droneID)
	fmt.Printf("[%s] 🚀 Decolando em direção ao %s...\n", droneID, targetSector)

	// Simula o tempo de voo e resolução do problema (10 segundos)
	time.Sleep(10 * time.Second)

	fmt.Printf("[%s] ✅ Missão no %s concluída. Retornando à base...\n", droneID, targetSector)

	completeCmd := Command{
		Action:  "MISSION_COMPLETE",
		DroneID: droneID,
	}

	if err := encoder.Encode(completeCmd); err != nil {
		fmt.Printf("[%s] ⚠️ Erro ao reportar conclusão da missão: %v\n", droneID, err)
		os.Exit(1)
	}
}
