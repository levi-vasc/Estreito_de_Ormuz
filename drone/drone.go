package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

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

	baseAddr := os.Args[1]
	fmt.Printf("[Novo Drone] Iniciando sistemas. Tentando conectar a %s...\n", baseAddr)

	conn, err := net.Dial("tcp", baseAddr)
	if err != nil {
		fmt.Printf("❌ Falha ao conectar à base: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	// 1. Solicita registro SEM identificação própria
	regCmd := Command{Action: "REGISTER"}
	if err := encoder.Encode(regCmd); err != nil {
		fmt.Printf("❌ Erro ao solicitar registro: %v\n", err)
		os.Exit(1)
	}

	// 2. Aguarda o "batismo" da Base (recebimento do ID oficial)
	var ackCmd Command
	if err := decoder.Decode(&ackCmd); err != nil || ackCmd.Action != "REGISTER_ACK" {
		fmt.Printf("❌ Falha ao receber identidade oficial da base\n")
		os.Exit(1)
	}

	// 3. Assume a identidade fornecida
	droneID := ackCmd.DroneID
	fmt.Printf("[%s] ✅ Registrado com sucesso na base!\n", droneID)

	// Inicia rotina de Heartbeat para manter conexão viva
	go startHeartbeat(encoder, droneID)

	// Loop principal de recebimento de comandos
	for {
		var incomingCmd Command
		if err := decoder.Decode(&incomingCmd); err != nil {
			fmt.Printf("[%s] ❌ Conexão com a base perdida: %v\n", droneID, err)
			os.Exit(1) // Morre para que o Docker reinicie o container
		}

		if incomingCmd.Action == "FLY" {
			executeFlight(encoder, droneID, incomingCmd.Target)
		}
	}
}

func startHeartbeat(encoder *json.Encoder, droneID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		beatCmd := Command{
			Action:  "HEARTBEAT",
			DroneID: droneID,
		}
		encoder.Encode(beatCmd)
	}
}

func executeFlight(encoder *json.Encoder, droneID string, targetSector string) {
	fmt.Printf("\n[%s] 🚀 Decolando em direção ao %s...\n", droneID, targetSector)

	// Simulação do tempo de voo e atendimento da ocorrência (10s)
	time.Sleep(10 * time.Second)

	fmt.Printf("[%s] ✅ Missão em %s concluída. Retornando...\n", droneID, targetSector)

	completeCmd := Command{
		Action:  "MISSION_COMPLETE",
		DroneID: droneID,
	}

	if err := encoder.Encode(completeCmd); err != nil {
		fmt.Printf("[%s] ⚠️ Erro ao reportar conclusão: %v\n", droneID, err)
		os.Exit(1)
	}
}
