package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

// Define o contrato do payload JSON utilizado para a troca
// bidirecional de instruções de controle e telemetria entre o drone e a base.
type Command struct {
	Action  string `json:"action"`
	Target  string `json:"target"`
	DroneID string `json:"drone_id"`
}

// Inicializa o processo autônomo do drone, configurando o seed de entropia
// e iniciando um loop de resiliência ativo (client-side load balancing e failover)
// que tenta estabelecer conexão com nós de base disponíveis estocasticamente.
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: ./drone <BASE_ADDR1,BASE_ADDR2,...>")
		return
	}

	bases := strings.Split(os.Args[1], ",")

	rand.Seed(time.Now().UnixNano())

	fmt.Printf("[Sistema Drone] Iniciado. Bases conhecidas: %v\n", bases)

	for {
		targetBase := bases[rand.Intn(len(bases))]

		fmt.Printf("🔄 Tentando conectar à base: %s...\n", targetBase)
		err := runDroneCycle(targetBase)

		if err != nil {
			fmt.Printf("❌ Conexão perdida com %s: %v\n", targetBase, err)
			fmt.Println("⚠️ Iniciando protocolo de redistribuição em 3 segundos...")
			time.Sleep(3 * time.Second)
		}
	}
}

// Orquestra o ciclo de vida da sessão TCP com uma base operacional.
// Ele executa o handshake inicial de registro, provisiona o canal de keep-alive, e
// atua como um consumer bloqueante (event loop) para despachar comandos remotos (RPCs).
func runDroneCycle(baseAddr string) error {
	conn, err := net.DialTimeout("tcp", baseAddr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(Command{Action: "REGISTER"}); err != nil {
		return err
	}

	var ackCmd Command
	if err := decoder.Decode(&ackCmd); err != nil || ackCmd.Action != "REGISTER_ACK" {
		return fmt.Errorf("falha ao receber identidade oficial da base")
	}

	droneID := ackCmd.DroneID
	fmt.Printf("[%s] ✅ Registrado com sucesso na base %s!\n", droneID, baseAddr)

	done := make(chan struct{})
	defer close(done)

	go startHeartbeat(encoder, droneID, done)

	for {
		var incomingCmd Command
		if err := decoder.Decode(&incomingCmd); err != nil {
			return fmt.Errorf("desconexão detectada no decoder")
		}

		if incomingCmd.Action == "FLY" {
			if err := executeFlight(encoder, droneID, incomingCmd.Target); err != nil {
				return err
			}
		}
	}
}

// Mantém a persistência da sessão enviando pacotes periódicos de telemetria
// de forma assíncrona para o socket da base. O ciclo de vida da goroutine é delimitado
// pelo canal de cancelamento 'done'.
func startHeartbeat(encoder *json.Encoder, droneID string, done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			beatCmd := Command{
				Action:  "HEARTBEAT",
				DroneID: droneID,
			}
			encoder.Encode(beatCmd)
		case <-done:
			return
		}
	}
}

// Bloqueia a rotina de consumo do socket para simular a latência inerente
// à execução da tarefa de campo. Ao término da latência simulada, transmite um ACK
// de conclusão (callback) serializado de volta para a base.
func executeFlight(encoder *json.Encoder, droneID string, targetSector string) error {
	fmt.Printf("\n[%s] 🚀 Decolando em direção ao %s...\n", droneID, targetSector)

	time.Sleep(10 * time.Second)

	fmt.Printf("[%s] ✅ Missão em %s concluída. Retornando...\n", droneID, targetSector)

	completeCmd := Command{
		Action:  "MISSION_COMPLETE",
		DroneID: droneID,
	}

	if err := encoder.Encode(completeCmd); err != nil {
		fmt.Printf("[%s] ⚠️ Erro ao reportar conclusão. A base pode ter caído durante o voo.\n", droneID)
		return err
	}
	return nil
}
