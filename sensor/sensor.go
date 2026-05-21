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

// Define a estrutura do payload JSON emitido pelo hardware,
// encapsulando a tipagem da anomalia de rede, sua prioridade intrínseca
// de atendimento e o identificador do setor de origem.
type SensorEvent struct {
	Type     string `json:"type"`
	Priority int    `json:"priority"`
	SectorID string `json:"sector_id"`
}

// Inicializa o ciclo de vida do sensor autônomo, provisionando a entropia
// inicial, parseando os argumentos de injeção de dependência (topologia de rede)
// e orquestrando um loop estocástico de geração de eventos de telemetria.
func main() {
	rand.Seed(time.Now().UnixNano())
	sector_ID := os.Args[1]

	brokers_addrs := strings.Split(os.Args[2], ",")

	eventos_possiveis := []string{"EMBARCACAO_DERIVA", "BLOQUEIO_ROTA", "OBJETO_NAO_IDENTIFICADO"}

	for {
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Second)

		evento := SensorEvent{
			Type:     eventos_possiveis[rand.Intn(len(eventos_possiveis))],
			Priority: rand.Intn(3) + 1,
			SectorID: sector_ID,
		}

		sendToBrokerWithFailover(brokers_addrs, evento)
	}
}

// Implementa um mecanismo de resiliência ativo
// para transmissão de pacotes. A função interage recursivamente com a lista de endpoints conhecidos;
// em caso de falha de I/O no nó primário (timeout), o evento sofre roteamento progressivo para nós secundários.
func sendToBrokerWithFailover(brokers []string, event SensorEvent) {
	for i, addr := range brokers {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			defer conn.Close()
			encoder := json.NewEncoder(conn)
			encoder.Encode(event)

			if i == 0 {
				fmt.Printf("[Sensor %s] ✅ %s (Prio %d) enviado ao broker PRINCIPAL: %s\n", event.SectorID, event.Type, event.Priority, addr)
			} else {
				fmt.Printf("[Sensor %s] ⚠️ FAILOVER: %s (Prio %d) enviado ao broker SECUNDÁRIO: %s\n", event.SectorID, event.Type, event.Priority, addr)
			}
			return
		}
		fmt.Printf("[Sensor %s] ❌ Falha ao contatar broker %s. Buscando vizinho...\n", event.SectorID, addr)
	}

	fmt.Printf("[Sensor %s] 🚨 FATAL: Todos os brokers vizinhos caíram. Evento perdido!\n", event.SectorID)
}
