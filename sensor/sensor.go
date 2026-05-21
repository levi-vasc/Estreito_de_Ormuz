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

type SensorEvent struct {
	Type     string `json:"type"`
	Priority int    `json:"priority"`
	SectorID string `json:"sector_id"`
}

func main() {
	rand.Seed(time.Now().UnixNano())
	sector_ID := os.Args[1]

	// Divide a string recebida no Docker Compose em um array de endereços
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

func sendToBrokerWithFailover(brokers []string, event SensorEvent) {
	for i, addr := range brokers {
		// Tenta conectar. Se falhar, o err não será nulo e ele vai para o próximo do loop
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
