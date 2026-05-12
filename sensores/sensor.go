package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"
)

type SensorEvent struct {
	Type     string `json:"type"`     // ex: "EMBARCACAO_A_DERIVA", "BLOQUEIO"
	Priority int    `json:"priority"` // 1 a 3
	SectorID string `json:"sector_id"`
}

func main() {
	rand.Seed(time.Now().UnixNano())
	broker_addr := "localhost:5001" // IP do Broker do seu setor (via Docker ENV)
	sector_ID := "Setor_Alpha"

	eventos_possiveis := []string{"EMBARCACAO_DERIVA", "BLOQUEIO_ROTA", "OBJETO_NAO_IDENTIFICADO"}

	for {
		// Simula um intervalo autônomo e aleatório entre eventos
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Second)

		evento := SensorEvent{
			Type:     eventos_possiveis[rand.Intn(len(eventos_possiveis))],
			Priority: rand.Intn(3) + 1,
			SectorID: sector_ID,
		}

		sendToBroker(broker_addr, evento)
	}
}

func sendToBroker(address string, event SensorEvent) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Erro ao conectar com o Broker local:", err)
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	encoder.Encode(event)
	fmt.Printf("Sensor disparou evento crítico nível %d: %s\n", event.Priority, event.Type)
}
