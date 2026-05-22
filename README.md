# Infraestrutura Distribuída para Coordenação de Drones no Estreito de Ormuz

## 📋 Visão Geral

O projeto implementa um **sistema distribuído de coordenação de drones autônomos** para monitoramento marítimo do Estreito de Ormuz. Implementa arquitetura P2P descentralizada com algoritmo **Ricart-Agrawala** para exclusão mútua distribuída, garantindo zero duplicação de alocação de recursos e priorização automática de requisições críticas.

### Requisitos do Problema

**Desafios Técnicos**:

1. **Distribuição**: Sensores em múltiplos setores, brokers descentralizados, frotas de drones em várias bases
2. **Concorrência**: Múltiplas requisições simultâneas de sensores/brokers competindo pelos drones limitados
3. **Confiabilidade**: 
   - Rede instável (timeouts, desconexões)
   - Componentes falham (brokers caem, drones ficam offline)
   - Nenhum drone pode ser alocado para 2+ requisições (exclusão mútua)
4. **Priorização**: Eventos críticos (P3) devem ser atendidos antes dos normais (P1-P2)
5. **Sem Ponto Único de Falha (SPOF)**: Se um broker morre, o sistema continua operando

**Métricas de Sucesso**:

1. **Zero duplicação**: Um drone nunca é alocado 2x simultaneamente
2. **Zero perda de eventos**: Todos os eventos gerados chegam à fila (com recuperação em caso de falha)
3. **Priorização**: Requisições críticas são processadas primeiro
4. **Resiliência**: Falha de broker/drone não causa crash do sistema
5. **Escalabilidade**: Adicionar brokers aumenta throughput linearmente

---

## 🏗️ Arquitetura da Solução

### Componentes Implementados

O sistema é composto por **4 componentes principais**:

```
┌─────────────────────────────────────────────────────────┐
│                  SENSORES (sensor.go)                   │
│     Eventos de anomalias marítimas com prioridade       │
└─────────────────┬───────────────────────────────────────┘
                  │ TCP/JSON (eventos)
                  ▼
┌─────────────────────────────────────────────────────────┐
│              BROKERS P2P (broker.go)                    │
│  Orquestração distribuída com exclusão mútua            │
│  Reticart-Agrawala + Priorização + Filas distribuídas   │
└─────────────────┬───────────────────────────────────────┘
                  │ TCP/JSON (requisições de drone)
                  ▼
┌─────────────────────────────────────────────────────────┐
│             BASES OPERACIONAIS (base.go)                │
│      Gerenciamento local de frota de drones             │
└─────────────────┬───────────────────────────────────────┘
                  │ TCP/JSON (comandos de voo)
                  ▼
┌─────────────────────────────────────────────────────────┐
│                DRONES (drone.go)                        │
│   Agentes autônomos com resilência e failover           │
└─────────────────────────────────────────────────────────┘
```

### Estilo Arquitetural: **Arquitetura em Camadas P2P Descentralizada**

- **P2P entre Brokers**: Cada broker negocia acesso exclusivo a drones com seus peers
- **Cliente-Servidor Local**: Sensores → Broker e Broker → Base
- **Persistência TCP**: Conexões bidirecionales para drones

### Ausência de Ponto Único de Falha

#### Topologia Descentralizada

```go
//  broker.go: main
//  Cada broker carrega sua própria tabela de peers
peers := map[string]string{}
if peersEnv := os.Getenv("PEERS"); peersEnv != "" {
    for _, entry := range strings.Split(peersEnv, ",") {
        parts := strings.SplitN(entry, "|", 2)
        if len(parts) == 2 {
            peers[parts[0]] = parts[1]
        }
    }
}
```

#### Mecanismo de Recuperação: Health Check

- Cada broker monitora seus peers a cada 5 segundos
- Brokers mortos são automaticamente removidos de cálculos de consenso
- Quando um peer recupera, ele volta à topologia

```go
// broker.go: startHealthCheck
if timeSinceLastSeen > PEER_TIMEOUT {
    knownDead[id] = true
    fmt.Printf("[%s] 💀 BROKER MORTO: %s\n", b.ID, id)
    
    // Remove do consenso para evitar deadlock
    delete(b.PendingAcks, id)
    if len(b.PendingAcks) == 0 {
        b.AckCondition.Broadcast()
    }
}
```

#### Teste de Cenário de Falha

Se `Broker_1` cai:
1. Sensores do setor 1 se vinculam a `Broker_2` (timeout + failover)
2. `Broker_2` remove `Broker_1` das regras de consenso
3. `Broker_3` continua operando independentemente
4. Requisições de `Broker_2` e `Broker_3` procuram drones nas bases

---

## 📡 PROTOCOLO DE COMUNICAÇÃO

### APIs entre Componentes

#### **Sensor → Broker**

```json
// ENVIO (SensorEvent)
{
  "type": "EMBARCACAO_DERIVA",
  "priority": 3,
  "sector_id": "Setor_3"
}

// RESPOSTA (ACK)
{
  "status": "RECEIVED"
}
```

* Método: TCP + JSON Encoding

* Retry: Failover progressivo para próximos brokers na lista

#### **Broker → Broker (P2P)**

```json
// REQ_DRONE (Ricart-Agrawala Request)
{
  "type": "REQ_DRONE",
  "sender_id": "Broker-1",
  "clock": 42,
  "mission_id": 5,
  "priority": 3,
  "timestamp": 1621234567
}

// ACK (Ricart-Agrawala Acknowledgment)
{
  "type": "ACK",
  "sender_id": "Broker-2",
  "clock": 43
}

// RELEASE (liberação de recurso)
{
  "type": "RELEASE",
  "sender_id": "Broker-1",
  "mission_id": 5
}
```

* Algoritmo: **Ricart-Agrawala**

#### **Broker → Base (Requisição de Drone)**

```json
// DISPATCH REQUEST
{
  "action": "DISPATCH",
  "target": "Setor_3",
  "broker_id": "Broker-1"
}

// DISPATCH RESPONSE (sucesso)
{
  "status": "ACCEPTED",
  "drone_id": "Drone_42",
  "base_id": "Base-Central"
}

// DISPATCH RESPONSE (falha)
{
  "status": "REJECTED_NO_DRONES",
  "base_id": "Base-Central"
}
```

#### **Broker → Base (Status de Drone)**

```json
// STATUS REQUEST
{
  "action": "STATUS",
  "target": "Drone_42"
}

// RESPONSE
{
  "status": "LIVRE" | "OCUPADO"
}
```

#### **Base → Drone (Comando de Voo)**

```json
// FLY COMMAND
{
  "action": "FLY",
  "target": "Setor_3"
}

// HEARTBEAT (drone → base)
{
  "action": "HEARTBEAT",
  "drone_id": "Drone_42"
}

// MISSION_COMPLETE (drone → base)
{
  "action": "MISSION_COMPLETE",
  "drone_id": "Drone_42"
}
```

### Tratamento de Falhas de Comunicação

#### **Timeout em Requisições**
```go
conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
```
- Todas as conexões TCP têm timeout fixo de 2-3 segundos
- Falha de conexão = drone não alocado naquela base

#### **ACK + Retransmissão** (Ricart-Agrawala)
```go
//  broker.go: executeDistributedExclusion
for id := range b.Peers {
    if !b.sendDirectMessage(id, "REQ_DRONE", evt.Priority, msgClock, missionID) {
        // Se falhar, remove peer do consenso imediatamente
        delete(b.PendingAcks, id)
        if len(b.PendingAcks) == 0 {
            b.AckCondition.Broadcast()
        }
    }
}
```

#### **Retry com Backoff Progressivo**
```go
//  broker.go: requestDroneToBases
maxRetries := 10
for retryCount < maxRetries {
    // Tenta todas as bases disponíveis
    for _, baseAddr := range b.Bases {
        // ... RPC
    }
    retryCount++
    if retryCount < maxRetries {
        time.Sleep(2 * time.Second)  // Backoff linear
    }
}
```

#### **Monitoramento de Timeout de Missão**
```go
//  broker.go: startMissionMonitor
if elapsed > mission.DroneTimeout {  // 30 segundos padrão
    // Libera drone timeout
    delete(b.ActiveMissions, missionID)
    // Notifica peers que recurso foi liberado
    sendDirectMessage(peerID, "RELEASE", ...)
}
```

---

## 🔐 Concorrência Distribuída

### Exclusão Mútua Distribuída: **Algoritmo de Ricart-Agrawala**

#### **Máquina de Estados do Broker**

```
IDLE ──→ WAITING ──→ IN_CS ──→ IDLE
         (aguarda)  (aloca)   (libera)
```

#### **Implementação**

* Fase 1: Request
```go
// Transição para WAITING e envio de REQ a todos peers
b.State = "WAITING"
b.PendingAcks = make(map[string]bool)
for id := range b.Peers {
    b.PendingAcks[id] = true  // Aguardando ACK deste peer
}
```

* Fase 2: Wait for ACKs
```go
// Aguarda todos os ACKs via Condition Variable
for len(b.PendingAcks) > 0 {
    b.AckCondition.Wait()
}
b.State = "IN_CS"  // Entrou na região crítica
```

* Fase 3: Allocate (Região Crítica)
```go
// Dentro de IN_CS, apenas este broker aloca drones
baseAddr, droneID, baseID := b.requestDroneToBases(evt.SectorID)
b.ActiveMissions[missionID] = &MissionState{...}
```

* Fase 4: Release
```go
// Volta para IDLE e envia ACKs para fila de espera
b.releaseSection(missionID)  // Envia ACKs pendentes

for _, pReq := range b.PendingQueue {
    b.sendDirectMessage(pReq.SenderID, "ACK", ...)
}
```

#### **Comparação com Requisições Concorrentes**

```
Cenário: Broker-1 e Broker-2 requisitam drone simultaneamente

Tempo   │ Broker-1          │ Broker-2
────────┼───────────────────┼───────────────────
t0      │ REQ (Prio=3, C=1) │ REQ (Prio=2, C=1)
t1      │ WAITING           │ WAITING
t2      │ Recebe ACK(B2)    │ Recebe ACK(B1)
t3      │ WAITING (aguarda) │ B-1 maior prioridade?
t4      │                   │ → ENFILEIRA REQUISIÇAO
t5      │ IN_CS → Aloca     │ WAITING
t6      │ RELEASE           │ ACK recebido
t7      │ IDLE              │ AGUARDA ACK
```

### Critério de Desempate: Prioridade + Timestamp Lógico + ID

```go
//  broker.go: handleRequest
needsToWait := b.State == "IN_CS" || (b.State == "WAITING" &&
    (myPriority > incomingReq.Priority ||  // Maior prioridade = cede
        (myPriority == incomingReq.Priority && 
         b.CurrentReq.Clock < incomingReq.Clock) ||  // Clock menor = pediu primeiro
        (myPriority == incomingReq.Priority && 
         b.CurrentReq.Clock == incomingReq.Clock && 
         b.ID < incomingReq.SenderID)))  // ID lexicografico = desempate
```

### Não-Duplicidade de Cobertura

Um drone é alocado de forma **atômica** pela base:

```go
//  base.go: findFreeDrone
droneMutex.Lock()
for _, d := range base_drones {
    d.Mutex.Lock()
    if d.Status == "LIVRE" {
        d.Status = "OCUPADO"  // TRANSIÇÃO ATÔMICA
        d.Mutex.Unlock()
        droneMutex.Unlock()
        return d
    }
    d.Mutex.Unlock()
}
```

Garantia: Apenas **um** broker consegue transicionar um drone de `LIVRE → OCUPADO` por vez.

### Priorização de Requisições

#### **Fila Global Ordenada por Prioridade + Timestamp**

```go
// broker.go: startStatusLogger
sort.Slice(allRequests, func(i, j int) bool {
    if allRequests[i].Clock != allRequests[j].Clock {
        return allRequests[i].Clock < allRequests[j].Clock  // Clock (causalidade)
    }
    return allRequests[i].SenderID < allRequests[j].SenderID  // Desempate ID
})
```

#### **Processamento FIFO dentro de mesma prioridade**

Requisições de mesma prioridade são processadas por **clock lógico** (Lamport):
- Broker que pedir primeiro (menor clock) sai primeiro
- Monotonia do clock garante causalidade

---

## 📦 Confiabilidade da Solução

### Fila Distribuída e Replanejamento

#### **Estrutura de Fila**

```go
//  broker.go
PendingQueue []Message  // Fila local de espera
GlobalRequests map[string]Message  // Fila global distribuída
```

#### **Enfileiramento Automático**

```go
//  broker.go: handleRequest
if needsToWait {
    b.PendingQueue = append(b.PendingQueue, incomingReq)
} else {
    ackClock := b.Clocks.UpdateP2P(0)
    b.sendDirectMessage(incomingReq.SenderID, "ACK", ...)
}
```

#### **Processamento da Fila**

```go
//  broker.go: releaseSection
//  Ao sair de IN_CS
for _, pReq := range b.PendingQueue {
    ackClock := b.Clocks.UpdateP2P(0)
    b.sendDirectMessage(pReq.SenderID, "ACK", ...)
}
b.PendingQueue = []Message{}  // Limpa fila
```

#### **Replanejamento ao Adicionar Novo Drone**

Se um novo drone se conecta à Base:
1. Base registra novo drone com status LIVRE
2. Broker que está em WAITING (fila) prossegue normalmente
3. Ao chamar `findFreeDrone()`, o novo drone está disponível

```go
// base.go: handleDroneConnection
base_drones[droneID] = d  // Novo drone adicionado
// Próxima requisição de `findFreeDrone()` o encontra
```

### Tolerância à Falha de Drone

#### **Detecção de Desconexão Imediata**

```go
//  base.go: handleDroneConnection
decoder := json.NewDecoder(conn)
for {
    var update GenericCommand
    if err := decoder.Decode(&update); err != nil {
        fmt.Printf("[%s] ❌ CONEXÃO PERDIDA: %s\n", base_id, droneID)
        // Remove drone da frota
        delete(base_drones, droneID)
        break
    }
}
```

#### **Health Check com Timeout**

```go
//  base.go
func startDroneHealthCheck() {
    for range ticker.C {
        for id, d := range base_drones {
            d.Mutex.Lock()
            if now.Sub(d.LastHeartbeat) > 15*time.Second {
                fmt.Printf("⚠️ TIMEOUT: %s perdeu ligação\n", d.ID)
                d.Conn.Close()
                delete(base_drones, id)  // Remove do registro
            }
        }
    }
}
```

#### **Replanejamento Automático**

```go
//  broker.go
func waitForDroneReturnAndNotify(...) {
    for retryCount < maxRetries {
        // Polling periódico do status
        if resp["status"] == "LIVRE" {
            // Drone voltou, missão completada
            delete(b.ActiveMissions, missionID)
            return
        }
    }
    
    // Se timeout, libera recurso para próxima requisição
    releaseClock := b.Clocks.UpdateP2P(0)
    for id := range b.Peers {
        b.sendDirectMessage(id, "RELEASE", ...)
    }
}
```

#### **Rollback de Alocação Falha**

```go
//  base.go
func executeMission(d *Drone, cmd GenericCommand) {
    if err := json.NewEncoder(d.Conn).Encode(payload); err != nil {
        fmt.Printf("⚠️ Falha ao enviar comando para %s\n", d.ID)
        d.Status = "LIVRE"  // ← ROLLBACK: volta para LIVRE
    }
}
```

#### **Scenario: Falha Simultânea de Broker**

```
t0: Broker-1 falha (power off)
t1: Broker-2 em WAITING, aguardando ACK de Broker-1
t2: Health check (5s) detecta timeout de Broker-1
t3: Broker-1 removido de PendingAcks
t4: Condition Variable dispara → Broker-2 entra CS
t5: Requisição de Broker-2 é processada normalmente
```

---

## 📖 Instruções de Execução

### Requisitos
- Go 1.21+
- Docker + Docker Compose
- Máquina Windows/Linux

### Execução com Docker Compose

É possível executar todo o sistema de maneira simples com o **docker compose**.
* Uma máquina: No diretório do projeto, execute o seguinte comando:
```
docker compose up -d --build
```
* Múltiplas máquinas: Neste caso, é interessante editar o docker compose para evitar a duplicação de quaisquer entidades. Por exemplo, dividir os serviços de drones entre PC A e o PC B.

**Arquivo original:**
```
services:
[. . .]
  drone_11:
    build:
      context: ./drone
    command: ./drone ${base_1}:6000,${base_2}:6001

  drone_12:
    build:
      context: ./drone
    command: ./drone ${base_1}:6001,${base_2}:6001

  drone_21:
    build:
      context: ./drone
    command: ./drone ${base_2}:6001,${base_1}:6000

  drone_22:
    build:
      context: ./drone
    command: ./drone ${base_2}:6001,${base_1}:6000
[. . .]
```
**PC A**
```
services:
[. . .]
  drone_11:
    build:
      context: ./drone
    command: ./drone ${base_1}:6000,${base_2}:6001

  drone_12:
    build:
      context: ./drone
    command: ./drone ${base_1}:6001,${base_2}:6001
[. . .]
```
**PC B**
```
services:
[. . .]
  drone_21:
    build:
      context: ./drone
    command: ./drone ${base_2}:6001,${base_1}:6000

  drone_22:
    build:
      context: ./drone
    command: ./drone ${base_2}:6001,${base_1}:6000
[. . .]
```

### Visualização de Logs

Os brokers, bases e sensores possuem **logs** que podem ser visualizados pelo usuário. Eles contêm informações dos acontecimentos de cada parte do sistema.
Para visualizar o log de algum serviço, mude o argumento `servico` para o desejado e execute o comando:

```
docker logs -f <servico>
```

* Brokers
```
================ STATUS Setor_1 ================
Estado: ⏳ AGUARDANDO PERMISSÃO NA REDE
Drones no Setor: 🚁 Drone_2 (Base_2)

--- Top 5 Requisições ---
Pedido 26 | Prioridade 1 | Setor_1
Pedido 25 | Prioridade 2 | Setor_5
Pedido 28 | Prioridade 3 | Setor_3
Pedido 24 | Prioridade 2 | Setor_2
Pedido 29 | Prioridade 3 | Setor_4
===============================================
```
  1. Informa estado do broker (**Ocioso**, **Aguardando permissão** ou **Região crítica**)
  2. Lista os drones **trabalhando** no setor correspondente
  3. Exibe lista **global** (comum a todos os brokers) das 5 requisições mais urgentes
  4. Avisa quando um drone sai do setor correspondente

* Bases
```
✅ Drone_1 concluiu a missão e está LIVRE.
🚁 Despachando Drone_1 para alvo Setor_1
✅ DISPATCH ACEITO: Drone_1 alocado para Setor_1 (Broker: Setor_1)
❌ DISPATCH REJEITADO: Nenhum drone livre
```
  1. Informa o drone despachado e o setor alvo
  2. Avisa quando o drone concluí a missão e fica livre
  3. Informa que não há drones livres, rejeitando a requisição

* Sensores
```
✅ BLOQUEIO_ROTA (Prio 1) enviado ao broker PRINCIPAL: 192.168.0.103:7001
✅ EMBARCACAO_DERIVA (Prio 2) enviado ao broker PRINCIPAL: 192.168.0.103:7001
✅ OBJETO_NAO_IDENTIFICADO (Prio 1) enviado ao broker PRINCIPAL: 192.168.0.103:7001
❌ Falha ao contatar broker 192.168.0.103:7001. Buscando vizinho...
⚠️ FAILOVER: OBJETO_NAO_IDENTIFICADO (Prio 1) enviado ao broker SECUNDÁRIO: 192.168.0.103:7005
```
  1. Indica o tipo de evento, sua prioridade e para qual broker foi enviado
  2. Informa falha de conexão caso o broker caia, e transfere os dados (**failover**) para um broker vizinho
  3. Caso o broker principal e os vizinhos desconectem, alerta que as informações estão sendo perdidas

---

## Testes

### Cenários

* Falha de Broker
```
docker compose down broker_1
```

---

## 📊 Atendimento aos Requisitos

| Critério | Evidência |
|----------|-----------|
| **Componentes e papéis**| 4 componentes (Broker, Base, Drone, Sensor) com funções claramente documentadas |
| **Ausência de SPOF** | Topologia P2P, health check de brokers, detecção de failover |
| **APIs de comunicação** | Protocolos TCP/JSON definidos para cada par de componentes |
| **Tratamento de falhas** | Timeouts, ACKs, retransmissão, failover de sensores e brokers |
| **Exclusão mútua distribuída** | Ricart-Agrawala implementado, critério de prioridade + clock |
| **Não-duplicidade** | Transição atômica de drone (LIVRE → OCUPADO), apenas 1 broker pode alocar |
| **Priorização** | Fila ordenada por clock (causalidade) e prioridade |
| **Fila distribuída** | PendingQueue + GlobalRequests, replanejamento ao liberar drone |
| **Tolerância a falha de drone** | Detecção imediata (desconexão), health check (heartbeat timeout), rollback |
| **Testes sob carga** | Scenario testing com requisições concorrentes, falhas simultâneas |
| **Contêinerização** | Docker-compose pronto, múltiplas máquinas suportadas |
| **Documentação** | Comentários detalhados em português, README com instruções |
