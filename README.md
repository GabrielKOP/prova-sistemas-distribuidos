# Trabalho Prático: Linha de Produção e Consumo Distribuída (TCP/IP & Asyncio)

Este projeto consiste em um sistema concorrente e distribuído composto por **8 instâncias independentes** que simulam uma linha de produção e consumo de mensagens. Toda a comunicação do sistema é realizada utilizando sockets **TCP/IP puros** com a biblioteca nativa `asyncio` do Python. A orquestração e execução de todos os contêineres ocorrem de forma integrada através do **Docker Compose**.

---

## 1. Topologia da Rede e Arquitetura

O sistema é dividido em 8 serviços que rodam em uma rede interna isolada no Docker (`dist-net`), estruturados em dois domínios principais:

```
                  [ DOMÍNIO DE PRODUÇÃO ]
+-------------------+  +-------------------+  +-------------------+
|  Produtor Node 1  |  |  Produtor Node 2  |  |  Produtor Node 3  |
|    (Gera 'A')     |  |    (Gera 'B')     |  |    (Gera 'C')     |
+---------+---------+  +---------+---------+  +---------+---------+
          |                      |                      |
          +-----------------+    |    +-----------------+
                            |    |    |
                            v    v    v (TCP)
                     +--------------------+
                     |  Servidor Produtor | (Porta 5000)
                     |    [ Fila FIFO ]   |
                     +---------+----------+
                               |
                               | (TCP + ACK)
                               v
                     [ DOMÍNIO DE CONSUMO ]
                     +--------------------+
                     | Servidor Consumidor| (Porta 6000 para Produtor)
                     |  [ Estoque A,B,C ] | (Porta 7000 para Consumidores)
                     |  [ Web Dash 8000 ] | (Porta 8000 para Navegador)
                     +----+----+----+-----+
                          |    |    |
          +---------------+    |    +---------------+
          |                    |                    |
          v (TCP)              v (TCP)              v (TCP)
+---------+---------+  +---------+---------+  +---------+---------+
| Consumidor Node 1 |  | Consumidor Node 2 |  | Consumidor Node 3 |
| (Pede A, B ou C)  |  | (Pede A, B ou C)  |  | (Pede A, B ou C)  |
+-------------------+  +-------------------+  +-------------------+
```

### 1.1. O que cada componente realiza:

1. **Nós Produtores (3 instâncias)**:
   * **Função**: Gerar produtos simulados em intervalos de tempo aleatórios.
   * **Máquina 1**: Responsável por gerar exclusivamente o produto **"A"**.
   * **Máquina 2**: Responsável por gerar exclusivamente o produto **"B"**.
   * **Máquina 3**: Responsável por gerar exclusivamente o produto **"C"**.
   * **Funcionamento**: Conectam-se como clientes TCP no Servidor Produtor, enviam a carga útil (dados em formato JSON) e encerram a conexão. Se o servidor estiver temporariamente fora do ar, o nó tenta se reconectar automaticamente para evitar a perda de dados.

2. **Servidor Produtor (1 instância)**:
   * **Função**: Centralizar o recebimento da produção de forma concorrente e assíncrona, armazenando os itens recebidos em uma fila FIFO em memória (`asyncio.Queue`).
   * **Garantia de Entrega (ACK)**: Para evitar a perda de mensagens em caso de oscilações ou quedas na rede, um processo worker em segundo plano realiza o envio. O produto só é removido da fila interna após o recebimento de um pacote de confirmação (`"status": "ACK"`) enviado pelo Servidor Consumidor. Se ocorrer falha de conexão ou timeout de 5 segundos, o worker realiza a retentativa de envio do mesmo item.

3. **Servidor Consumidor (1 instância)**:
   * **Função**: Centralizar os estoques em memória (utilizando filas exclusivas com deques para `A`, `B` e `C`) e gerenciar a distribuição sob demanda.
   * **Portas de escuta**:
     * Porta `6000`: Escuta conexões e dados enviados pelo Servidor Produtor (respondendo com ACK imediatamente).
     * Porta `7000`: Escuta conexões assíncronas dos 3 Nós Consumidores simultaneamente.
     * Porta `8000`: Serve o Painel Web (Dashboard) do sistema.
   * **Bloqueio de Demanda (CPU-Safe)**: Se um consumidor solicitar um produto que está fora de estoque, a requisição entra em estado de espera passiva segura (sem loops de consulta contínuos que sobrecarregam a CPU). A corrotina do consumidor é suspensa temporariamente na RAM (`await condition.wait()`) usando a primitiva assíncrona `asyncio.Condition()`, sendo reativada via `notify_all()` no momento exato em que um novo produto correspondente é entregue pela produção.

4. **Nós Consumidores (3 instâncias)**:
   * **Função**: Simular clientes concorrentes que demandam um produto aleatório (A, B ou C).
   * **Sincronização**: Após o envio do pedido, aguardam passivamente pela resposta antes de iniciar um novo ciclo de consumo.

---

## 2. Detalhes de Engenharia do Sistema

### 2.1. Protocolo de Enquadramento (TCP Framing)
Como o protocolo TCP é orientado a fluxo de bytes contínuo e não demarca o início e fim de mensagens individuais, foi adotada a seguinte solução em `network_utils.py`:
* Um cabeçalho inicial de **4 bytes** (inteiro em formato `big-endian`) é anexado a cada mensagem, definindo o tamanho exato do payload JSON que vem a seguir.
* O receptor lê inicialmente os 4 bytes do cabeçalho, calcula o tamanho necessário e faz uma leitura direta (`readexactly`) correspondente aos dados reais, eliminando problemas de fragmentação ou junção de pacotes.

### 2.2. Concorrência Assíncrona e Sincronização
* O sistema não utiliza threads ou processos pesados para tratar as conexões. Em vez disso, adota **corrotinas** concorrentes gerenciadas pelo *Event Loop* de thread única do Python, resultando em baixo consumo de recursos.
* A primitiva `asyncio.Condition()` resolve problemas de race conditions no estoque. Quando as corrotinas são acordadas com a chegada de novos itens, elas reavaliam o estoque usando a estrutura `while not stocks[type]`, garantindo que o primeiro consumidor ativo retire o produto e os demais retornem ao estado de espera seguro se o estoque for esgotado novamente.

### 2.3. Painel de Monitoramento (Web Dashboard)
Para facilitar a visualização do fluxo de dados:
* O Servidor Consumidor possui um servidor HTTP embutido que fornece uma página HTML/CSS/JS simples de forma nativa (sem dependências externas como Flask ou FastAPI).
* A atualização é baseada em **Server-Sent Events (SSE)**, que cria uma conexão unidirecional contínua entre o navegador e o servidor para o envio imediato de alterações de estado.
* **Console Invertido**: A lista de logs do painel exibe os eventos mais recentes sempre no topo da página.
* O acesso é feito através do endereço: **[http://localhost:8000](http://localhost:8000)**.

---

## 3. Estrutura do Projeto

* `network_utils.py`: Contém rotinas de framing (enquadramento de pacotes) e logs formatados com cores no console.
* `producer_node.py`: Código executado pelos geradores de itens A, B e C.
* `producer_server.py`: Código do gateway de produção, contendo a fila FIFO e lógica de entrega com ACK.
* `consumer_server.py`: Código do gerenciador de estoques, controle de sincronização de demanda e servidor do Dashboard.
* `consumer_node.py`: Código dos clientes que geram demandas no sistema.
* `requirements.txt`: Declara a dependência apenas da biblioteca padrão do Python (sistema livre de dependências externas).
* `Dockerfile`: Especificação para a construção da imagem Docker baseada no `python:3.11-slim` com logs não buferizados (`PYTHONUNBUFFERED=1`).
* `docker-compose.yml`: Script de orquestração de rede e serviços das 8 instâncias.

---

## 4. Requisitos para Execução

* **Docker** e **Docker Compose** instalados na máquina host.
* Python 3.11+ (caso queira executar as classes localmente sem contêineres).

---

## 5. Como Executar o Projeto

### 5.1. Inicializando os Contêineres
Abra um terminal no diretório raiz do projeto e execute o comando:
```bash
docker compose up --build
```
Este comando compila a imagem padrão, estabelece a rede virtual interna do Docker e inicia as 8 instâncias concorrentemente.

### 5.2. Acessando a Interface Visual
Com a rede do Docker ativa, abra o navegador e acesse:
👉 **[http://localhost:8000](http://localhost:8000)**

O painel exibirá o estoque disponível em tempo real, o número de clientes suspensos aguardando produtos e o terminal de logs com os eventos mais recentes no topo.

### 5.3. Finalizando os Serviços
Para interromper a execução e liberar as portas ocupadas do host, utilize `Ctrl+C` no console do compose, ou execute o comando abaixo em um terminal na mesma pasta:
```bash
docker compose down
```
