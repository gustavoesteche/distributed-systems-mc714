# Projeto 1

Gustavo Esteche - 256223
Lucas Pimentel - 252615

## Como rodar (passo a passo)

Os comandos abaixo devem ser executados a partir da pasta `project1`.

### 1. Instalar o Docker

Instale o Docker e Docker Compose. Confirme com:

```bash
docker compose version
```

### 2. Ajustar os parâmetros do experimento

Edite o arquivo `experiment-settings.yml`:

- `LAMBDA`: taxa de chegada
- `MU`: taxa de serviço
- `DURATION`: duração do experimento, em segundos
- `--warmup-time` (no `transform.py`): segundos iniciais ignorados no cálculo das métricas
- `SEED`: semente das APIs
- `STRATEGY`: `random`, `round-robin`, `shortest-queue` ou `all`

### 3. Subir o sistema e rodar o experimento

```bash
chmod +x run.sh
./run.sh
```

O script:

1. Sobe as 3 APIs e os 3 load balancers em segundo plano
2. Acompanha os logs dos load balancers neste terminal (linhas `forwarding ...`)
3. Roda o container `experiment` em primeiro plano

Os CSVs brutos são gravados em `experiment/data/`.

Para parar os serviços depois:

```bash
docker compose -f docker-compose.yml -f experiment-settings.yml down
```

### 4. Ver logs dos load balancers (se não usou o `run.sh`)

Os encaminhamentos **não** aparecem no log do experimento. Eles são impressos no container de cada load balancer.

```bash
docker compose -f docker-compose.yml -f experiment-settings.yml logs -f \
  load-balancer-round-robin \
  load-balancer-random \
  load-balancer-shortest-queue
```

Ou um de cada vez, por exemplo:

```bash
docker compose -f docker-compose.yml -f experiment-settings.yml logs -f load-balancer-random
```

Você deve ver linhas no formato:

```text
load-balancer-random  | forwarding POST /process to http://project1-api-1:8080
```

### 5. Processar os resultados

Na pasta `project1`, com as dependências de `experiment/analysis/requirements.txt` instaladas:

```bash
python experiment/analysis/transform.py \
  --filepath experiment/data/lambda_12.0_mu_1.0.csv \
  --save-path experiment/processed_data/results.csv \
  --duration 500 \
  --warmup-time 50 \
  --lambda 1.2 \
  --mu 1
```

`--warmup-time` ignora os primeiros segundos do experimento ao calcular as métricas. O script acrescenta uma linha em `experiment/processed_data/results.csv` com `X`, `ER`, `EN`, `ETq`, `U1`, `U2`, `U3`, `lambda` e `mu`.

### Teste manual (opcional)

```bash
docker compose -f docker-compose.yml -f experiment-settings.yml up --build -d
```

Depois, requisições:

- Round Robin: `curl http://localhost:8080/ping`
- Random: `curl http://localhost:8081/ping`
- Shortest Queue: `curl http://localhost:8082/ping`

## Simulação teórica (sem Docker e sem espera real)

A implementação de eventos discretos em Go está em `simulation/`. Ela segue
o modelo do enunciado e avança um relógio virtual, sem usar `sleep`:

```bash
cd simulation
go test ./...
go run .
python3 -m pip install -r requirements.txt
python3 plot.py
```

O simulador é Go; os gráficos são gerados pelo script Python a partir dos
CSVs. Consulte `simulation/README.md` para as opções e saídas.
