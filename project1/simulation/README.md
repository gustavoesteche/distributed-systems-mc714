# Simulação teórica em Go

## Modelo implementado

- três servidores homogêneos, cada um com uma thread e fila FCFS ilimitada;
- chegadas de Poisson com taxa `lambda`;
- serviços exponenciais independentes com taxa `mu` por servidor;
- balanceador instantâneo;
- políticas `random`, `round-robin` e `shortest-queue`;
- desempate aleatório entre servidores com a menor fila;
- 5000 unidades simuladas por execução e warm-up de 500 por padrão;
- 10 sementes por configuração e intervalo de confiança de 95% de Student;
- semente-base nova a cada execução (impressa no início); use `-seed <valor>`
  para reproduzir uma execução anterior;
- lambdas `0.6, 1.2, 1.8, 2.4, 2.7` e o caso instável `3.3`.

`N1`, `N2` e `N3` incluem a requisição em serviço. `Q1`, `Q2` e `Q3`
contêm apenas as requisições esperando.

## Execução

```bash
cd project1/simulation
go test ./...
go run .
python3 -m pip install -r requirements.txt
python3 confidence.py
python3 plot.py
```

O comando padrão executa todas as 18 configurações (seis taxas e três
políticas), com dez repetições cada. Como o tempo é apenas simulado, a
execução termina sem esperar as 5000 unidades no relógio real. 

Ou pode rodar apenas um comando, `run.sh`

Exemplo com uma única política:

```bash
go run . \
  -strategy shortest-queue \
  -lambdas 0.6,1.2,1.8,2.4,2.7,3.3 \
  -mu 1 -duration 5000 -warmup 500 -runs 10
```

Cada execução sorteia uma semente-base diferente e a imprime na saída.
Para repetir exatamente os mesmos números (por exemplo, os do relatório),
fixe a semente:

```bash
go run . -seed 714
```

Para configurar taxas diferentes por servidor:

```bash
go run . -mus 1.5,1.0,0.5
```

`-mu 1` continua sendo o atalho para três servidores com a mesma taxa.
Quando as taxas são diferentes, a coluna `mu` dos CSVs usa o formato
`mu1;mu2;mu3`, mantendo a estrutura compacta pedida para os resultados.

Use `go run . -h` para ver todas as opções.

## Saídas

Os arquivos são recriados em `out/` a cada execução:

- `runs.csv`: dados detalhados por semente, incluindo o resíduo absoluto da
  Lei de Little em `little_law_gap_abs`;
- `results_random.csv`, `results_round_robin.csv` e
  `results_shortest_queue.csv`: médias com apenas
  `X,ER,EN,ETq,U1,U2,U3,lambda,mu`;
- `queue_trace.csv`: tamanhos das filas ao longo do tempo para `lambda=3.3`.

O `confidence.py` lê `runs.csv` e escreve `results_ci.csv` com a média e o
intervalo de confiança de 95% (t de Student) de cada métrica por política e
`lambda`, calculados sobre as 10 réplicas.

O simulador Go produz apenas os CSVs. O `plot.py` lê esses arquivos e gera:

- `response_time.png`: curva analítica e pontos simulados com barras de erro;
- `unstable_queues.png`: crescimento médio das filas e aproximação fluida.

Para a política aleatória estável, cada servidor é uma fila M/M/1 com
taxa de chegada `lambda/3`. No caso homogêneo, a curva analítica usa:

- `E[R] = 1/(mu-lambda/3)`;
- `E[N] = lambda*E[R]`;
- `E[Tq] = E[R]-1/mu`;
- `Ui = lambda/(3*mu)`.

Com taxas diferentes, o gráfico usa
`E[R] = (1/3) * sum(1/(mu_i-lambda/3))`, válida quando
`lambda/3 < min(mu_i)`.

Quando a condição de estabilidade deixa de valer, essas médias estacionárias
não existem. O gráfico das filas compara a simulação com a aproximação
fluida `lambda-sum(mu_i)`.
