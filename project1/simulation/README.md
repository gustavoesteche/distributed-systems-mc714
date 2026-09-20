# Simulação teórica em Go

## Modelo implementado

- três servidores homogêneos, cada um com uma thread e fila FCFS ilimitada;
- chegadas de Poisson com taxa `lambda`;
- serviços exponenciais independentes com taxa `mu` por servidor;
- balanceador instantâneo;
- políticas `random`, `round-robin` e `shortest-queue`;
- opcionalmente `weighted-shortest-queue` (fila ponderada por `1/μ`), quando
  `-weighted-shortest-queue` está ligado: o servidor mais rápido continua a
  receber requisições mesmo com fila maior;
- desempate aleatório entre servidores com a menor fila;
- 5000 unidades simuladas por execução e warm-up de 500 por padrão;
- 10 sementes por configuração e intervalo de confiança de 95% (student);
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

Ou pode rodar apenas um comando, `run.sh`. O primeiro argumento `-output` escolhe a pasta dos resultados; o restante vai para o simulador:

```bash
./run.sh
./run.sh out_different_mu -mus 1.5,1.0,0.5 -weighted-shortest-queue
./run.sh -name out_weighted -mus 1.5,1.0,0.5 -weighted-shortest-queue
```

Exemplo com uma única política:

```bash
go run . \
  -strategy shortest-queue \
  -lambdas 0.6,1.2,1.8,2.4,2.7,3.3 \
  -mu 1 -duration 5000 -warmup 500 -runs 10
```

Cada execução sorteia uma semente-base diferente e a imprime na saída.
Para repetir exatamente os mesmos números, por exemplo, fixe a semente:

```bash
go run . -seed 714
```

Para configurar taxas diferentes por servidor:

```bash
go run . -mus 1.5,1.0,0.5
```

Para incluir a política de fila mais curta ponderada por `1/μ`:

```bash
go run . -mus 1.5,1.0,0.5 -weighted-shortest-queue -name out_different_mu
```

O flag acrescenta `weighted-shortest-queue` às políticas pedidas em
`-strategy` (por padrão `all`). Também é possível rodá-la sozinha com
`-strategy weighted-shortest-queue`. A regra compara `N_i/μ_i` em vez de
`N_i`, então um servidor rápido pode ganhar mesmo com mais requisições
do que um servidor lento.

`-mu 1` continua sendo o atalho para três servidores com a mesma taxa.
Quando as taxas são diferentes, a coluna `mu` dos CSVs usa o formato
`mu1;mu2;mu3`, mantendo a estrutura compacta pedida para os resultados.

Use `go run . -h` para ver todas as opções.

Se rodar o simulador e os scripts Python em separado, passe o mesmo nome
de pasta para os três:

```bash
go run . -name out_different_mu -mus 1.5,1.0,0.5 -weighted-shortest-queue
python3 confidence.py --name out_different_mu
python3 plot.py --name out_different_mu
python3 plot.py --name out_different_mu --no-analytical
```

`--no-analytical` omite a curva analítica da política aleatória.

## Saídas

Os arquivos são recriados a cada execução na pasta `out/` (ou na pasta
indicada por `-output`):

- `runs.csv`: dados detalhados por semente, incluindo o resíduo absoluto da
  Lei de Little em `little_law_gap_abs`;
- `results_random.csv`, `results_round_robin.csv` e
  `results_shortest_queue.csv`: médias com apenas
  `X,ER,EN,ETq,U1,U2,U3,lambda,mu`;
  com `-weighted-shortest-queue`, também
  `results_weighted_shortest_queue.csv`;
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
`lambda/3 < min(mu_i)`. Todas as políticas entram no gráfico de `E[R]`
enquanto `lambda < sum(mu_i)` (o caso `lambda=3.3` fica só em
`unstable_queues.png`). O eixo de `E[R]` é logarítmico para as políticas
que já divergiram continuarem visíveis junto das que ainda são estáveis.

Quando a condição de estabilidade deixa de valer, essas médias estacionárias
não existem. O gráfico das filas compara a simulação com a aproximação
fluida `lambda-sum(mu_i)`.
