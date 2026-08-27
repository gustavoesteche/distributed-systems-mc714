# Projeto 1

Gustavo Esteche - Coloca o RA aqui faz favor
Lucas Pimentel - 252615

# Como rodar

- Instale Docker + Docker Compose
- Execute `docker compose up --build` na raiz do projeto 1
- Faça requisições via CURL GET para:
  - Round Robin: `http://localhost:8080/ping`
  - Random: `http://localhost:8081/ping`
  - Shortest Queue: `http://localhost:8082/ping`
- Consulte logs via `docker compose logs -f`
