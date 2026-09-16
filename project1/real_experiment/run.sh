set -e

compose() {
  docker compose -f docker-compose.yml -f experiment-settings.yml "$@"
}

compose up --build -d \
  api \
  load-balancer-round-robin \
  load-balancer-random \
  load-balancer-shortest-queue

# Load balancers start detached; follow their logs so forwarding lines
# appear in this terminal together with the experiment output.
compose logs -f \
  load-balancer-round-robin \
  load-balancer-random \
  load-balancer-shortest-queue &
logs_pid=$!
trap 'kill "$logs_pid" 2>/dev/null || true' EXIT

compose up --build --no-deps experiment
