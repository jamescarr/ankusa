#!/usr/bin/env bash
# Bring the jetstream-k8s example up on a local kind cluster, or tear it down.
#
#   ./run.sh          # same as ./run.sh up; safe to re-run
#   ./run.sh down     # delete the kind cluster
#
# See README.md for what runs where. Unlike oban-consumer's run.sh this is an
# interactive demo, so `up` never tears anything down.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Env knobs (override by exporting before invoking this script).
: "${CLUSTER:=ankusa-jetstream}"
: "${NS:=ankusa-jetstream}"
: "${ANKUSA_IMAGE:=jamescarr/ankusa:dev}"
# 0 skips `mise run docker:build`: use a published image, e.g.
#   BUILD_ANKUSA=0 ANKUSA_IMAGE=jamescarr/ankusa:edge ./run.sh
: "${BUILD_ANKUSA:=1}"
: "${ISTIO_VERSION:=1.30.6}"
: "${KEDA_VERSION:=2.21.0}"
: "${NATS_CHART_VERSION:=2.15.1}"
: "${METRICS_SERVER_CHART_VERSION:=3.14.0}"

DIR=examples/jetstream-k8s
CONSUMER_IMAGE=ankusa-jetstream-consumer:dev
PANEL_IMAGE=ankusa-jetstream-panel:dev

need() {
  for tool in "$@"; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      echo "error: required tool '$tool' not found on PATH (try: mise install)" >&2
      exit 2
    fi
  done
}

down() {
  need kind
  echo "==> deleting kind cluster '$CLUSTER'"
  kind delete cluster --name "$CLUSTER"
}

up() {
  need docker kind kubectl helm curl
  if [ "$BUILD_ANKUSA" = 1 ]; then need mise; fi

  echo "==> ensuring kind cluster '$CLUSTER' exists"
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    echo "    cluster '$CLUSTER' already exists, skipping creation"
  else
    kind create cluster --name "$CLUSTER" --config "$DIR/infra/kind.yaml"
  fi
  kubectl config use-context "kind-$CLUSTER" >/dev/null

  echo "==> building images"
  if [ "$BUILD_ANKUSA" = 1 ]; then mise run docker:build; fi
  docker build -f "$DIR/consumer/Dockerfile" -t "$CONSUMER_IMAGE" .
  docker build -t "$PANEL_IMAGE" "$DIR/control_panel"

  echo "==> loading images into kind"
  # An image that only exists in a registry (a published $ANKUSA_IMAGE) is
  # pulled by the nodes instead.
  for img in "$ANKUSA_IMAGE" "$CONSUMER_IMAGE" "$PANEL_IMAGE"; do
    if docker image inspect "$img" >/dev/null 2>&1; then
      kind load docker-image "$img" --name "$CLUSTER"
    else
      echo "    $img is not local; the nodes will pull it"
    fi
  done

  echo "==> helm repos"
  helm repo add istio https://istio-release.storage.googleapis.com/charts >/dev/null
  helm repo add nats https://nats-io.github.io/k8s/helm/charts/ >/dev/null
  helm repo add kedacore https://kedacore.github.io/charts >/dev/null
  helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ >/dev/null
  helm repo update >/dev/null

  echo "==> metrics-server (HPA needs pod CPU; kind's kubelets have self-signed certs)"
  helm upgrade --install metrics-server metrics-server/metrics-server \
    -n kube-system --version "$METRICS_SERVER_CHART_VERSION" \
    --set 'args={--kubelet-insecure-tls}' --wait

  echo "==> Istio $ISTIO_VERSION: base, istiod, ingress gateway"
  helm upgrade --install istio-base istio/base -n istio-system --create-namespace \
    --version "$ISTIO_VERSION" --set defaultRevision=default --wait
  helm upgrade --install istiod istio/istiod -n istio-system \
    --version "$ISTIO_VERSION" -f "$DIR/infra/istiod-values.yaml" --wait
  helm upgrade --install istio-ingress istio/gateway -n istio-ingress --create-namespace \
    --version "$ISTIO_VERSION" -f "$DIR/infra/istio-gateway-values.yaml" --wait

  echo "==> KEDA $KEDA_VERSION"
  helm upgrade --install keda kedacore/keda -n keda --create-namespace \
    --version "$KEDA_VERSION" --wait

  echo "==> namespace '$NS' (sidecar injection on)"
  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
  kubectl label namespace "$NS" istio-injection=enabled --overwrite

  echo "==> NATS: 3-node JetStream cluster"
  helm upgrade --install nats nats/nats -n "$NS" \
    --version "$NATS_CHART_VERSION" -f "$DIR/infra/nats-values.yaml" --wait --timeout 5m

  echo "==> demo chart: ankusa fleet, claim-check, floci, consumer, control panel, mesh routes"
  local existed=0
  if helm status demo -n "$NS" >/dev/null 2>&1; then existed=1; fi
  helm upgrade --install demo "$DIR/chart" -n "$NS" \
    --set "images.ankusa.repository=${ANKUSA_IMAGE%:*}" \
    --set "images.ankusa.tag=${ANKUSA_IMAGE##*:}" \
    --wait --timeout 10m
  if [ "$existed" = 1 ]; then
    # Rebuilt images keep the same :dev tag, so Helm sees nothing to change.
    kubectl -n "$NS" rollout restart deployment/ankusa deployment/claim-check \
      deployment/consumer deployment/control-panel
    kubectl -n "$NS" rollout status deployment/ankusa --timeout=300s
    kubectl -n "$NS" rollout status deployment/control-panel --timeout=300s
  fi

  echo "==> waiting for http://localhost:8080 (ingest through the gateway, panel behind it)"
  local ingest=000 panel=000
  for _ in $(seq 1 60); do
    ingest=$(curl -s -o /dev/null -w '%{http_code}' -XPOST localhost:8080/webhooks/demo \
      -H 'content-type: application/json' -d '{"id":"smoke"}' || true)
    panel=$(curl -s -o /dev/null -w '%{http_code}' localhost:8080/health || true)
    if [ "$ingest" = 201 ] && [ "$panel" = 200 ]; then break; fi
    sleep 2
  done
  if [ "$ingest" != 201 ] || [ "$panel" != 200 ]; then
    echo "error: POST /webhooks/demo returned $ingest (want 201), GET /health returned $panel (want 200)" >&2
    kubectl -n "$NS" get pods >&2
    exit 1
  fi

  cat <<EOF

==> up
    control panel   http://localhost:8080/
    ingest          curl -XPOST localhost:8080/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
    watch           kubectl -n $NS get pods,hpa,scaledobject -w
    tear down       $DIR/run.sh down
EOF
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  *)
    echo "usage: $0 [up|down]" >&2
    exit 2
    ;;
esac
