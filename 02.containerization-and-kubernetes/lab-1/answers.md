# Lab 1 — Answers

## Quick note on my setup

I actually ran this lab in two different environments, and the final
`deployment.yaml` / `service.yaml` committed here reflect the **Onyxia**
version (a VS Code service on a shared Kubernetes cluster), not the
original WSL/minikube run.

**On WSL2 (ARM64, `aarch64`)**: the official
`gcr.io/google-samples/kubernetes-bootcamp:v1` image is `amd64`-only, so
it crashed with `exec format error`. I swapped it for
`baniyuga/kubernetes-bootcamp:v1`, an ARM64-compatible build, to get
things working locally.

**On Onyxia**: the cluster runs `amd64`, so the official Google image
works fine with no swap needed. Instead, the namespace enforces a
**resource quota** (`onyxia-quota`) — every pod needs explicit
`resources.requests`/`resources.limits` or it gets rejected outright
(`failed quota: ... must specify limits.cpu ...`). My account also can't
use `NodePort` the way minikube does (no `minikube ip`, no Docker tunnel),
so the service is `ClusterIP` and I reach it with `kubectl port-forward`
or by calling the service name directly from inside the cluster.

## Part 1 — Installing minikube

`kubectl top pods -A --sort-by cpu --sum=true` shows CPU usage for every
pod across all namespaces, sorted from highest to lowest, plus a total row
at the end (`--sum=true`) adding it all up.

(Note: on Onyxia there's no `minikube start`/`status`/`ip` — the cluster
is already there, shared, with my own namespace pre-configured.)

## Part 2 — kubectl basics

- No, you can't reach the app from outside the pod at this point. Port
  8080 only exists inside the pod's network — nothing is routing it to the
  outside world yet, that's what the Service is for.

## Part 3 — Exposing a service

- **WSL**: exposed as `NodePort` on port `8080`, mapped to a random high
  port (`32737`-ish). Needed `minikube service ...` to open a Docker
  tunnel, then `http://127.0.0.1:<tunnel-port>` worked in the browser.
- **Onyxia**: `NodePort` isn't practical here — used `ClusterIP` instead
  and `kubectl port-forward service/kubernetes-bootcamp-service
  8082:8080`, then `curl localhost:8082` (had to pick a port other than
  8080, since VS Code's own web UI already uses that one). Both showed
  `Hello Kubernetes bootcamp! | Running on: <pod-name>`.

## Part 4 — Scaling up and down

- Used `kubectl get pods -l app=kubernetes-bootcamp` to check how many
  pods were running.
- With multiple replicas, repeated `curl`/refreshes showed different pod
  names — the Service load-balances across all replicas. One catch on
  Onyxia: `kubectl port-forward` targets one fixed pod and does **not**
  load-balance; calling the service by its DNS name from inside the
  cluster (`curl kubernetes-bootcamp-service:8080`) does show the
  round-robin behavior properly.
- Scaling back down, the extra pods went into `Terminating` and then
  disappeared.

## Part 5 — Rolling update

- Switching to `jocatalin/kubernetes-bootcamp:v2` triggered a
  `CrashLoopBackOff` on WSL (wrong architecture). `kubectl rollout undo`
  rolled it back cleanly to the working ARM64 image.
- `jocatalin/kubernetes-bootcamp:v3` doesn't exist on Docker Hub at all —
  it fails with `ImagePullBackOff` no matter the platform. Since
  `RollingUpdate` waits for the new pod to be ready before killing old
  ones, the old (working) pods just kept serving traffic the whole time —
  no downtime despite the broken update.
- Rolled back to the original image with `kubectl rollout undo`.

## Part 6 — YAML manifests

Final values used (Onyxia version, matching the committed files):

- `TO COMPLETE #1` (deployment.yaml, containers) →
  `image: gcr.io/google-samples/kubernetes-bootcamp:v1`, plus a
  `resources` block (`requests`/`limits` for cpu and memory) — required
  by the Onyxia quota, not part of the original lab instructions.
- `TO COMPLETE #2` (deployment.yaml, spec) → `replicas: 1`, bumped to `3`
  later in step 6.6.
- `TO COMPLETE` (service.yaml, selector.app) → `kubernetes-bootcamp`,
  matching the pod's label.
- `TO COMPLETE` (service.yaml, ports.port) → `8080`, the app's listening
  port. Service type is `ClusterIP` (not `NodePort`) with `targetPort:
  8080` added explicitly.
- After `kubectl apply -f deployment.yaml`, pods came up fine (once the
  resource quota was satisfied).
- After `kubectl apply -f service.yaml`, the app was reachable via
  `kubectl port-forward` and via the service's DNS name from inside the
  cluster.
- With `replicas: 3`, calling the service by DNS name repeatedly showed
  three different pod names — confirms load balancing across replicas.
