# Lab 1 — Answers

## Quick note on my setup

My machine runs WSL2 on **ARM64** (`aarch64`), not the usual `amd64`. The
official `gcr.io/google-samples/kubernetes-bootcamp:v1` image is
`amd64`-only, so it crashed with `exec format error` as soon as I tried to
run it. I swapped it for **`baniyuga/kubernetes-bootcamp:v1`**, an
ARM64-compatible build of the same app, and everything worked the same way
from there.

Same story for `jocatalin/kubernetes-bootcamp:v2` and `:v3` in the rolling
update part — both `amd64`-only, so those pods just kept crashing. I used
`kubectl rollout undo` to roll back to the working ARM64 image instead of
forcing it.

## Part 1 — Installing minikube

`kubectl top pods -A --sort-by cpu --sum=true` shows CPU usage for every
pod across all namespaces, sorted from highest to lowest, plus a total row
at the end (`--sum=true`) adding it all up.

## Part 2 — kubectl basics

- No, you can't reach the app from outside the pod at this point. Port
  8080 only exists inside the pod's network — nothing is routing it to the
  outside world yet, that's what the Service is for.

## Part 3 — Exposing a service

- Exposed the deployment as `NodePort` on port `8080`, which mapped to a
  random high port on the cluster side (something like `32737`).
- Since I'm on the Docker driver, I needed `minikube service ...` to open
  a tunnel — after that, `http://127.0.0.1:<tunnel-port>` worked fine in
  the browser and showed `Hello Kubernetes bootcamp! | Running on: <pod-name>`.

## Part 4 — Scaling up and down

- Used `kubectl get pods -l app=kubernetes-bootcamp` to check how many pods
  were running.
- With 5 replicas, hammering Ctrl+F5 on the page kept showing different pod
  names each time — the Service is load-balancing across all the replicas.
- Scaling back down to 2, the other 3 pods went into `Terminating` and then
  disappeared.

## Part 5 — Rolling update

- Switching to `jocatalin/kubernetes-bootcamp:v2` triggered a
  `CrashLoopBackOff` — wrong architecture for my machine, so I couldn't
  actually see this specific update play out.
- `kubectl rollout undo deployments/kubernetes-bootcamp` rolled it back
  cleanly to the working ARM64 image.
- I did get to see the general rolling-update behavior earlier though — pod
  names changing as new pods take over from old ones.

## Part 6 — YAML manifests

- `TO COMPLETE #1` (deployment.yaml, containers) →
  `image: baniyuga/kubernetes-bootcamp:v1` (the ARM64 build).
- `TO COMPLETE #2` (deployment.yaml, spec) → `replicas: 1`, bumped to `3`
  later in step 6.6.
- `TO COMPLETE` (service.yaml, selector.app) → `kubernetes-bootcamp`,
  matching the pod's label.
- `TO COMPLETE` (service.yaml, ports.port) → `8080`, the app's listening
  port.
- After `kubectl apply -f deployment.yaml`, pods came up fine.
- After `kubectl apply -f service.yaml`, the app was reachable through the
  browser via the minikube tunnel.
- With `replicas: 3`, refreshing the page repeatedly showed three different
  pod names (`6wtpp`, `q4ztv`, `z65b5`) — confirms the load balancing across
  replicas.
