# Lab 2 — Answers

## Quick note on my setup

This time I ran the lab directly on **Onyxia** (a VS Code service on a
shared Kubernetes cluster) instead of local minikube. A couple of things
work differently there:

- My account can't create namespaces (`Forbidden: cannot create resource
  "namespaces" at the cluster scope`), so everything below runs in my
  assigned namespace instead of a dedicated `lab-configmap-secret`
  namespace. All the `-n lab-configmap-secret` flags and the `namespace:`
  field in the YAML files were dropped for that reason.
- The namespace has a **resource quota** (`onyxia-quota`) that caps total
  memory/CPU. Every pod needed explicit `resources.requests` and
  `resources.limits`, and at one point I had to delete earlier pods
  (`spark-app`, `ceph-connector`) before I had enough quota left to create
  `spark-config-reader`.
- Teardown used `kubectl delete configmap/secret/pod` one by one instead
  of `kubectl delete namespace`, since I don't own a dedicated namespace
  to delete.

## Part 2 — ConfigMaps

All three creation methods worked without any issues:

- **From file** (`pipeline-config.properties`) → `kubectl create configmap
  pipeline-config --from-file=...`
- **From literals** (`data-paths`) → three `--from-literal` flags for
  bronze/silver/gold paths.
- **From YAML** (`configmap-data-platform.yaml`) → includes both simple
  key-value pairs and multi-line file content (`application.yaml`,
  `table_mappings.csv`).

Inspecting it confirmed the multi-line values are stored as-is under
`data`, and `kubectl get configmap ... -o jsonpath='{.data.application\.yaml}'`
correctly pulls out just that one file's content.

## Part 3 — Secrets

Same three methods, same result — data.access-key etc. show up
base64-encoded in `kubectl get secret ... -o yaml` (e.g.
`QUtJQUlPU0ZPRE5ON0VYQU1QTEU=`).

Decoding:
- Manual: `kubectl get secret ... -o jsonpath='{.data.secret-key}' |
  base64 -d` gives back the plaintext key.
- With `jq`: `kubectl get secret ... -o json | jq -r '.data | to_entries[]
  | "\(.key)=\(.value | @base64d)"'` decodes every field at once — handy
  when a Secret has more than one or two keys.

## Part 4 — Using ConfigMap & Secret in pods

**Env vars from ConfigMap** (`pod-with-configmap.yaml`): `kubectl exec
spark-app -- env | grep -E "LOG_LEVEL|SPARK"` showed `LOG_LEVEL=INFO` and
`SPARK_WORKERS=4` coming straight from the ConfigMap, right alongside the
image's own built-in `SPARK_*` vars.

**Env vars from Secret** (`pod-with-secret.yaml`): the pod prints `Access
Key: AKIAIOSFODNN7EXAMPLE` from its command, confirming the Secret value
made it into the environment. Worth noting: this pod's `RESTARTS` climbed
to 3-4 even though it completed successfully each time — with no
`restartPolicy: Never` set, Kubernetes just restarts it again by default.

**ConfigMap as a volume** (`pod-with-configmap-volume.yaml`): both
`application.yaml` and `tables.csv` (renamed from `table_mappings.csv`
via `items`) showed up correctly under `/etc/config/`. `ls -la` reveals
Kubernetes actually mounts these as symlinks into a timestamped hidden
directory (`..2026_09_21_22_46_39...`) — that's how it does atomic
updates if the ConfigMap changes later without breaking anything mid-read.

**Secret as a volume** (`pod-with-secret-volume.yaml`): `cat
/etc/ceph-credentials/s3-credentials.txt` printed the full credentials
file exactly as stored, mounted read-only with `defaultMode: 0600`.

## Cleanup

Deleted every pod, ConfigMap and Secret individually since there was no
dedicated namespace to drop in one shot. Final `kubectl get all` /
`get configmap` / `get secret` only showed the platform's own resources
(my VS Code pod/service, `kube-root-ca.crt`, and Onyxia's internal
Helm/service secrets) — nothing left over from the lab.
