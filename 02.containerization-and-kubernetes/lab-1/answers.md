# Lab 1 — Réponses aux questions

## Note sur l'environnement

Ma machine tourne sous WSL2 en architecture **ARM64** (`aarch64`). L'image
officielle `gcr.io/google-samples/kubernetes-bootcamp:v1` n'est disponible
qu'en `amd64` et échoue au démarrage avec `exec format error`. J'ai donc
utilisé l'image de remplacement compatible ARM64 **`baniyuga/kubernetes-bootcamp:v1`**,
qui reproduit le même comportement applicatif (même code Node.js).

Pour la même raison, les images `jocatalin/kubernetes-bootcamp:v2` et `:v3`
utilisées dans la partie 5 (rolling update) sont elles aussi en `amd64`
uniquement et n'ont pas pu être testées sur cette machine (crash immédiat
en `Error` / `exec format error`). Le rollback (`kubectl rollout undo`) a
bien fonctionné et a restauré le déploiement stable sur l'image ARM64.

## Partie 1 — Installation de minikube

`kubectl top pods -A --sort-by cpu --sum=true` liste la consommation CPU de
tous les pods de tous les namespaces, triée par ordre décroissant de CPU
utilisé, avec une ligne de total (`--sum=true`) agrégeant la consommation
de l'ensemble des pods.

## Partie 2 — Commandes `kubectl`

- Hors du pod, l'application **n'est pas accessible** depuis la machine
  locale : le port 8080 n'est exposé qu'à l'intérieur du réseau du pod,
  aucun Service Kubernetes ne le relie encore vers l'extérieur.

## Partie 3 — Exposer un service

- Le service a été exposé en `NodePort` sur le port `8080` (port interne)
  mappé à un port aléatoire côté cluster (ex: `32737`).
- Avec le driver Docker sous Linux/WSL, un tunnel (`minikube service ...`)
  est nécessaire pour accéder au service depuis le navigateur — confirmé
  en ouvrant l'URL `http://127.0.0.1:<port_tunnel>` dans le navigateur, qui
  affiche bien `Hello Kubernetes bootcamp! | Running on: <nom-du-pod>`.

## Partie 4 — Scale up / down

- Commande utilisée pour vérifier les pods : `kubectl get pods -l app=kubernetes-bootcamp`.
- Après passage à 5 répliques, un rafraîchissement répété (Ctrl+F5) de la
  page fait apparaître des noms de pods différents à chaque requête : le
  Service répartit la charge (**load balancing**) entre toutes les
  répliques actives.
- Après redescente à 2 répliques, les 3 pods en trop passent bien au statut
  `Terminating` puis disparaissent.

## Partie 5 — Rolling update

- Le passage à `jocatalin/kubernetes-bootcamp:v2` a provoqué un
  `CrashLoopBackOff` (image incompatible avec l'architecture ARM64 de la
  machine), empêchant d'observer le comportement normal de mise à jour
  progressive sur cette version précise.
- `kubectl rollout undo deployments/kubernetes-bootcamp` a bien annulé le
  changement et restauré les pods sains sur l'ancienne image (ARM64).
- Le principe du rolling update (transition progressive entre anciens et
  nouveaux pods, visible via le changement de nom de pod dans la réponse
  HTTP) avait déjà été observé lors de la création initiale du deployment.

## Partie 6 — Manifests YAML

- `TO COMPLETE #1` (deployment.yaml, dans `containers`) →
  `image: baniyuga/kubernetes-bootcamp:v1` (version ARM64 de l'image du lab).
- `TO COMPLETE #2` (deployment.yaml, dans `spec`) → `replicas: 1` (puis `3`
  à l'étape 6.6).
- `TO COMPLETE` (service.yaml, `selector.app`) → `kubernetes-bootcamp`
  (doit correspondre au label du pod).
- `TO COMPLETE` (service.yaml, `ports.port`) → `8080` (port d'écoute de
  l'application Node.js).
- Après `kubectl apply -f deployment.yaml`, les pods démarrent bien en
  `Running`.
- Après `kubectl apply -f service.yaml`, le service est accessible via le
  navigateur (tunnel minikube), réponse HTTP confirmée.
- Avec `replicas: 3`, les rafraîchissements répétés du navigateur montrent
  bien des noms de pods différents (`6wtpp`, `q4ztv`, `z65b5`), confirmant
  la répartition de charge entre les 3 répliques.
