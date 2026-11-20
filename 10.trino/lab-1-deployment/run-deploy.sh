D="$(cd "$(dirname "$0")" && pwd)"
cd "$D"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
echo "### bucket"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"
echo "### credentials"
set -a; . /home/onyxia/work/.polaris-credentials.env; set +a
grep -c POLARIS_ /home/onyxia/work/.polaris-credentials.env
echo "### secrets and configmap"
kubectl create secret generic trino-polaris-oauth \
  --from-literal=POLARIS_DEV_CREDENTIAL="$POLARIS_DEV_CREDENTIAL" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trino-polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "### helm"
helm repo add trino https://trinodb.github.io/charts
helm repo update
helm dependency update ./trino/deployment
helm upgrade --install trino ./trino/deployment --wait --timeout 10m
echo "### pods"
kubectl get pods
echo "### verify"
curl -s -i http://trino:8080/v1/info | head -1
curl -s http://trino:8080/v1/info | jq -c '{nodeVersion, starting, coordinator}'
echo "### done"
