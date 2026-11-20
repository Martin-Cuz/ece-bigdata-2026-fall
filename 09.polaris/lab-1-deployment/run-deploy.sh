D="$(cd "$(dirname "$0")" && pwd)"
cd "$D"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_MGMT_URI="http://polaris-mgmt:8182"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
echo "### bucket"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"
echo "### configmap and secrets"
kubectl create configmap polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --from-literal=POLARIS_URI="$POLARIS_URI" \
  --from-literal=POLARIS_MGMT_URI="$POLARIS_MGMT_URI" \
  --from-literal=POLARIS_REALM="POLARIS" \
  --from-literal=POLARIS_CLIENT_ID="root" \
  --from-literal=POLARIS_BASE_LOCATION="s3://$LAB_BUCKET_NAME/warehouse/" \
  --from-literal=S3_ENDPOINT="https://s3.seaweedfs.adm.adaltas.cloud/" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-credential \
  --from-literal=POLARIS_ROOT_SECRET="s3cr3t" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "### helm"
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
helm dependency update polaris/deployment
helm upgrade --install polaris polaris/deployment --wait --timeout 10m
echo "### health"
curl -s http://polaris-mgmt:8182/q/health | jq -c '.status'
echo "### pods"
kubectl get pods
echo "### credentials"
kubectl logs job/polaris-bootstrap \
  | grep '"credentials"' \
  | jq -r 'select(.principal == "dev" or .principal == "analyst")
      | "POLARIS_\(.principal | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
  > /home/onyxia/work/.polaris-credentials.env
grep -c POLARIS_ /home/onyxia/work/.polaris-credentials.env
echo "### admin api"
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
API="$POLARIS_URI/api/management/v1"
curl -s "$API/catalogs" -H "Authorization: Bearer $TOKEN" | jq -c '.catalogs[] | {type, name, properties}'
curl -s "$API/principals" -H "Authorization: Bearer $TOKEN" | jq -c '[.principals[].name]'
curl -s "$API/principal-roles" -H "Authorization: Bearer $TOKEN" | jq -c '[.roles[].name]'
curl -s "$API/catalogs/$POLARIS_CATALOG/catalog-roles" -H "Authorization: Bearer $TOKEN" | jq -c '[.roles[].name]'
echo "### done"
