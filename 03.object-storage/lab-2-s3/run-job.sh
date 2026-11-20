cd "$(dirname "$0")/../../ece-2026-bigdata"
export PATH="$HOME/.local/bin:$PATH"
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile default)
export LAB_BUCKET_NAME=$KUBERNETES_NAMESPACE
echo "### can-i"; kubectl auth can-i create jobs; kubectl auth can-i create secrets
uv sync -q
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
ls -lh users.csv orders.csv
echo "### configmaps and secret"
kubectl delete -f ../03.object-storage/lab-2-s3/job-upload-bronze.yaml --ignore-not-found
kubectl delete configmap datasets s3-config --ignore-not-found
kubectl delete secret s3-credentials --ignore-not-found
kubectl create configmap datasets --from-file=users.csv --from-file=orders.csv
kubectl create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$(aws configure get region --profile 'default')" \
  --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME"
aws configure export-credentials --profile 'default' --format env-no-export \
  | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > s3.env
kubectl create secret generic s3-credentials --from-env-file=s3.env
rm s3.env
echo "### job"
kubectl apply -f ../03.object-storage/lab-2-s3/job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=180s
kubectl logs job/upload-bronze
echo "### verify"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
echo "### cleanup k8s"
kubectl delete -f ../03.object-storage/lab-2-s3/job-upload-bronze.yaml
kubectl delete configmap datasets s3-config
kubectl delete secret s3-credentials
echo "### done"
