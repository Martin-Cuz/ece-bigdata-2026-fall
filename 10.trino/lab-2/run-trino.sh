D="$(cd "$(dirname "$0")" && pwd)"
export PATH="$HOME/.local/bin:$PATH"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export AWS_REGION="us-east-1"
B="s3://$LAB_BUCKET_NAME"
set -a; . /home/onyxia/work/.polaris-credentials.env; set +a
SPARK="spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

echo "##### silver tables registered in polaris"
if ! aws s3 --profile default ls "$B/warehouse/silver/users/metadata/" > /dev/null 2>&1; then
  $SPARK "$D/../../09.polaris/lab-2/polaris_lab.py" prep > ~/work/trino_prep.log 2>&1
fi
VERSION=$(aws s3 --profile default cp "$B/warehouse/silver/users/metadata/version-hint.text" - | tr -d '\r\n')
export USERS_METADATA_FILE="s3a://$LAB_BUCKET_NAME/warehouse/silver/users/metadata/v$VERSION.metadata.json"
$SPARK "$D/../../09.polaris/lab-2/polaris_lab.py" restore_users > ~/work/trino_restore.log 2>&1
sed -n '/### register/,$p' ~/work/trino_restore.log | tr -s ' ' | grep -v -E '^\|[-+]|^\+|^$|^\s+at ' | cut -c1-200

echo "##### trino labs 2 and 3"
export TRINO_HOST=trino
export TRINO_PORT=8080
export TRINO_USER=$KUBERNETES_NAMESPACE
cd "$D/../../ece-2026-bigdata"
uv sync -q
uv run python "$D/trino_lab.py" > ~/work/trino_lab.log 2>&1
cat ~/work/trino_lab.log

echo "##### spark comparison"
$SPARK "$D/spark_compare.py" > ~/work/trino_spark.log 2>&1
sed -n '/### spark/,$p' ~/work/trino_spark.log | tr -s ' ' | grep -v -E '^\|[-+]|^\+|^$|^\s+at ' | cut -c1-200

echo "##### gold layout in the bucket"
aws s3 --profile default ls "$B/warehouse/gold/" --recursive | awk '{print $3, $4}' | grep -v '/data/' | head -60
aws s3 --profile default ls "$B/warehouse/gold/" | awk '{print $2}'
echo "### done"
