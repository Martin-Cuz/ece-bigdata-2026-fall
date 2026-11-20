D="$(cd "$(dirname "$0")" && pwd)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export AWS_REGION="us-east-1"
B="s3://$LAB_BUCKET_NAME"
API="$POLARIS_URI/api/management/v1"
set -a; . /home/onyxia/work/.polaris-credentials.env; set +a
SPARK="spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

run() {
  $SPARK "$D/polaris_lab.py" "$1" > ~/work/polaris_$1.log 2>&1
  grep -v -E '^(WARNING|Setting default log|To adjust logging|Using Spark|[0-9]{2}/[0-9]{2}/[0-9]{2} )|^:: |^downloading|^Ivy|^The jars|^\s+at |^\s*$|^\+' ~/work/polaris_$1.log | cut -c1-330
}

token() {
  curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials -d client_id=root \
    -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token
}

if [ -z "$ONLY_EX2" ]; then
echo "##### prepare"
run prep

VERSION=$(aws s3 --profile default cp "$B/warehouse/silver/orders/metadata/version-hint.text" - | tr -d '\r\n')
echo "### version-hint: $VERSION"
aws s3 --profile default ls "$B/warehouse/silver/orders/metadata/" | grep metadata.json | awk '{print $3, $4}'
export METADATA_FILE="s3a://$LAB_BUCKET_NAME/warehouse/silver/orders/metadata/v$VERSION.metadata.json"
echo "### objects before registration: $(aws s3 --profile default ls "$B/warehouse/silver/orders/" --recursive | wc -l)"

echo "##### dev part 1"
run dev1
echo "### objects after registration and creation of users"
aws s3 --profile default ls "$B/warehouse/silver/" --recursive | awk '{print $3, $4}'

echo "##### analyst"
run analyst

echo "##### dev part 2"
run dev2

fi

if [ -n "$ONLY_EX2" ]; then
  run gold
fi

echo "##### exercise 2: silver_reader"
TOKEN=$(token)
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
curl -s -X POST "${H[@]}" "$API/catalogs/$POLARIS_CATALOG/catalog-roles" \
  -d '{"catalogRole":{"name":"silver_reader","properties":{}}}' -o /dev/null -w "create catalog role: %{http_code}\n"
for p in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  curl -s -X PUT "${H[@]}" "$API/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" \
    -d "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$p\"}}" -o /dev/null -w "grant $p on silver: %{http_code}\n"
done
curl -s -X POST "${H[@]}" "$API/principal-roles" -d '{"principalRole":{"name":"silver_reader_role","properties":{}}}' \
  -o /dev/null -w "create principal role: %{http_code}\n"
curl -s -X PUT "${H[@]}" "$API/principal-roles/silver_reader_role/catalog-roles/$POLARIS_CATALOG" \
  -d '{"catalogRole":{"name":"silver_reader"}}' -o /dev/null -w "assign catalog role: %{http_code}\n"
RESPONSE=$(curl -s -X POST "${H[@]}" "$API/principals" -d '{"principal":{"name":"reader","properties":{}},"credentialRotationRequired":false}')
echo "$RESPONSE" | jq -r '"POLARIS_READER_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' >> /home/onyxia/work/.polaris-credentials.env
echo "create principal reader: $(echo "$RESPONSE" | jq -r '.principal.name')"
curl -s -X PUT "${H[@]}" "$API/principals/reader/principal-roles" -d '{"principalRole":{"name":"silver_reader_role"}}' \
  -o /dev/null -w "assign principal role: %{http_code}\n"
set -a; . /home/onyxia/work/.polaris-credentials.env; set +a
run reader

echo "##### cleanup"
run cleanup
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" "$API/principals/reader" -o /dev/null -w "delete principal reader: %{http_code}\n"
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" "$API/principal-roles/silver_reader_role" -o /dev/null -w "delete principal role: %{http_code}\n"
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" "$API/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader" -o /dev/null -w "delete catalog role: %{http_code}\n"
grep -v POLARIS_READER_CREDENTIAL /home/onyxia/work/.polaris-credentials.env > /tmp/cred.env && cp /tmp/cred.env /home/onyxia/work/.polaris-credentials.env
echo "### done"
