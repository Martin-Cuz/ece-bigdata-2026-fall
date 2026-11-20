cd "$(dirname "$0")/../../ece-2026-bigdata"
export PATH="$HOME/.local/bin:$PATH"
export AWS_PROFILE=default
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile default)
export LAB_BUCKET_NAME=$KUBERNETES_NAMESPACE
B="s3://$LAB_BUCKET_NAME"
A="aws --profile default"
step() { echo; echo "### $*"; }

step "s5cmd install"
(
  set -e
  V=$(curl -s https://api.github.com/repos/peak/s5cmd/releases/latest | jq -r '.tag_name | .[1:]')
  mkdir -p ~/.local/bin
  case "$(uname -m)" in x86_64) AR=64bit ;; aarch64|arm64) AR=arm64 ;; esac
  F="s5cmd_${V}_Linux-${AR}.tar.gz"
  U="https://github.com/peak/s5cmd/releases/download/v${V}"
  T=$(mktemp -d); cd "$T"
  curl -fsSLO "$U/$F"; curl -fsSLO "$U/s5cmd_checksums.txt"
  grep " ${F}$" s5cmd_checksums.txt | sha256sum -c
  tar -xf "$F" -C ~/.local/bin s5cmd; chmod +x ~/.local/bin/s5cmd; rm -rf "$T"
)
s5cmd version

step "uv sync and datasets"
uv sync -q
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
ls -l users.csv orders.csv

step "bucket verification"
$A s3 ls | grep "$LAB_BUCKET_NAME"

step "upload (PUT)"
$A s3 cp users.csv "$B/bronze/users.csv"
step "list"
$A s3 ls "$B/bronze/"
s5cmd --endpoint-url "$S3_ENDPOINT_URL" --profile default ls "$B/bronze/"
step "download (GET)"
$A s3 cp "$B/bronze/users.csv" ./users_downloaded.csv
diff users.csv users_downloaded.csv && echo identical
step "move"
$A s3 mv "$B/bronze/users.csv" "$B/bronze/users_renamed.csv"
$A s3 ls "$B/bronze/"
step "delete"
$A s3 rm "$B/bronze/users_renamed.csv"
step "metadata"
$A s3 cp users.csv "$B/bronze/users.csv"
$A s3api head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv
md5sum users.csv
$A s3 cp users.csv "$B/bronze/users.csv" --metadata "source=dataset_users.py,version=1,rows=50"
$A s3api head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv | jq '.Metadata'
step "presigned url"
URL=$($A s3 presign "$B/bronze/users.csv" --expires-in 300)
curl -s "$URL" | head -n 3
step "multipart"
dd if=/dev/urandom of=large_dataset.bin bs=1M count=200 status=none
$A s3 cp large_dataset.bin "$B/large/dataset.bin" --only-show-errors
$A s3api head-object --bucket "$LAB_BUCKET_NAME" --key large/dataset.bin | jq -r '.ETag'
$A s3api list-multipart-uploads --bucket "$LAB_BUCKET_NAME"
step "versioning"
$A s3api put-bucket-versioning --bucket "$LAB_BUCKET_NAME" --versioning-configuration Status=Enabled
$A s3 cp users.csv "$B/bronze/users.csv" --only-show-errors
uv run dataset-users -c 40 -o csv > users_v2.csv
$A s3 cp users_v2.csv "$B/bronze/users.csv" --only-show-errors
$A s3api list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix bronze/users.csv | jq -c '.Versions[] | {VersionId, IsLatest, Size}'
OLD=$($A s3api list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix bronze/users.csv | jq -r '[.Versions[] | select(.IsLatest|not)][0].VersionId')
$A s3api get-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv --version-id "$OLD" users_v1.csv >/dev/null
diff -q users.csv users_v1.csv && echo "version 1 restored"
$A s3 rm "$B/bronze/users.csv"
$A s3 ls "$B/bronze/"
$A s3api list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix bronze/users.csv | jq -c '{versions: (.Versions|length), markers: ((.DeleteMarkers // [])|length)}'
step "lifecycle"
cat > lifecycle.json <<'EOF'
{"Rules":[
{"ID":"expire-large-after-30-days","Status":"Enabled","Filter":{"Prefix":"large/"},"Expiration":{"Days":30}},
{"ID":"delete-noncurrent-versions","Status":"Enabled","Filter":{"Prefix":""},"NoncurrentVersionExpiration":{"NoncurrentDays":7}},
{"ID":"abort-incomplete-multipart","Status":"Enabled","Filter":{"Prefix":""},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}
EOF
$A s3api put-bucket-lifecycle-configuration --bucket "$LAB_BUCKET_NAME" --lifecycle-configuration file://lifecycle.json
$A s3api get-bucket-lifecycle-configuration --bucket "$LAB_BUCKET_NAME" | jq -c '.Rules[].ID'
step "bucket policy"
cat > policy.json <<EOF
{"Version":"2012-10-17","Statement":[{"Sid":"ProtectBronzeLayer","Effect":"Deny","Principal":"*","Action":["s3:DeleteObject"],"Resource":"arn:aws:s3:::$LAB_BUCKET_NAME/bronze/*"}]}
EOF
$A s3 cp users.csv "$B/bronze/protected.csv" --only-show-errors
$A s3api put-bucket-policy --bucket "$LAB_BUCKET_NAME" --policy file://policy.json
$A s3 rm "$B/bronze/protected.csv"
$A s3api delete-bucket-policy --bucket "$LAB_BUCKET_NAME"
step "object acl"
$A s3api get-object-acl --bucket "$LAB_BUCKET_NAME" --key bronze/protected.csv | jq -c '.Grants'
step "consistency"
$A s3 cp users.csv "$B/bronze/users.csv" --only-show-errors
$A s3 cp "$B/bronze/users.csv" /dev/null && echo "immediately available"
step "cleanup"
for prefix in bronze/ large/; do
  $A s3api list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix "$prefix" --output json \
  | jq -c '{Objects: ([.Versions[]?, .DeleteMarkers[]?] | map({Key, VersionId})), Quiet: true}' > delete.json
  if [ "$(jq '.Objects | length' delete.json)" -gt 0 ]; then
    $A s3api delete-objects --bucket "$LAB_BUCKET_NAME" --delete file://delete.json >/dev/null
  fi
done
$A s3api put-bucket-versioning --bucket "$LAB_BUCKET_NAME" --versioning-configuration Status=Suspended
$A s3api delete-bucket-lifecycle --bucket "$LAB_BUCKET_NAME"
rm -f large_dataset.bin users_downloaded.csv users_v1.csv users_v2.csv delete.json lifecycle.json policy.json
$A s3 ls "$B/" --recursive
echo; echo "### done"
