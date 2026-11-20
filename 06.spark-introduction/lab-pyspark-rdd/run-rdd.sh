D="$(cd "$(dirname "$0")" && pwd)"
mkdir -p ~/rdd-work && cd ~/rdd-work
export LAB_BUCKET_NAME=$KUBERNETES_NAMESPACE
aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users.csv --only-show-errors
aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/orders.csv" orders.csv --only-show-errors
ls -la users.csv orders.csv
spark-submit --master 'local[*]' "$D/rdd_lab.py" 2>&1 | grep -v -E '^(WARNING|Setting default log|To adjust logging|Using Spark|[0-9]{2}/[0-9]{2}/[0-9]{2} )'
rm -f users.csv orders.csv
echo "### cleanup done"
