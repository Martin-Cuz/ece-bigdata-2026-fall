D="$(cd "$(dirname "$0")" && pwd)"
export LAB_BUCKET_NAME=$KUBERNETES_NAMESPACE
B="s3://$LAB_BUCKET_NAME"
spark-submit "$D/iceberg_lab.py" > ~/work/iceberg_spark.log 2>&1
grep -v -E '^(WARNING|Setting default log|To adjust logging|Using Spark|[0-9]{2}/[0-9]{2}/[0-9]{2} )|^:: |^downloading|^Ivy|^The jars' ~/work/iceberg_spark.log
echo "### orders layout"
aws s3 --profile default ls "$B/warehouse/silver/orders/" --recursive | awk '{print $3, $4}'
echo "### users layout"
aws s3 --profile default ls "$B/warehouse/silver/users/" --recursive | awk '{print $3, $4}'
echo "### parquet directory layout"
aws s3 --profile default ls "$B/silver/spark/orders.parquet/" --recursive | awk '{print $3, $4}'
echo "### cleanup"
aws s3 --profile default rm "$B/warehouse/" --recursive --only-show-errors
aws s3 --profile default ls "$B/" --recursive | awk '{print $3, $4}'
echo "### done"
