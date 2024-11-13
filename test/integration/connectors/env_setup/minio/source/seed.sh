#!/usr/bin/env sh

set -e

echo URL: "${S3_ENDPOINT_URL}"
echo Credentials: "${S3_ACCESS_KEY}":"${S3_SECRET_KEY}"
/usr/bin/mc alias set s3 "${S3_ENDPOINT_URL}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}"
/usr/bin/mc mb s3/"${S3_BUCKET_NAME}" --ignore-existing
/usr/bin/mc anonymous set public s3/"${S3_BUCKET_NAME}"
mc cp /init/wiki_movie_plots_small.csv s3/"${S3_BUCKET_NAME}"/wiki_movie_plots_small.csv
