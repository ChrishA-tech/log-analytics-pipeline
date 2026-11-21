# Run on real AWS (keep costs near zero)
1. Create account; set a £5 budget alert first. Use eu-west-2.
2. IAM user (not root) with S3, DynamoDB, Athena, Glue access. `aws configure` locally; never commit keys.
3. In src/common.py remove `endpoint_url` and the test credentials so boto3 uses your profile. Use a globally unique bucket name.
4. Run: ingest.py, etl.py (reads S3), dynamo_serving.py (on-demand table). Record real timings.
5. Athena: create an external table over s3://<bucket>/curated/requests/ and run the 5 queries. Redshift Serverless only if credits cover it.
6. Delete everything: bucket contents + bucket, DynamoDB table, Athena/Glue tables, any Redshift workgroup.
Then replace the emulated timings in the README and CV with the real AWS numbers.
