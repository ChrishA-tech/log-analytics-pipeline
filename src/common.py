import boto3, json, os, time
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:5000")
BUCKET = "log-lake"
def client(svc):
    return boto3.client(svc, endpoint_url=S3_ENDPOINT, region_name="eu-west-2", aws_access_key_id="test", aws_secret_access_key="test")
def resource(svc):
    return boto3.resource(svc, endpoint_url=S3_ENDPOINT, region_name="eu-west-2", aws_access_key_id="test", aws_secret_access_key="test")
class Timer:
    def __enter__(self): self.t = time.perf_counter(); return self
    def __exit__(self, *a): self.s = time.perf_counter() - self.t
