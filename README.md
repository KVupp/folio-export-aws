# folio-export-aws

The App.java is the main class to run the aws interaction with S3. 
There are no credentials provided, In order for it to work, the credentials will be looked up in default credential provider chain. 
More Info: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html



MINIO setup:
docker pull minio/minio
docker run -p 9001:9000 minio/minio server /data

