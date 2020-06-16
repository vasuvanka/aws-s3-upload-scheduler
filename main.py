from s3_upload import S3Upload

bucket_name = "bucket name"
path = "directorypath"
my_s3 = S3Upload(path=path, bucket_name=bucket_name)
my_s3.start()
