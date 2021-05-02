from boto.s3.connection import S3Connection

conn = S3Connection(AWS_KEY, AWS_SECRET)

bucket = conn.get_bucket(mongodoc)
destination = bucket.new_key()
destination.name = filename
destination.set_contents_from_file(myfile)
destination.make_public()





# To consume less downstream bandwidth, decrease the maximum concurrency
config = TransferConfig(max_concurrency=5)

# Download an S3 object
s3 = boto3.client('s3')
s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME', Config=config)
