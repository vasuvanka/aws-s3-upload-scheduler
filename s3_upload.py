import os
import boto3
from botocore.exceptions import ClientError
import logging


class S3Upload(object):
    def __init__(self, path, bucket_name):
        super().__init__()
        self.path = path
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')

    def start(self):
        for p in os.listdir(path=self.path):
            xp = self.path+"/"+p
            if os.path.isdir(xp) and "_backedup_" not in p:
                success = self.read_dir(xp)
                if success:
                    os.rename(xp, self.path + "/_backedup_" + p)
            elif os.path.isfile(xp) and "_backedup_" not in p:
                success = self.create_s3_file(xp, p)
                if success:
                    os.rename(xp, self.path + "/_backedup_" + p)
        return True

    def read_dir(self, path):
        for p in os.listdir(path=path):
            xp = path+"/"+p
            if os.path.isdir(xp) and "_backedup_" not in p:
                success = self.read_dir(xp)
                if success:
                    os.rename(xp, path + "/_backedup_" + p)
            elif os.path.isfile(xp) and "_backedup_" not in p:
                success = self.create_s3_file(xp, p)
                if success:
                    os.rename(xp, path + "/_backedup_" + p)
        return True

    def create_s3_file(self, path, name):
        try:
            with open(path, "rb") as f:
                self.s3.upload_fileobj(f, self.bucket_name, path, ExtraArgs={
                    'Metadata': {'name': name}})
                logging.info("file uploaded with name : {}", format(name))
        except ClientError as e:
            logging.error(e)
            return False
        return True
