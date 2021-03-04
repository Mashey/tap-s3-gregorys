import boto3
import json
import pandas as pd

from .utils import create_json_schema, get_abs_path


class S3Client:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self._s3 = self._session.resource("s3")

    def get_bucket(self, bucket_name):
        return self._s3.Bucket(bucket_name)

    def get_objects(self, bucket, prefix=""):
        return bucket.objects.filter(Prefix=prefix)

    def filter_objects_by_pattern(self, objects, pattern):
        objs = []
        for object in objects:
            if pattern in object.key:
                objs.append(object)
        return objs

    def get_updated_objects(self, objects, last_modified):
        objs = []
        for object in objects:
            if object.last_modified >= last_modified:
                objs.append(object)
        return objs

    def get_schema(
        self, bucket_name, search_prefix, search_pattern, file_type, delimiter
    ):
        bucket = self.get_bucket(bucket_name)
        objects = self.filter_objects_by_pattern(
            self.get_objects(bucket, search_prefix), search_pattern
        )

        df = None
        json_obj = {}
        if file_type == "csv":
            for obj in objects:
                df = pd.read_csv(
                    obj.get()["Body"], index_col=None, dtype=str
                ).clean_names(remove_special=True)
                for column in df.columns:
                    json_obj[column] = "string"
            return create_json_schema(json_obj)

        if file_type == "json":
            with open(get_abs_path("schemas/menu_export.json")) as f:
                return json.load(f)
