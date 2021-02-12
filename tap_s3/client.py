import boto3
import json
import pandas as pd

from .utils import clean_dataframe, create_json_schema, get_abs_path


class S3Client:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key)
        self._s3 = self._session.resource('s3')

    def get_bucket(self, bucket_name):
        return self._s3.Bucket(bucket_name)


    def get_objects(self, bucket, prefix=''):
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


    def get_schema(self, bucket_name, search_prefix, search_pattern, file_type, delimiter):
        bucket  = self.get_bucket(bucket_name)
        objects = self.filter_objects_by_pattern(
            self.get_objects(bucket, search_prefix),
            search_pattern)
        
        df = None
        if file_type == 'csv':
            df = self.read_csv_objects(objects)

        if file_type == 'json':
            with open(get_abs_path('schemas/menu_export.json')) as f:
                return json.load(f)


        # build the most complete json object as possible
        # row data doesn't matter, just types
        json_obj = {}
        for column, column_vals in df.iteritems():
            json_obj[column] = None
            value = ''
            valid_value_index = column_vals.first_valid_index()
            if valid_value_index is not None:
                value = column_vals[valid_value_index]
            json_obj[column] = value
        
        return create_json_schema(json_obj)

    
    def read_csv_objects(self, objects):
        dfs = []
        for obj in objects:
            dfs.append(pd.read_csv(obj.get()['Body'], index_col=None, dtype=str).clean_names(remove_special=True))
        if len(dfs) <= 1:
            try:
                return clean_dataframe(dfs[0])
            except IndexError:
                return pd.DataFrame()
        else:
            return clean_dataframe(pd.concat(dfs, ignore_index=True))

        
    def read_json_objects(self, objects):
        json_objs = []
        for obj in objects:
            raw_data = json.loads(obj.get()['Body'].read())
            menus = raw_data['menus']
            json_objs.extend(menus)
        return json_objs

