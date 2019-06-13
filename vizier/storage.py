import os
import io
import boto3
import json
import base64
from decorator import decorator
from .config import configure


def _create_bucket(bucket_name, **kwargs):
    """ Create an Amazon S3 bucket
    :param bucket_name: Unique string name
    :return: True if bucket is created, else False
    """
    s3_client = _create_s3_client(**kwargs)
    s3_client.create_bucket(Bucket=bucket_name)


def _create_s3_client(**kwargs):
    profile_name = kwargs['configuration']['amt_client_params']['s3_profile_name']
    session = boto3.Session(profile_name=profile_name)
    return session.client(service_name='s3')


@configure
def list_objects(bucket, prefix='', **kwargs):
    s3_client = _create_s3_client(**kwargs)
    continuation_token = None
    while True:
        if continuation_token:
            objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for i in objects.get('Contents', []):
            yield i
        if 'NextContinuationToken' in objects:
            continuation_token = objects['NextContinuationToken']
        else:
            break


def build_object_path(obj_name=None, **kwargs):
    project_name = kwargs['configuration']['experiment_params']['project_name']
    experiment_name = kwargs['configuration']['experiment_params']['experiment_name']
    s3_storage_loc = kwargs['configuration']['experiment_params']['s3_storage_location']
    s3_storage_loc = s3_storage_loc.split('/')
    bucket_name = s3_storage_loc[0]
    base_prefix = '/'.join(s3_storage_loc[1:])
    path_prefix = os.path.join(base_prefix, project_name, experiment_name)
    obj_key = os.path.join(path_prefix, obj_name)
    return bucket_name, path_prefix, obj_key


def _prepare_json_for_s3_upload(obj):
    obj = json.dumps(obj, sort_keys=True, default=str)
    obj = base64.b64encode(obj.encode('ascii'))
    return io.BytesIO(obj)


@configure
def upload_object(obj_name, obj=None, obj_path=None, **kwargs):
    s3_client = _create_s3_client(**kwargs)
    bucket_name, path_prefix, obj_key = build_object_path(obj_name, **kwargs)
    if obj:
        bin_obj_f = _prepare_json_for_s3_upload(obj)
        s3_client.upload_fileobj(bin_obj_f, bucket_name, obj_key)
    elif obj_path:
        obj_fp = os.path.join(obj_path, obj_name)
        s3_client.upload_file(obj_fp, bucket_name, obj_key)
    confirm_upload = list(list_objects(bucket_name, path_prefix, **kwargs))
    return confirm_upload


@configure
def download_object(obj_name, **kwargs):
    s3_client = _create_s3_client(**kwargs)
    bucket_name, path_prefix, obj_key = build_object_path(obj_name, **kwargs)
    obj_resp = s3_client.get_object(Bucket=bucket_name, Key=obj_key)
    bin_obj = obj_resp['Body'].read()
    return json.loads(base64.decodebytes(bin_obj))

