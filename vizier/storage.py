import os
import io
import base64
import json
import boto3
from pprint import pprint
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
            objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix,
                                                ContinuationToken=continuation_token)
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
    batch_id = kwargs['configuration']['experiment_params']['batch_id']
    s3_storage_loc = kwargs['configuration']['experiment_params']['s3_storage_location']
    s3_storage_loc = s3_storage_loc.split('/')
    bucket_name = s3_storage_loc[0]
    base_prefix = '/'.join(s3_storage_loc[1:])
    path_prefix = os.path.join(base_prefix, project_name, batch_id)
    obj_key = os.path.join(path_prefix, obj_name)
    return bucket_name, path_prefix, obj_key


def _prepare_json_for_s3_upload(obj):
    obj = json.dumps(obj, sort_keys=True, default=str)
    obj = base64.b64encode(obj.encode('ascii'))
    return io.BytesIO(obj)


@configure
def list_working_folder(display_meta_data=False, **kwargs):
    bucket_name, path_prefix, obj_key = build_object_path('', **kwargs)
    working_folder = list(list_objects(bucket_name, path_prefix, **kwargs))
    working_folder.sort(key=lambda x: x['LastModified'])
    for resp in working_folder:
        del(resp['ETag'], resp['StorageClass'])
    if display_meta_data:
        for resp in working_folder:
            pprint(resp)
            print()
    else:
        for resp in working_folder:
            print(os.path.split(resp['Key'])[-1])


@configure
def upload_object(obj_fp, obj=None, **kwargs):
    s3_client = _create_s3_client(**kwargs)
    obj_name = os.path.split(obj_fp)[-1]
    bucket_name, path_prefix, obj_key = build_object_path(obj_name, **kwargs)
    if obj:
        bin_obj_f = _prepare_json_for_s3_upload(obj)
        resp = s3_client.upload_fileobj(bin_obj_f, bucket_name, obj_key)
    else:
        resp = s3_client.upload_file(obj_fp, bucket_name, obj_key)
    # confirm_upload = list(list_objects(bucket_name, path_prefix, **kwargs))
    # return confirm_upload


@configure
def download_object(obj_name, **kwargs):
    s3_client = _create_s3_client(**kwargs)
    bucket_name, _, obj_key = build_object_path(obj_name, **kwargs)
    obj_resp = s3_client.get_object(Bucket=bucket_name, Key=obj_key)
    bin_obj = obj_resp['Body'].read()
    return json.loads(base64.decodebytes(bin_obj))
