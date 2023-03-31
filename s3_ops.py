import json

from botocore.exceptions import ClientError
import boto3

s3 = boto3.resource("s3")
s3c = boto3.client("s3")
LOCATION_CONSTRAINT = "eu-central-1"


def upload_to_s3(bucket_name: str, file_path: str, key: str):
    s3c.upload_file(file_path, bucket_name, key)


def overwrite_json_file_array(bucket_name, file_name, data) -> list:
    ob = s3.Object(bucket_name, file_name)  # type: ignore
    ob.put(Body=(bytes(json.dumps(data).encode("UTF-8"))))
    return get_contents_of_json_file_array(bucket_name, file_name)


def add_to_json_file_array(bucket_name, file_name, new_data) -> list:
    data = get_contents_of_json_file_array(bucket_name, file_name)
    data.append(new_data)
    return overwrite_json_file_array(bucket_name, file_name, data)


def create_bucket(bucket_name):
    s3.create_bucket(  # type: ignore
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": LOCATION_CONSTRAINT},
    )


def delete_item(bucket_name, file_name):
    return s3c.delete_object(
        Bucket=bucket_name,
        Key=file_name,
    )


def get_contents_of_json_file_array(
    bucket_name, file_name, create_if_not_exists: bool = True
) -> list:
    ob = s3.Object(bucket_name, file_name)  # type: ignore
    try:
        return json.loads(ob.get()["Body"].read().decode("utf-8"))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            if create_if_not_exists:
                ob.put(Body=(bytes(json.dumps([]).encode("UTF-8"))))
                return get_contents_of_json_file_array(bucket_name, file_name, False)
        raise


def list_files(bucket_name):
    bucket = s3.Bucket(bucket_name)  # type: ignore
    return [ob.key for ob in bucket.objects.all()]
