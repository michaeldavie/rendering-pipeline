import boto3, zipfile


def download_blender_file_from_s3(uri):
    """Downloads the blend file from S3 and stores it locally.

    Keyword arguments:
    uri -- S3 URI of the file to download
    """

    uri_components = uri.split("s3://")[1].split("/")
    bucket = uri_components[0]
    file = uri_components[1]

    # Copy input file from S3 to EFS
    s3 = boto3.resource("s3")
    s3.meta.client.download_file(bucket, file, "/mnt/data/{}".format(file))

    # Check if the file is a zip archive and extract the files if it is
    if zipfile.is_zipfile("/mnt/data/{}".format(file)):
        with zipfile.ZipFile("/mnt/data/{}".format(file), "r") as zip_ref:
            zip_ref.extractall("/mnt/data/")

            for f in zip_ref.namelist():
                if f.endswith(".blend"):
                    file = f
                    break

    return file


def lambda_handler(event, context):
    # Download the zip file from s3 and extract it to EFS
    blend_file = download_blender_file_from_s3(event["inputUri"])
    return {"statusCode": 200, "body": blend_file}
