import boto3
import os


def upload_files(path):
    # Create a Boto3 session with AWS credentials and region information
    session = boto3.Session(
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('AWS_SCT_KEY'),
        region_name=os.environ.get('REGION_NAME')
    )

    # Create an S3 resource using the session
    s3 = session.resource('s3')

    # Specify the S3 bucket to which files will be uploaded
    bucket = s3.Bucket('testing-upload-py')

    # Walk through the local directory and its subdirectories
    for subdir, dirs, files in os.walk(path):
        for file in files:
            # Construct the full local path of the file
            full_path = os.path.join(subdir, file)

            # Open the file in binary mode and upload it to the S3 bucket
            with open(full_path, 'rb') as data:
                # Upload the file to S3 with a key (object name) relative to the specified path
                bucket.put_object(Key=full_path[len(path) + 1:], Body=data)


if __name__ == "__main__":
    upload_files('D:/testing-s3-aws')
