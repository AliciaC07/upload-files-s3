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
            full_path = os.path.join(subdir, file)

            # Calculate the relative path of the file with respect to the specified path
            relative_path = os.path.relpath(full_path, path)

            # Replace the system-specific path separator with '/' for S3
            relative_path = relative_path.replace(os.path.sep, '/')

            # Upload the file to S3 with the full folder structure as part of the key
            with open(full_path, 'rb') as data:
                bucket.put_object(Key=relative_path, Body=data)


if __name__ == "__main__":
    upload_files('D:/testing-s3-aws')
