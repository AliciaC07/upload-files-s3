import os
import time
import boto3


def upload_files(path: str, bucket_name: str, aws_access_key_id: str, aws_secret_access_key: str, aws_region: str,
                 folder_to_rename: str = "clientes", ignore_folders: list = None, folder_name_mapping: dict = None):
    # Start the timer
    start_time = time.time()

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    for subdir, dirs, files in os.walk(path):
        # Check if the current directory should be ignored
        if ignore_folders and any(folder in dirs for folder in ignore_folders):
            dirs[:] = [d for d in dirs if d not in ignore_folders]
            continue

        # Check if the current directory should be processed
        if folder_to_rename in subdir:
            for file in files:
                full_path = os.path.join(subdir, file)

                # Calculate the relative path of the file with respect to the specified path
                relative_path = os.path.relpath(full_path, path)

                # Replace the system-specific path separator with '/' for S3
                relative_path = relative_path.replace(os.path.sep, '/')

                # Construct the new key by replacing the folder name if a mapping is provided
                new_key = relative_path.replace(folder_to_rename,
                                                folder_name_mapping.get(folder_to_rename, folder_to_rename))

                # Upload the file to S3 with the new key
                with open(full_path, 'rb') as data:
                    # Ensure data is not None before uploading
                    if data:
                        bucket.put_object(Key=new_key, Body=data)
        else:
            for file in files:
                full_path = os.path.join(subdir, file)

                # Calculate the relative path of the file with respect to the specified path
                relative_path = os.path.relpath(full_path, path)

                # Replace the system-specific path separator with '/' for S3
                relative_path = relative_path.replace(os.path.sep, '/')

                # Upload the file to S3 with the full folder structure as part of the key
                with open(full_path, 'rb') as data:
                    bucket.put_object(Key=relative_path, Body=data)

    # Stop the timer
    end_time = time.time()

    # Calculate and print the elapsed time
    elapsed_time = end_time - start_time
    print(f"Upload completed in {elapsed_time:.2f} seconds.")


# Usage example
if __name__ == "__main__":
    folder_name_mapping = {'1': '30'}
    upload_files(os.environ.get('PATH'), os.environ.get('BUCKET_NAME'),
                 os.environ.get('AWS_ACCESS_KEY'),
                 os.environ.get('AWS_SCT_KEY'),
                 os.environ.get('REGION_NAME'), folder_to_rename='1',
                 ignore_folders=['pagares', '2', 'off'], folder_name_mapping=folder_name_mapping)

