import os
import time
import boto3


def upload_files(path: str, bucket_name: str, aws_access_key_id: str, aws_secret_access_key: str, aws_region: str,
                 new_folder_name: str,
                 folder_to_rename: list = None, ignore_folders: list = None, folder_name_mapping: dict = None):
    # Start the timer
    start_time = time.time()

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    iterated_paths = set()

    for subdir, dirs, files in os.walk(path):
        # Check if the current directory should be ignored
        if ignore_folders and any(folder in dirs for folder in ignore_folders):
            dirs[:] = [d for d in dirs if d not in ignore_folders]
            continue

        if all(folders in dirs for folders in
               folder_to_rename) and 'clientes' in subdir and subdir not in iterated_paths:
            for sdr, direct, filesUp in os.walk(subdir):
                folder_number = folder_clean_str(sdr)
                # Check if the current directory should be processed
                for rename in folder_to_rename:
                    if rename in folder_number:
                        for file in filesUp:
                            full_path = os.path.join(sdr, file)

                            # Calculate the relative path of the file with respect to the specified path
                            relative_path = os.path.relpath(full_path, path)

                            # Replace the system-specific path separator with '/' for S3
                            relative_path = relative_path.replace(os.path.sep, '/')

                            # Construct the new key by replacing the folder name if a mapping is provided
                            built_path = replace_value_after_clients(relative_path, rename,
                                                                     folder_name_mapping[rename])
                            new_key = built_path

                            if new_folder_name:
                                new_key = os.path.join(new_folder_name, new_key)

                            # Upload the file to S3 with the new key
                            with open(full_path, 'rb') as data:
                                # Ensure data is not None before uploading
                                if data:
                                    bucket.put_object(Key=new_key, Body=data)
                iterated_paths.add(sdr)

        elif 'clientes' not in subdir and subdir not in iterated_paths:
            for file in files:
                full_path = os.path.join(subdir, file)

                # Calculate the relative path of the file with respect to the specified path
                relative_path = os.path.relpath(full_path, path)

                # Replace the system-specific path separator with '/' for S3
                relative_path = relative_path.replace(os.path.sep, '/')

                if new_folder_name:
                    relative_path = os.path.join(new_folder_name, relative_path)

                # Upload the file to S3 with the full folder structure as part of the key
                with open(full_path, 'rb') as data:
                    bucket.put_object(Key=relative_path, Body=data)

    # Stop the timer
    end_time = time.time()

    # Calculate and print the elapsed time
    elapsed_time = end_time - start_time
    print(f"Upload completed in {elapsed_time:.2f} seconds.")


def replace_value_after_clients(path, old_value, new_value):
    # Split the path based on '/'
    path_parts = path.split('/')

    # Find the index of 'clientes' in the path_parts
    try:
        clients_index = path_parts.index('clientes')
    except ValueError:
        # 'clientes' not found in the path, return the original path
        return path

    # Check if the path has more elements after 'clientes'
    if clients_index + 2 < len(path_parts):
        # Replace the value after 'clientes'
        path_parts[clients_index + 1] = path_parts[clients_index + 1].replace(old_value, new_value)

    # Join the path back together
    new_path = '/'.join(path_parts)

    return new_path


def folder_clean_str(path_folder):
    relative_path = path_folder.replace(os.path.sep, '/')
    path_parts = relative_path.split('/')
    try:
        clients_index = path_parts.index('clientes')
    except ValueError:
        # 'clientes' not found in the path, return the original path
        return path_folder
    if clients_index + 1 < len(path_parts):
        folder_name = path_parts[clients_index + 1]
        return folder_name
    else:
        return path_folder


# Usage example
if __name__ == "__main__":
    folder_name_mapping = {'1': '31', '2': '32'}
    upload_files(os.environ.get('PATH'), os.environ.get('BUCKET'),
                 os.environ.get('AWS_ACCESS_KEY'),
                 os.environ.get('AWS_SCT_KEY'),
                 os.environ.get('REGION_NAME'), 'prestamosramos/', folder_to_rename=['1', '2'],
                 ignore_folders=['pagares'], folder_name_mapping=folder_name_mapping)
