import boto3
from tqdm import tqdm

def s3_sess_maker():
    ses = boto3.Session(profile_name='default profile name')
    return ses.client('s3')



def move_to_backup(s3, filename):
    bucket_name = 'bucket name'
    s3.copy_object(
        Bucket='<backet name>',
        CopySource={'Bucket': bucket_name, 'Key': filename},
        Key=filename.replace('devicelogs/','logs_large_files_backup/')
    )
    s3.delete_object(Bucket='bucket name',Key=filename)
    return
#
def list_files_in_bucket(s3,bucket_name= 'bucket name',prefix='devicelogs/'):
    large_files =  []

    # Initial call to list_objects_v2
    response = s3.list_objects_v2(Bucket=bucket_name,Prefix=prefix)

    # Iterate over objects
    while 'Contents' in response:
        # Process objects
        for obj in response['Contents']:
            if obj['Size'] > 15728640:
                large_files.append(obj['Key'])

        # Check if there are more objects to fetch
        if response['IsTruncated']:
            continuation_token = response['NextContinuationToken']
            response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
        else:
            break
    return large_files
# Usage: Provide your bucket name

def spliting_to_files(s3,files):
    chunk_size = 15 * 1024 * 1024
    for file in tqdm(files):
        original = s3.get_object(Bucket='bucket name', Key=file)
        content = original['Body'].read().decode('utf-8')
        print('read original file')
        chunks = [content[i:i + chunk_size] for i in range(0, len(content), chunk_size)]
        print('file devided to chunks')
        for i, chunk in enumerate(chunks):
            chunk_file_name = file.replace('devicelogs/', '')
            chunk_file_name = f'splited-part{i + 1}_{chunk_file_name}'
            s3.put_object(Body=chunk, Bucket= 'bucket name', Key=f'devicelogs/{chunk_file_name}')
            print(f'Saved {chunk_file_name}')
        move_to_backup(filename=file, s3=s3)

    return


if __name__=='__main__':
    s3_ses = s3_sess_maker()
    file_list = list_files_in_bucket(s3=s3_ses)
    for key in tqdm(file_list):
        spliting_to_files(s3=s3_ses,file=key)
