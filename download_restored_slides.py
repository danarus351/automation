import json
import os
import boto3
from jira import JIRA as jr
import requests
import concurrent.futures
from botocore.exceptions import ClientError
from tqdm import tqdm
import time


def convert_slides_to_paths(slides):
    fixed_sld = []
    for sld in slides:
        if not(sld == ''):
            machine_id = sld.split('-')[1]
            if int(sld.split('-')[2]) > 1000:
                sub = sld.split('-')[2][0]
            else:
                sub = '0'
            fixed  = f'{machine_id}/{sub}/{sld.replace(" ","")}'
            fixed = fixed.replace(' ','')
            local_path = f'/mnt/tlvdb16/{fixed}/'
            fixed_sld.append((fixed,local_path))
    return fixed_sld


def download_s3_file(s3, destination_path, file_path, destination_file_path, obj,bucket_name='sightdx-tlvdb'):
    try:
        if not(os.path.exists(destination_file_path)):
            s3.download_file(bucket_name, file_path, destination_file_path)
            print(f"Downloaded: {destination_file_path}")
        else:
            raise FileExistsError()

    except FileExistsError:
        if os.path.getsize(destination_file_path) < obj['Size']:
            os.remove(destination_file_path)
            s3.download_file(bucket_name, file_path, destination_file_path)
            print(f"Downloaded: {destination_file_path}")
        else:
            print(f"file already exists {destination_file_path}")
    except ClientError as err:
        print(err)
        if err.response['Error']['Code'] == 'InvalidObjectState':
            print("The operation is not valid for the object's storage class.")
            with open('deep_object','a') as f:
                f.write(f"{obj['Key']}\n")
                f.close()
        else:
            # Handle other botocore ClientErrors
            print(f"An unexpected error occurred: {err}")

    except PermissionError:
        with open('permission_error_file','a') as f:
            f.write(f"{obj['Key']}\n")
            f.close()
    except KeyboardInterrupt:
        print(destination_file_path)

def download_folder_from_s3(folder_path, destination_path, images=True, bucket_name='sightdx-tlvdb'):
    destination_path = os.path.join('/mnt/tlvdb16/', folder_path)
    # destination_path = destination_path.replace('/mnt/tlvdb16/', '/mnt/tlvdb16/clinical_trial/')
    try:
        os.mkdir(destination_path)
    except FileExistsError:
        print(f"folder already exists {destination_path}")

    seassion = boto3.Session(profile_name='snowball_bucket')
    s3 = seassion.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': folder_path}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                if not obj['Key'].endswith('/'): # Skip directories

                    if '.zip' in obj['Key']:
                        file_path = obj['Key']
                        if images:
                            destination_file_path = os.path.join(destination_path, os.path.basename(file_path))
                            download_s3_file(s3=s3, file_path=file_path,
                                             destination_file_path=destination_file_path,obj=obj, destination_path=destination_path)

                    else:
                        file_path = obj['Key']
                        destination_file_path = os.path.join(destination_path, os.path.basename(file_path))
                        download_s3_file(s3=s3, file_path=file_path,
                                         destination_file_path=destination_file_path, obj=obj,destination_path=destination_path)

def proccess_multitheard(chunk, images):

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(download_folder_from_s3, sld[0], sld[1], images): sld for sld in chunk}
        for future in tqdm(futures):
            future.result()
        executor.shutdown(wait=True)
    for future in tqdm(futures):
        future.result()


if __name__ == '__main__':
    with open('<jira api>','r') as f:
        jira_conn = json.load(f)
        f.close()
    token = jira_conn['token']
    email = jira_conn['email']
    url = jira_conn['url']
    jql = 'project = "IS" AND issuetype = "AWS slides request" AND status in ("pending aws unfreeze") order by created DESC'
    conn = jr(url, basic_auth=(email,token))
    queue = conn.search_issues(jql)
    for ticket in queue:
        issue = conn.issue(ticket)
        try:
            if issue.fields.customfield_10266[0].value == 'No':
                images = False
            else:
                images = True
        except:
            images = True

        attachments = issue.fields.attachment
        for attchment in attachments:
            filename = attchment.filename
            attachment_url = attchment.content
            attchment_contecnt = requests.get(attachment_url, auth=(email, token)).content.decode('utf-8')
            attchment_contecnt = attchment_contecnt.replace('\r', '')
            slides = attchment_contecnt.strip(' ').split('\n')
            for idx, sld in enumerate(slides):
                if '\ufeff' in sld:
                    sld = sld.replace('\ufeff', '')
                    slides[idx] = sld

            zipped_paths = convert_slides_to_paths(slides)
            if len(zipped_paths) >= 70 :
                chunk_size = int(len(zipped_paths) / 70)
            else:
                chunk_size = len(zipped_paths)
            chunks = [zipped_paths[i:i + chunk_size] for i in range(0, len(zipped_paths), chunk_size)]

            for sld in zipped_paths:
                download_folder_from_s3(folder_path=sld[0],destination_path=sld[1], images=images)
            # if chunk_size >= 70 :
            #     workers = 78
            # else:
            #     workers = len(chunks)
            # with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            #     # futures = {executor.submit(proccess_multitheard, chunk, images):chunk for chunk in chunks}
            #     futures = {executor.submit(download_folder_from_s3, sld[0], sld[1], images): sld for sld in zipped_paths}