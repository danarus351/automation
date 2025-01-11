from __future__ import print_function

import os.path
from pyairtable import Table

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import io
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import socket
from tqdm import tqdm


# If modifying these scopes, delete the file token.json.

def report_builder(airtable_obj, export_status='Successful', file_name='N\A', file_category='N\A'):
    global report
    value_list = []
    values = ["doc_id", "document_name", "revision", "status", 'DMR']
    for value in values:
        if value in airtable_obj:
            value_list.append(airtable_obj[value])
        else:
            value_list.append('N/A')
    report.append(value_list + [file_name, export_status, file_category])


def get_parent(service, file_real_id):
    parent = service.files().get(fileId=file_real_id, fields='parents').execute()
    return parent['parents'][0]


def download_multiple_file(items, service, creds, airtable_obj):
    global report
    try:
        pdf = False
        no_approved = False
        for item in items:
            item_name = item['name'].lower()
            item_name = item_name.strip(' ')

            if item_name == 'approved':
                no_approved = True
                files = service.files().list(q="'{}' in parents".format(item['id']),
                                             pageSize=10).execute()
                # elif current_folder_name == 'A'
                files = files.get('files', [])

                for file in files:
                    if str(file['name']).endswith('.pdf'):
                        pdf = True
                    download_file(airtable_obj=airtable_obj, real_file_id=file['id'], file_name=file['name'],
                                  creds=creds, save_path=save_path, mimetype=file['mimeType'])
        if no_approved == False:
            report_builder(airtable_obj=airtable_obj, export_status='NO Approved')
            if pdf == False:
                #  report.append([airtable_name, '', 'NO PDF'])
                report_builder(airtable_obj=airtable_obj, export_status='NO PDF')
    except HttpError as error:
        if error.reason == 'This file is too large to be exported.':
            # report.append([airtable_name, '', 'File too big'])
            report_builder(airtable_obj=airtable_obj, export_status='File too big')
            # export_links(real_file_id=file['id'], service=service, file_name=file['name'], airtable_name=airtable_name)
        # TODO(developer) - Handle errors from drive API.
        else:
            print(f'An error occurred: {error}')
            # report[airtable_name] = 'not available'
            # report.append([airtable_name, '', 'not available'])
            report_builder(airtable_obj=airtable_obj, export_status=error)


def find_header_airtable(table, view_name):
    headers_list = {}
    for record in table.all(view=view_name):
        record = (record['fields']).keys()
        location = False
        doc_name = False
        for header in record:
            if 'location in drive' in header.lower().strip(' .') or 'location in the drive' in header.lower().strip(
                    ' .'):
                headers_list['location in the drive'] = header
                location = True
            if (header.lower().strip(' .')) == 'document name':
                headers_list['document name'] = header
                doc_name = True
            if location and doc_name:
                return headers_list


def pull_airtable(base_id, table_name, view_name, save_path):
    global report
    doc_table = {}
    with open('<Path to Airtable api key>', 'r') as f:
        api_key = f.read().strip('\n')
        f.close()
    table = Table(api_key, base_id, table_name)
    table.all()
    headers = find_header_airtable(table, view_name)
    try:
        drive_location_header = headers['location in the drive']
        doc_name_header = headers['document name']
    except:
        drive_location_header = 'location in the drive'
        doc_name_header = 'document name'

    for doc in table.all(view=view_name):
        doc_id = doc['fields']['Doc ID']

        if 'Status' in doc['fields']:
            if doc['fields']['Status'] == 'Approved':
                airtable_doc_name = str(doc['fields'][doc_name_header])
                if drive_location_header in doc['fields']:
                    location_in_drive = str(doc['fields'][drive_location_header])
                    location_in_drive = location_in_drive.split('/')
                    if not ('airtable.com' in location_in_drive):
                        if len(location_in_drive) == 7:
                            location_in_drive = location_in_drive[len(location_in_drive) - 2]
                        elif len(location_in_drive) == 4:
                            location_in_drive = location_in_drive[len(location_in_drive) - 1]
                            location_in_drive = (location_in_drive.split('='))[1]
                        else:
                            location_in_drive = location_in_drive[len(location_in_drive) - 1]
                            location_in_drive = location_in_drive.split('?')
                            location_in_drive = location_in_drive[0]
                        if 'Revision' in doc['fields']:
                            rev = doc['fields']['Revision']
                        else:
                            rev = ''
                        if 'Status' in doc['fields']:
                            stat = doc['fields']['Status']
                        else:
                            stat = ''
                        if 'DMR' in doc['fields']:
                            dmr = doc['fields']['DMR']
                        else:
                            dmr = ''
                        doc_table[doc_id] = {"doc_id": doc_id,
                                             "location_in_drive": location_in_drive,
                                             "document_name": airtable_doc_name,
                                             "revision": rev,
                                             "status": stat,
                                             'DMR': dmr
                                             }

                    else:
                        # report.append([doc_name, '', 'airtable link'])
                        airtable_obj = {'doc_id': doc['fields']['Doc ID']}
                        if 'Doc ID' in doc['fields']:
                            airtable_obj['status'] = doc['fields']['Doc ID']
                        if doc_name_header in doc['fields']:
                            airtable_obj['document_name'] = doc['fields'][doc_name_header]
                        if 'Status' in doc['fields']:
                            airtable_obj['status'] = doc['fields']['Status']
                        if 'Revision' in doc['fields']:
                            airtable_obj['revision'] = doc['fields']['Revision']
                        if 'DMR' in doc['fields']:
                            airtable_obj['DMR'] = doc['fields']['DMR']
                        report_builder(airtable_obj=airtable_obj, export_status='airtable link')

                else:
                    airtable_obj = {'doc_id': doc['fields']['Doc ID']}
                    if doc_name_header in doc['fields']:
                        airtable_obj['document_name'] = doc['fields'][doc_name_header]
                    if 'Status' in doc['fields']:
                        airtable_obj['status'] = doc['fields']['Status']
                    if 'Revision' in doc['fields']:
                        airtable_obj['revision'] = doc['fields']['Revision']
                    if 'DMR' in doc['fields']:
                        airtable_obj['DMR'] = doc['fields']['DMR']
                    report_builder(airtable_obj=airtable_obj, export_status='no link')
            else:
                airtable_obj = {'doc_id': doc['fields']['Doc ID']}
                if doc_name_header in doc['fields']:
                    airtable_obj['document_name'] = doc['fields'][doc_name_header]
                if 'Status' in doc['fields']:
                    airtable_obj['status'] = doc['fields']['Status']
                if 'Revision' in doc['fields']:
                    airtable_obj['revision'] = doc['fields']['Revision']
                if 'DMR' in doc['fields']:
                    airtable_obj['DMR'] = doc['fields']['DMR']
                report_builder(airtable_obj=airtable_obj,
                               export_status='record is in status {}'.format(doc['fields']['Status']))

        else:
            airtable_obj = {'doc_id': doc['fields']['Doc ID']}
            if doc_name_header in doc['fields']:
                airtable_obj['document_name'] = doc['fields'][doc_name_header]
            if 'Status' in doc['fields']:
                airtable_obj['status'] = doc['fields']['Status']
            if 'Revision' in doc['fields']:
                airtable_obj['revision'] = doc['fields']['Revision']
            if 'DMR' in doc['fields']:
                airtable_obj['DMR'] = doc['fields']['DMR']
            report_builder(airtable_obj=airtable_obj, export_status='record as no status')

    return doc_table


def export_to_excel():
    global report
    df_report = pd.DataFrame()
    df_report = pd.DataFrame(report,
                             columns=["doc_id", "document_name", "revision", "status", 'DMR', "file_name", "export_status",
                                      "file_category", ])
    df_report['File edition'] = 1
    df_report.to_excel(excel_writer=os.path.join(save_path, 'export_report.xlsx'), index=False)


def downloader_func(request):
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

    return file


def download_file(real_file_id, file_name, creds, save_path,
                  airtable_obj, mimetype='application/vnd.google-apps.document'):
    global report
    file_category = 'Source File'
    service = build('drive', 'v3', credentials=creds)
    mimtype_list = ['application/pdf', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'application/vnd.openxmlformats-officedocument.presentationml.presentation']
    if mimetype in mimtype_list or file_name.endswith('.pdf') or file_name.endswith(
            '.docm') or mimetype == 'application/pdf':
        if file_name.endswith('.pdf'):
            file_category = 'Quality Procedure'
        request = service.files().get_media(fileId=real_file_id)
        file = downloader_func(request)
        file.seek(0)
        file_name = file_name.replace('/', "_")
        report_builder(airtable_obj, file_name=file_name, file_category=file_category)
        # print(os.path.join(save_path, file_name))
        with open(os.path.join(save_path, file_name), 'wb') as f:
            f.write(file.read())
            f.close()

    elif mimetype == 'application/vnd.google-apps.form':
        report_builder(airtable_obj=airtable_obj, file_name=file_name, export_status='Form no download',
                       file_category=file_category)


    elif mimetype == 'application/vnd.google-apps.spreadsheet':
        mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        request = service.files().export_media(fileId=real_file_id, mimeType=mimetype)
        file = downloader_func(request)
        file.seek(0)
        file_name = file_name.replace('/', "_")
        # report.append([airtable_name, f'{file_name}.xlsx','', airtable_title, file_category])
        report_builder(airtable_obj=airtable_obj, file_name=f'{file_name}.xlsx', export_status='Successful',
                       file_category=file_category)

        with open(os.path.join(save_path, f'{file_name}.xlsx'), 'wb') as f:
            f.write(file.read())
            f.close()
    elif mimetype == 'application/vnd.google-apps.shortcut':
        results = service.files().get(fileId=real_file_id, fields='shortcutDetails').execute()
        real_file_id = results['shortcutDetails']["targetId"]
        mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        request = service.files().export_media(fileId=real_file_id, mimeType=mimetype)
        file = downloader_func(request)
        file.seek(0)
        file_name = file_name.replace('/', "_")
        # report.append([airtable_name, f'{file_name}.xlsx', '', airtable_title, file_category])
        report_builder(airtable_obj=airtable_obj, file_name=f'{file_name}.xlsx', file_category=file_category)

        with open(os.path.join(save_path, f'{file_name}.xlsx'), 'wb') as f:
            f.write(file.read())
            f.close()

    elif mimetype == 'application/vnd.google-apps.folder':
        results = service.files().list(q="'{}' in parents".format(real_file_id),
                                       pageSize=10, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        download_multiple_file(service=service, creds=creds, items=items, airtable_obj=airtable_obj)

    elif mimetype == 'application/vnd.google-apps.presentation':
        mimetype = 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
        request = service.files().export_media(fileId=real_file_id, mimeType=mimetype)
        file = downloader_func(request)
        file.seek(0)
        file_name = file_name.replace('/', "_")
        # report.append([airtable_name, f'{file_name}.pptx', '', airtable_title, file_category])
        report_builder(airtable_obj=airtable_obj, file_name=f'{file_name}.pptx', file_category=file_category)

        with open(os.path.join(save_path, f'{file_name}.pptx'), 'wb') as f:
            f.write(file.read())
            f.close()
    elif file_name.endswith('.docx'):
        try:
            request = service.files().get_media(fileId=real_file_id)
            file = downloader_func(request)
            file.seek(0)
            file_name = file_name.replace('/', "_")
            # report.append([airtable_name, file_name,'', airtable_title, file_category])
            report_builder(airtable_obj=airtable_obj, file_name=file_name, file_category=file_category)

            with open(os.path.join(save_path, file_name), 'wb') as f:
                f.write(file.read())
                f.close()
        except:
            mimetype = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            request = service.files().export_media(fileId=real_file_id, mimeType=mimetype)
            file = downloader_func(request)
            file.seek(0)
            file_name = file_name.replace('/', "_")
            # report.append([airtable_name, f'{file_name}.docx','', airtable_title, file_category])
            report_builder(airtable_obj=airtable_obj, file_name=f'{file_name}.docx', file_category=file_category)

            with open(os.path.join(save_path, f'{file_name}.docx'), 'wb') as f:
                f.write(file.read())
                f.close()
    elif mimetype == 'application/vnd.google-apps.drawing':
        mimetype = 'application/pdf'
        request = service.files().export_media(fileId=real_file_id, mimeType=mimetype)
        file = downloader_func(request)
        file.seek(0)
        file_name = file_name.replace('/', "_")
        # report.append([airtable_name, f'{file_name}.pdf', 'was google drawing not really pdf', airtable_title, file_category])
        report_builder(airtable_obj=airtable_obj, file_name=f'{file_name}.pdf',
                       export_status='was google drawing not really pdf',
                       file_category=file_category)

        with open(os.path.join(save_path, f'{file_name}.pdf'), 'wb') as f:
            f.write(file.read())
            f.close()
    else:
        mimetype = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        request = service.files().export_media(fileId=real_file_id, mimeType=mimetype)
        file = downloader_func(request)
        file.seek(0)
        file_name = file_name.replace('/', "_")
        # report.append([airtable_name, f'{file_name}.docx', '', airtable_title, file_category])
        report_builder(airtable_obj=airtable_obj, file_name=f'{file_name}.docx',
                       file_category=file_category)
        with open(os.path.join(save_path, f'{file_name}.docx'), 'wb') as f:
            f.write(file.read())
            f.close()


def search_file(save_path, airtable_obj):
    global report
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('../token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open('../token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        socket.setdefaulttimeout(1800)
        service = build('drive', 'v3', credentials=creds)

        # Call the Drive v3 API
        results = service.files().list(q="'{}' in parents".format(airtable_obj['location_in_drive']),
                                       pageSize=10, fields="nextPageToken, files(id, name)").execute()
        current_folder_name = str(
            (service.files().get(fileId=airtable_obj['location_in_drive'], fields='name').execute())[
                'name']).lower().strip(' ')
        status_list = ['draft', 'obsolete', 'approved']
        folder_list = []
        for folder in results['files']:
            folder_list.append((folder['name']).lower().strip(' '))
        if results['files'] == []:
            results = service.files().get(fileId=airtable_obj['location_in_drive']).execute()
            check_approved = (results['name']).lower()
            check_approved = check_approved.strip(' ')

            if check_approved == 'draft' or check_approved == 'obsolete':

                file_id = get_parent(file_real_id=results['id'], service=service)
                results = service.files().list(q="'{}' in parents".format(file_id),
                                               pageSize=10, fields="nextPageToken, files(id, name)").execute()
                items = results.get('files', [])
                report_builder(airtable_obj=airtable_obj, export_status='code return a dir')
                download_multiple_file(items, service, creds, airtable_obj)
            else:
                download_file(real_file_id=results['id'], file_name=results['name'], creds=creds,
                              airtable_obj=airtable_obj, save_path=save_path, mimetype=results['mimeType'])
        elif current_folder_name in status_list:
            file_id = get_parent(service=service, file_real_id=airtable_obj['location_in_drive'])
            results = service.files().list(q="'{}' in parents".format(file_id),
                                           pageSize=10, fields="nextPageToken, files(id, name)").execute()
            report_builder(airtable_obj=airtable_obj, export_status='return back')
        elif 'approved' in folder_list:
            items = results.get('files', [])
            download_multiple_file(items, service, creds, airtable_obj)
        else:
            report_builder(airtable_obj=airtable_obj, export_status='not in DOC control folder')



    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: in doc id: {airtable_obj["doc_id"]} {error}')
        # report[airtable_name] = 'not available'
        # report.append([airtable_name, 'not available'])
        report_builder(airtable_obj=airtable_obj, export_status='not available')


if __name__ == '__main__':
    # config
    header = []
    SCOPES = ['https://www.googleapis.com/auth/drive']
    report = []
    base_id = '<Airtable base ID>'
    table_name = "Airtable table name"
    view_name = "Airtable view name"
    save_path = '<Path to save files>'.format(table_name)
    if not (os.path.exists(save_path)):
        os.mkdir(save_path)
    doc_table = pull_airtable(base_id, table_name, view_name, save_path)
    for doc in tqdm(doc_table, position=0):
        doc = doc_table[doc]
        search_file(save_path=save_path, airtable_obj=doc)
    export_to_excel()
