import boto3
import csv
import json
from tqdm import tqdm
import concurrent.futures
import threading


counter = 0
counter_lock = threading.Lock()

def session_maker():
   session = boto3.Session(profile_name='<default profile>')
   client = session.client('s3')
   return client


def log_concatanator(line, postion, filename, client):
   if postion == 0:
      part_num = int(filename[23:filename.index('_')])-1
      base_filename = filename[filename.index('_'):]
      below_filename = f'devicelogs/splited-part{part_num}{base_filename}'
      try:
         new_file = client.get_object(Bucket='<bucket name>', Key=below_filename)
         content = new_file['Body'].read().decode('utf-8')
         content_splited = content.split('\n')[-1]
         line = content_splited.strip() + line.strip()
         with open('<path to fixed>/fixed_lines.txt', 'a') as f:
            f.write(line+'\n')
      except Exception as e:
         print(e)


   else:
      part_num = int(filename[23:filename.index('_')])+1
      base_filename = filename[filename.index('_'):]
      below_filename = f'devicelogs/splited-part{part_num}{base_filename}'
      try:
         new_file = client.get_object(Bucket='sdx-s3-snowflake', Key=below_filename)
      except:
         print(below_filename)
      content = new_file['Body'].read().decode('utf-8')
      content_splited = content.split('\n')[0]
      line = line.strip() + content_splited.strip()
      with open('<path to fixed>/fixed_lines.txt', 'a') as f:
         f.write(line+'\n')



def cutting_checker(client, filename):
   global counter
   filename = filename.strip('\n')
   filename = f'devicelogs/{filename}'
   original = client.get_object(Bucket='<bucket name>', Key=filename)
   content = original['Body'].read().decode('utf-8')
   content_splited = content.split('\n')

   try:
      json.loads(content_splited[0])
      # print('first line is ok')
   except:
      log_concatanator(line=content_splited[0], postion=0, filename=filename,client=client)
      with counter_lock:
         counter += 1
      print(f'\rnumber of proccess files: {counter}')
   try:
      last_line = content_splited[-1]
      if last_line == '':
         last_line = content_splited[-2]
      json.loads(last_line)
   except:
      log_concatanator(line=content_splited[-1], postion=len(content_splited)-1, filename=filename,client=client)
      with counter_lock:
         counter += 1
      print(f'\rnumber of proccess files: {counter}')
def read_csv():
   """"
   itrate files in the csv
   each file should be read from s3
   check if the problem is at the end or at the begining
   pull the file above or below
   combain the line with no spaceing
   write to master file
   upload the file to s3 and let the lambda fix it
   """
   client = session_maker()
   # reading csv file
   with open('<list of error files form snowflake>') as File_object:
      reader = csv.reader(File_object)
      next(reader, None)
      # for row in tqdm(reader):
      #    print(row[0])
      #    cutting_checker(client, row[0])

      with concurrent.futures.ProcessPoolExecutor(max_workers=10000) as executor:
         futures = {executor.submit(cutting_checker, client, row[0]) for row in reader}
      for future in futures:
         future.result()



   # filename = 'splited-part1_ls.s3.0fb04d77-ddac-4968-8dfb-5f5e55917cd3.2023-07-25T09.38.part1569.txt'
   # cutting_checker(session_maker(), filename)




if __name__ == '__main__':

   read_csv()