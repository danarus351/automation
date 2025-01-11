import json
from snowflake.snowpark import Session
import os




def create_snow_session():
    with open('<snow engin api json>, 'r') as f:
        connection_parameters = json.load(f)
        f.close()
    new_session = Session.builder.configs(connection_parameters).create()

    new_session.use_database('<db name>')
    new_session.use_schema('schema name')
    new_session.use_warehouse('warehouse name')
    return new_session



def create_snowpipes():
    session = create_snow_session()
    folders = ['<folders list>']
    base_path = '<folders base path>'
    for folder in folders:
        fp = os.path.join(base_path, folder)
        dirlis = [x for x in os.listdir(fp) if os.path.isdir(os.path.join(fp, x))]
        for dir in dirlis:
            # create stage
            stage_query = f'''CREATE OR REPLACE STAGE "{folder}_{dir}_stage" URL = 's3://<s3 bucket name path>{folder}/{dir}' storage_integration = S3INT;
            '''
            # create table
            table_query = f'''CREATE OR REPLACE  TABLE "{folder}_{dir}" AS
            SELECT METADATA$FILENAME  
            FROM
            @"{folder}_{dir}_stage"
            WHERE
            METADATA$FILENAME LIKE '%.png';
            '''

            #create pipe
            pipe_query = f'''create or replace pipe "{folder}_{dir}_pipe"  auto_ingest=true as
            copy into dwh1.wi_results."{folder}_{dir}" (METADATA$FILENAME) from 
            (SELECT METADATA$FILENAME  
            FROM
            @"{folder}_{dir}_stage");'''
            print(stage_query)
            print(session.sql(stage_query).collect())
            print(session.sql(table_query).collect())
            print(session.sql(pipe_query).collect())


if __name__ == '__main__':
    create_snowpipes()