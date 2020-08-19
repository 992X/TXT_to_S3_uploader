import os
import boto3
import pandas as pd
from os.path import join, dirname
from dotenv import load_dotenv
from botocore.config import Config

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER')
ACCESS_KEY_ID = os.getenv('ACCESS_KEY_ID')
ACCESS_SECRET_KEY = os.getenv('ACCESS_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

SOURCE_PATH = '~/Desktop/Sample dara from Romeo Power/'
FILE_NAMES = ['PK-0005_08-24-17.csv', 'PK-0005_08-25-17.csv', 'PK-0005_08-26-17.csv', 'PK-0005_08-27-17.csv']
NAME_FILE_NAME = 'names.txt'

s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

if __name__ == "__main__":
    # load name files to memory
    name_count = 0
    name_list = []
    with open(NAME_FILE_NAME) as fp: 
        while True: 
            name_list.append(fp.readline()[:-1])
            if not name_list[name_count]:
                break
            name_count += 1

    # load sample files to memory
    dfs = []
    for i in range(4):
        dfs.append(pd.read_csv(SOURCE_PATH + FILE_NAMES[i]))

    # Start uploading
    for i in range(name_count):
        s3.put_object(Bucket=BUCKET_NAME, Key=UPLOAD_FOLDER + name_list[i], Body=dfs[i%4].to_csv(index=False))
