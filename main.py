import datetime
import os
import time

import boto3
from github import Github
from lxml import html
import requests

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

BUCKET_NAME = 'foodscrapes'
REPO_NAME = 'spulec/foodscrapes'

def main():
    res = requests.get("https://www.thomaskeller.com/tfl/menu")
    root = html.fromstring(res.content)
    menu_link = root.xpath("//a[contains(@type, 'pdf')]")[0].attrib['href']
    file_name = menu_link.split("/")[-1]
    menu_res = requests.get(menu_link)
    s3_client.put_object(
        Body=menu_res.content,
        Bucket=BUCKET_NAME,
        Key=file_name,
    )

    response = textract_client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': BUCKET_NAME,
                'Name': file_name,
            }
        }
    )
    job_id = response['JobId']

    response = textract_client.get_document_text_detection(JobId=job_id)
    while response['JobStatus'] in ['FAILED', 'IN_PROGRESS']:
        response = textract_client.get_document_text_detection(JobId=job_id)
        time.sleep(5)

    text = [
        block['Text'] for block
        in response['Blocks']
        if 'Text' in block and block['BlockType'] == 'LINE'
    ]

    today = datetime.date.today()
    date_string = f"{today.year}-{today.month}-{today.day}"
    out_file = f"{date_string}.txt"

    github_client = Github(os.environ['GITHUB_ACCESS_TOKEN'])
    repo = github_client.get_repo(REPO_NAME)
    repo.create_file(
        path=f"frenchlaundry/{out_file}",
        message=date_string,
        content="\n".join(text),
    )


if __name__ == '__main__':
    main()