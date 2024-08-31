import json
import os
import boto3
import urllib.request

# S3クライアントとSlackのAPIトークンを初期化
s3 = boto3.client('s3')
bucket_name = 'recipe-7-7'  # S3のバケット名を指定
slack_token = os.getenv('SLACK_BOT_TOKEN')  # Slackのトークンを環境変数から取得

def lambda_handler(event, context):
    try:
        # イベント全体をログに記録
        print(json.dumps(event))
        
        # SlackイベントからファイルIDを取得
        body = json.loads(event.get('body', '{}'))
        if 'event' in body and 'file' in body['event']:
            file_id = body['event']['file']['id']
        else:
            return {'statusCode': 400, 'body': json.dumps('No file ID found in event')}
        
        if not slack_token:
            return {'statusCode': 500, 'body': json.dumps('Slack bot token not found.')}
        
        result = download_and_upload_file_from_slack(file_id, slack_token)
        return {'statusCode': 200, 'body': json.dumps(result)}
    
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f'Error processing event: {str(e)}')}

def download_and_upload_file_from_slack(file_id, token):
    # Slackのファイル情報を取得
    file_info_url = f"https://slack.com/api/files.info?file={file_id}"
    headers = {'Authorization': f"Bearer {token}"}
    
    # ファイル情報を取得するためのリクエスト
    request = urllib.request.Request(file_info_url, headers=headers)
    
    try:
        with urllib.request.urlopen(request) as response:
            file_info = json.loads(response.read().decode())

        if file_info['ok'] and 'url_private_download' in file_info['file']:
            download_url = file_info['file']['url_private_download']
            # ファイルをダウンロードするためのリクエスト
            download_request = urllib.request.Request(download_url, headers=headers)
            with urllib.request.urlopen(download_request) as file_response:
                file_content = file_response.read()
                file_name = file_info['file'].get('name', 'unknown_filename')
                # S3にファイルをアップロード
                s3.put_object(Bucket=bucket_name, Key=file_name, Body=file_content)
                return "File downloaded and uploaded to S3 successfully"
        else:
            return "Failed to retrieve file info or file not public"
    
    except urllib.error.URLError as e:
        return f"Failed to make request: {e}"
