import json
import boto3
import csv
from io import StringIO
from urllib.parse import unquote_plus

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('SweetsRecipes')

def lambda_handler(event, context):
    try:
        for sqs_record in event['Records']:
            # SQSメッセージのbodyをJSON形式で読み込み
            message_body = json.loads(sqs_record['body'])

            # 'Records' キーが存在するかチェック
            if 'Records' in message_body:
                for record in message_body['Records']:
                    # S3のバケット名とキーを取得
                    if 's3' in record:
                        bucket = record['s3']['bucket']['name']
                        key = unquote_plus(record['s3']['object']['key'])
                        response = s3.get_object(Bucket=bucket, Key=key)
                        csv_file = response['Body'].read().decode('utf-8')
                        rows = csv.DictReader(StringIO(csv_file))

                        for row in rows:
                            sweet_name = row['SweetName']
                            ingredients = {k: v for k, v in row.items() if k != 'SweetName'}
                            serving = {}  # デフォルトで空の辞書を使用

                            # ServingSizeフィールドがある場合は辞書型として保存
                            if 'ServingSize' in row:
                                serving = json.loads(row['ServingSize'])

                            item = {
                                'SweetName': sweet_name,
                                'Map': {
                                    'Ingredients': ingredients,
                                    'Serving': serving  # 正しく格納された Serving データ
                                }
                            }
                            table.put_item(Item=item)
            else:
                print("Expected 'Records' key in message body but not found.")
        
        return {'statusCode': 200, 'body': json.dumps('CSV processed and data uploaded to DynamoDB')}
    
    except Exception as e:
        print(f"Error processing record: {e}")
        return {'statusCode': 500, 'body': json.dumps('Error processing CSV file')}
