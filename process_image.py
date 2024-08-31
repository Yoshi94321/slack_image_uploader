import boto3
from boto3.dynamodb.conditions import Key
import json
import urllib.request
from urllib.parse import unquote_plus

# クライアントの初期化
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')
table = dynamodb.Table('SweetsRecipes')

# カスタムモデルのARN
custom_model_arn = 'arn:aws:rekognition:ap-northeast-1:********:project/sweets-rekognition/version/sweets-rekognition.********/********'

# SlackにDynamoDBから取得したレシピをアップする
def post_slack(message):
    send_data = {
        "text": message,
    }
    send_text = json.dumps(send_data)
    
    request = urllib.request.Request(
        "https://hooks.slack.com/services/********", 
        data=send_text.encode('utf-8'), 
        method="POST"
    )
    with urllib.request.urlopen(request) as response:
        response_body = response.read().decode('utf-8')

def format_data(item):
    # データの整形
    sweet_name = item['SweetName']
    ingredients = item['Map']['Ingredients']
    
    # Servingが文字列である場合、空の辞書に設定
    serving = item.get('Map', {}).get('Serving', {})
    if isinstance(serving, str):
        serving = {}

    ingredients_formatted = "\n".join(f"{key}: {value}" for key, value in ingredients.items())
    servings_formatted = "\n".join(f"{key}: {value}" for key, value in serving.items())

    message = f"スイーツ名: {sweet_name}\n\n材料:\n{ingredients_formatted}\n\n分量:\n{servings_formatted}"
    return message


def lambda_handler(event, context):
    try:
        for sqs_record in event['Records']:
            # SQSメッセージのbodyをJSON形式で読み込み
            message_body = json.loads(sqs_record['body'])
            for record in message_body['Records']:
                if 's3' in record:
                    # S3のバケット名とキーを取得
                    bucket = record['s3']['bucket']['name']
                    key = unquote_plus(record['s3']['object']['key'])

                    # Rekognitionを使って画像のカスタムラベル分析を実行
                    rekognition_response = rekognition.detect_custom_labels(
                        ProjectVersionArn=custom_model_arn,
                        Image={'S3Object': {'Bucket': bucket, 'Name': key}},
                        MaxResults=10
                    )

                    # Rekognitionからのレスポンスを処理
                    for label in rekognition_response['CustomLabels']:
                        sweet_name = label['Name']
                        # DynamoDBからレシピを照会
                        response = table.get_item(Key={'SweetName': sweet_name})

                        # レスポンスから安全にデータを取得
                        item = response.get('Item')
                        if item:
                            message = format_data(item)
                        else:
                            message = f"{sweet_name}の情報はDynamoDBに見つかりませんでした。"
                        post_slack(message)
    except Exception as e:
        print(f"処理中に未処理の例外が発生しました: {e}")
        raise e  # ランタイムに例外を通知するために再スロー

    return {
        'statusCode': 200,
        'body': '画像の処理が正常に完了しました'
    }
