import telebot
from loguru import logger
import os
import time
from telebot.types import InputFile
import boto3
import requests
import json
import botocore
from openai import OpenAI
from io import BytesIO
from PIL import Image
from botocore.exceptions import BotoCoreError, ClientError
import time
import uuid


class Bot:

    def __init__(self, token, telegram_chat_url):

        self.telegram_bot_client = telebot.TeleBot(token)

        self.telegram_bot_client.remove_webhook()
        time.sleep(0.5)

        self.telegram_bot_client.set_webhook(url=f'{telegram_chat_url}/{token}/',
                                             timeout=60,
                                             certificate=open("khaldounbotpublickey.pem", 'r'))

    def send_text(self, chat_id, text):
        self.telegram_bot_client.send_message(chat_id, text)

    @staticmethod
    def is_current_msg_photo(msg):
        return 'photo' in msg

    def download_user_photo(self, msg):
        if not self.is_current_msg_photo(msg):
            raise RuntimeError(f'Message content of type \'photo\' expected')

        file_info = self.telegram_bot_client.get_file(msg['photo'][-1]['file_id'])
        data = self.telegram_bot_client.download_file(file_info.file_path)

        timestamp = int(time.time())
        unique_id = str(uuid.uuid4().hex)
        file_name = f"{timestamp}_{unique_id}.jpg"

        with open(file_name, 'wb') as photo:
            photo.write(data)

        return file_name



    def send_photo(self, chat_id, img_path):
        if not os.path.exists(img_path):
            raise RuntimeError("Image path doesn't exist")

        self.telegram_bot_client.send_photo(
            chat_id,
            InputFile(img_path)
        )


    def handle_message(self, msg):

        logger.info(f'Incoming message: {msg}')
        self.send_text(msg['chat']['id'], f'Your original message: {msg["text"]}')


class ObjectDetectionBot(Bot):

    def __init__(self, token, telegram_chat_url):

        Bot.__init__(self, token, telegram_chat_url)

        secrets_manager_name = "Khaldoun-Secret"
        region_name = "eu-west-1"

        session = boto3.session.Session()
        self.secrets_client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = self.secrets_client.get_secret_value(
                SecretId=secrets_manager_name
            )
        except ClientError as e:
            raise e
        secrets = json.loads(get_secret_value_response['SecretString'])

        self.TELEGRAM_TOKEN = token
        self.TELEGRAM_APP_URL = telegram_chat_url

        self.images_bucket = secrets['BUCKET_NAME']
        self.AWS_REGION = secrets['REGION']
        self.s3_access_key = secrets['S3_ACCESS_KEY']
        self.s3_secret_key = secrets['S3_SECRET_KEY']
        self.queue_name = secrets['SQS_URL']

        self.dynamodb = boto3.resource('dynamodb', region_name=self.AWS_REGION)
        self.table_name = 'Khaldoun-DynamoDB-Table'
        self.table = self.dynamodb.Table(self.table_name)

        self.sqs_client = boto3.client('sqs', region_name=self.AWS_REGION)
        sqs_url = self.queue_name

        self.s3_client = boto3.client('s3', aws_access_key_id=self.s3_access_key,
                                      aws_secret_access_key=self.s3_secret_key)

        self.Bucket_Name = self.images_bucket
        self.aws_region = self.AWS_REGION

        self.s3_resource = boto3.resource(
            's3',
            region_name=self.aws_region,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key
        )

        self.sqs = boto3.client('sqs', region_name=self.aws_region)
        self.sqs_url = sqs_url

    def handle_message(self, msg):

        logger.info(f'Incoming message: {msg}')

        if self.is_current_msg_photo(msg):
            try:
                self.send_text(msg['chat']['id'], "Photo received!")

                img_path = self.download_user_photo(msg)

                img_name = self.upload_image_to_s3(img_path)

                self.send_sqs_message(msg, img_name)

            except Exception as e:
                logger.error(e)

    def continue_image_chat(self, chat_id, yolo_results, image_name):
        if len(yolo_results) == 1:
            if yolo_results[0] == {'class': "", 'cx': 0, 'cy': 0, 'width': 0, 'height': 0}:
                self.send_text(chat_id, "No predictions found")
                return

        if isinstance(yolo_results, list) and yolo_results:
            if isinstance(yolo_results[0], dict) and 'class' in yolo_results[0]:
                detection_counts = {}
                for item in yolo_results:
                    class_name = item['class']
                    detection_counts[class_name] = detection_counts.get(class_name, 0) + 1

                detection_descriptions = []
                for class_name, count in detection_counts.items():
                    if count == 1:
                        description = f"One {class_name} was detected.\n"
                    else:
                        description = f"{count} {class_name}s were detected.\n"
                    detection_descriptions.append(description)

                summary = ''.join(detection_descriptions)
                self.send_text(chat_id, f"Predictions summary:\n{summary}")

                file_name = os.path.basename(image_name)
                new_filename = self.download_predicted_image_from_s3(file_name)
                self.send_photo(chat_id, new_filename)

        else:
            self.send_text(chat_id, "No detection results available.")

    def upload_image_to_s3(self, img_path):
        try:
            self.s3_resource.Bucket(self.Bucket_Name).put_object(
                Key=os.path.basename(img_path),
                Body=open(img_path, 'rb')
            )
        except Exception as e:
            logger.error(e)
            raise

        return os.path.basename(img_path)

    def download_predicted_image_from_s3(self, file_name):
        s3_file_name = file_name.split('.')[0] + '_prediction.jpg'
        try:
            self.s3_resource.Bucket(self.Bucket_Name).download_file(
                s3_file_name,
                s3_file_name
            )

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error(f"The object does not exist.{e}")
            else:
                raise

        return s3_file_name

    def send_sqs_message(self, message, img_name):

        job_data = {
            "chat_id": message["chat"]["id"],
            "image_name": img_name,
            "telegram_message": message
        }
        job_data_json = json.dumps(job_data)

        message_deduplication_id = str(message["message_id"])

        self.sqs.send_message(QueueUrl=self.sqs_url, MessageBody=job_data_json,
                              MessageGroupId="khaldoun", MessageDeduplicationId=message_deduplication_id)
