from __future__ import print_function

import boto3
import json
import logging
import math
import base64
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Version 1.0.1")

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    info = EventInfo(event)
    logger.info("EventInfo: " + str(info))
    report = Report(info)

    return {
        'info': event,
        'info_base64': base64.b64encode(json.dumps(event)),
        'report': report.asJSON()
    }

class EventInfo:
    def __init__(self, event):
        self.src = event
        self.region = event['region']
        self.bucketName = event['bucketName']
        self.urlTimeout = event['urlTimeout']
        self.table_report = event['table_report']
        self.table_leaf = event['table_leaf']
        self.cognitoId = event['cognitoId']
        self.reportId = event['reportId']

    def __str__(self):
        return json.dumps(self.src, indent=4)

class Report:
    def __init__(self, info):
        self.info = info
        self.table_report = Table(info.table_report, info.cognitoId, info.reportId)
        self.table_leaf = Table(info.table_leaf, info.cognitoId, info.reportId)
        self.title = "Report:%s" % (info.reportId)

    def images(self):
        leaves = self.table_leaf.find()
        s3 = boto3.client('s3')
        def getUrl(leaf):
            path = "photo/reduced/mainview/%s/%s/%s.jpg" % (self.info.cognitoId, self.info.reportId, leaf['LEAF_ID'])
            return s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': self.info.bucketName,
                    'Key': path
                }
            )
        return map(getUrl, leaves)

    def asJSON(self):
        json = self.table_report.get()

        json['title'] = self.title
        json['description'] = self.title
        json['images'] = self.images()
        return json;

class Table:
    def __init__(self, tableName, cognitoId, reportId):
        self.column_cognitoId = "COGNITO_ID"
        self.column_reportId = "REPORT_ID"
        self.cognitoId = cognitoId
        self.reportId = reportId
        self.src = boto3.resource('dynamodb').Table(tableName)

    def get(self):
        res = self.src.get_item(Key={
                self.column_cognitoId: self.cognitoId,
                self.column_reportId: self.reportId
            }
        )
        return res['Item']

    def find(self):
        res = self.src.scan(
            IndexName="COGNITO_ID-REPORT_ID-index",
            FilterExpression=Attr(self.column_cognitoId).eq(self.cognitoId) & Attr(self.column_reportId).eq(self.reportId)
        )
        return res['Items']
