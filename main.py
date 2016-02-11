from __future__ import print_function

import boto3
import json
import logging
import math
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Version 0.0.1")

SURPLUS_RATE = 1.2
THRESHOLD_RATE = {'Upper': 0.8, 'Lower': 0.5}

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    info = EventInfo(event)
    logger.info("EventInfo: " + str(info))

    def calc():
        RERIOD = timedelta(minutes=10)
        ave = Metrics(message).getAverage(RERIOD)
        if ave == None:
            ave = 0.1
        return int(math.ceil(ave * SURPLUS_RATE))

    def update(provision):
        table = Table(message.getTableName(), message.getIndexName())
        table.update(message.getMetricName(), provision)

        for key, rate in THRESHOLD_RATE.items():
            Alarm(message.makeAlarmName(key)).update(rate, provision)

    update(calc())

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
        self.table_report = Table(info.table_report, info.cognitoId)
        self.table_leaf = Table(info.table_leaf, info.cognitoId)

    def images(self):
        leaves = self.table_leaf.find(self.info.reportId)
        s3 = boto3.client('s3')
        def getUrl(leaf):
            path = "photo/reduced/mainview/%s/%s/%s.jpg" % (self.info.cognitoId, self.info.reportId, leaf['LEAF_ID'])
            return s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': self.bucketName,
                    'Key': path
                }
            )
        return map(getUrl, leaves)

class Table:
    def __init__(self, tableName, cognitoId):
        self.column_cognitoId = "COGNITO_ID"
        self.column_reportId = "REPORT_ID"
        self.indexName = "COGNITO_ID-REPORT_ID-index"
        self.tableName = tableName
        self.cognitoId = cognitoId
        self.src = boto3.resource('dynamodb').Table(tableName)

    def get(self, reportId):
        res = self.src.get_item(Key={
                self.column_cognitoId: self.cognitoId,
                self.column_reportId: reportId
            }
        )
        return res['Item']

    def find(self, indexName, reportId):
        return self.src.scan(
            IndexName=indexName,
            FilterExpression=Attr(self.column_cognitoId).eq(self.cognitoId) & Attr(self.column_reportId).eq(reportId)
        )

    def update(self, metricName, provision):
        metricKeys = {
            'ConsumedReadCapacityUnits': 'ReadCapacityUnits',
            'ConsumedWriteCapacityUnits': 'WriteCapacityUnits'
        }
        metricKey = metricKeys[metricName]
        logger.info("Updating provision %s(%s) %s: %s" % (self.tableName, self.indexName, metricKey, provision))

        def updateThroughput(src):
            map = {}
            for name in metricKeys.values():
                map[name] = src[name]

            map[metricKey] = provision
            return map

        if self.indexName == None:
            self.src.update(ProvisionedThroughput=updateThroughput(self.src.provisioned_throughput))
        else:
            index = next(iter(filter(lambda x: x['IndexName'] == self.indexName, self.src.global_secondary_indexes)), None)
            if index == None:
                raise Exception('No index: ' + indexName)
            update = {
                'IndexName': indexName,
                'ProvisionedThroughput': updateThroughput(index['ProvisionedThroughput'])
            }
            self.src.update(GlobalSecondaryIndexUpdates=[{'Update': update}])

class Message:
    def __init__(self, text):
        self.src = json.loads(text)

    def __str__(self):
        return json.dumps(self.src, indent=4)

    def getMetricName(self):
        return self.src['Trigger']['MetricName']

    def getNamespace(self):
        return self.src['Trigger']['Namespace']

    def getDimensions(self):
        return self.src['Trigger']['Dimensions']

    def dimension(self, name):
        found = filter(lambda x: x['name'] == name, self.getDimensions())
        return next(iter(map(lambda x: x['value'], found)), None)

    def getTableName(self):
        return self.dimension('TableName')

    def getIndexName(self):
        return self.dimension('GlobalSecondaryIndexName')

    def getAlarmName(self):
        return self.src['AlarmName']

    def makeAlarmName(self, key):
        list = [self.getTableName(), self.getIndexName(), self.getMetricName(), key]
        return "-".join(filter(lambda x: x != None, list)).replace('.', '-')

class Metrics:
    def __init__(self, message):
        self.message = message
        def fixDim(x):
            map = {}
            for key, value in x.items():
                map[key.capitalize()] = value
            return map
        self.dimensions = map(fixDim, message.getDimensions())

    def getValue(self, key, period):
        endTime = datetime.now()
        startTime = endTime - period * 2

        statistics = cloudwatch.get_metric_statistics(
            Namespace=self.message.getNamespace(),
            MetricName=self.message.getMetricName(),
            Dimensions=self.dimensions,
            Statistics=[key],
            StartTime=startTime,
            EndTime=endTime,
            Period=period.seconds
        )

        logger.info("Current Metrics: " + str(statistics))
        return next(iter(map(lambda x: x[key], statistics['Datapoints'])), None)

    def getAverage(self, period):
        return self.getValue('Average', period)

    def getMaximum(self, period):
        return self.getValue('Maximum', period)

class Alarm:
    def __init__(self, name):
        self.name = name
        alarms = cloudwatch.describe_alarms(AlarmNames=[name])
        self.src = next(iter(alarms['MetricAlarms']), None)
        if self.src == None:
            raise Exception("No alarm found: " + name)

    def update(self, rate, provision):
        period = self.src['Period']
        value = provision * rate
        if value <= 0.5:
            value = 0
        threshold = value * period
        logger.info("Updating threshold %s: %s * %s = %s" % (self.name, value, period, threshold))

        cloudwatch.put_metric_alarm(
            AlarmName=self.src['AlarmName'],
            ActionsEnabled=self.src['ActionsEnabled'],
            MetricName=self.src['MetricName'],
            Namespace=self.src['Namespace'],
            Dimensions=self.src['Dimensions'],
            Statistic=self.src['Statistic'],
            OKActions=self.src['OKActions'],
            AlarmActions=self.src['AlarmActions'],
            InsufficientDataActions=self.src['InsufficientDataActions'],
            Period=period,
            EvaluationPeriods=self.src['EvaluationPeriods'],
            ComparisonOperator=self.src['ComparisonOperator'],
            Threshold=threshold
        )
