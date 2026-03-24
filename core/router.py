"""
AWS API Request Router.
Routes incoming requests to the correct service handler based on:
  - Authorization header (AWS4-HMAC-SHA256 ... SignedHeaders=host;...)
  - X-Amz-Target header (e.g., DynamoDB_20120810.PutItem)
  - Host header (e.g., sqs.us-east-1.amazonaws.com)
  - URL path patterns (e.g., /2015-03-31/functions for Lambda)
"""

import re
import logging

logger = logging.getLogger("localstack-clone")

# Service detection patterns
SERVICE_PATTERNS = {
    "s3": {
        "host_patterns": [r"s3[\.\-]", r"\.s3\."],
        "path_patterns": [r"^/(?!2\d{3}-)"],  # S3 is the fallback for non-API paths
    },
    "sqs": {
        "host_patterns": [r"sqs\."],
        "target_prefixes": ["AmazonSQS"],
        "path_patterns": [r"/queue/", r"Action="],
    },
    "sns": {
        "host_patterns": [r"sns\."],
        "target_prefixes": ["AmazonSNS"],
    },
    "dynamodb": {
        "target_prefixes": ["DynamoDB_20120810"],
        "host_patterns": [r"dynamodb\."],
    },
    "lambda": {
        "path_patterns": [r"^/2015-03-31/functions"],
        "host_patterns": [r"lambda\."],
    },
    "iam": {
        "host_patterns": [r"iam\."],
        "path_patterns": [r"Action=.*(CreateRole|GetRole|ListRoles|PutRolePolicy)"],
    },
    "sts": {
        "host_patterns": [r"sts\."],
        "target_prefixes": ["AWSSecurityTokenService"],
    },
    "secretsmanager": {
        "target_prefixes": ["secretsmanager"],
        "host_patterns": [r"secretsmanager\."],
    },
    "monitoring": {
        "host_patterns": [r"monitoring\."],
        "target_prefixes": ["GraniteServiceVersion20100801"],
    },
    "logs": {
        "target_prefixes": ["Logs_20140328"],
        "host_patterns": [r"logs\."],
    },
    "ssm": {
        "target_prefixes": ["AmazonSSM"],
        "host_patterns": [r"ssm\."],
    },
    "events": {
        "target_prefixes": ["AmazonEventBridge", "AWSEvents"],
        "host_patterns": [r"events\."],
    },
    "kinesis": {
        "target_prefixes": ["Kinesis_20131202"],
        "host_patterns": [r"kinesis\."],
    },
    "ses": {
        "host_patterns": [r"email\."],
        "path_patterns": [r"Action=Send"],
    },
    "states": {
        "target_prefixes": ["AWSStepFunctions"],
        "host_patterns": [r"states\."],
    },
    "ecs": {
        "target_prefixes": ["AmazonEC2ContainerServiceV20141113"],
        "host_patterns": [r"ecs\."],
        "path_patterns": [r"^/clusters", r"^/taskdefinitions", r"^/tasks", r"^/services", r"^/stoptask"],
    },
    "rds": {
        "host_patterns": [r"rds\."],
        "path_patterns": [r"Action=.*DB"],
    },
    "elasticache": {
        "host_patterns": [r"elasticache\."],
        "path_patterns": [r"Action=.*Cache"],
    },
    "glue": {
        "target_prefixes": ["AWSGlue"],
        "host_patterns": [r"glue\."],
    },
    "athena": {
        "target_prefixes": ["AmazonAthena"],
        "host_patterns": [r"athena\."],
    },
}


def detect_service(method: str, path: str, headers: dict, query_params: dict) -> str:
    """Detect which AWS service a request is targeting."""
    host = headers.get("host", "")
    target = headers.get("x-amz-target", "")
    auth = headers.get("authorization", "")
    content_type = headers.get("content-type", "")

    # 1. Check X-Amz-Target header (most reliable for JSON-based services)
    if target:
        for svc, patterns in SERVICE_PATTERNS.items():
            for prefix in patterns.get("target_prefixes", []):
                if target.startswith(prefix):
                    return svc

    # 2. Check Authorization header for service name in credential scope
    if auth:
        match = re.search(r"Credential=[^/]+/[^/]+/[^/]+/([^/]+)/", auth)
        if match:
            svc_name = match.group(1)
            if svc_name in SERVICE_PATTERNS:
                return svc_name
            # Map common credential scope names
            scope_map = {
                "monitoring": "monitoring",
                "execute-api": "lambda",
                "ses": "ses",
                "states": "states",
                "kinesis": "kinesis",
                "events": "events",
                "ssm": "ssm",
                "ecs": "ecs",
                "rds": "rds",
                "elasticache": "elasticache",
                "glue": "glue",
                "athena": "athena",
            }
            if svc_name in scope_map:
                return scope_map[svc_name]

    # 3. Check query parameters for Action-based APIs (SQS, SNS, IAM, STS, CloudWatch)
    action = query_params.get("Action", [""])[0] if isinstance(query_params.get("Action"), list) else query_params.get("Action", "")
    if action:
        action_service_map = {
            # SQS actions
            "SendMessage": "sqs", "ReceiveMessage": "sqs", "DeleteMessage": "sqs",
            "CreateQueue": "sqs", "DeleteQueue": "sqs", "ListQueues": "sqs",
            "GetQueueUrl": "sqs", "GetQueueAttributes": "sqs", "SetQueueAttributes": "sqs",
            "PurgeQueue": "sqs", "ChangeMessageVisibility": "sqs",
            "SendMessageBatch": "sqs", "DeleteMessageBatch": "sqs",
            # SNS actions
            "Publish": "sns", "Subscribe": "sns", "Unsubscribe": "sns",
            "CreateTopic": "sns", "DeleteTopic": "sns", "ListTopics": "sns",
            "ListSubscriptions": "sns", "ConfirmSubscription": "sns",
            "SetTopicAttributes": "sns", "GetTopicAttributes": "sns",
            "ListSubscriptionsByTopic": "sns",
            # IAM actions
            "CreateRole": "iam", "GetRole": "iam", "ListRoles": "iam",
            "DeleteRole": "iam", "CreateUser": "iam", "GetUser": "iam",
            "ListUsers": "iam", "CreatePolicy": "iam", "AttachRolePolicy": "iam",
            "PutRolePolicy": "iam", "CreateAccessKey": "iam",
            # STS actions
            "GetCallerIdentity": "sts", "AssumeRole": "sts",
            "GetSessionToken": "sts",
            # CloudWatch actions
            "PutMetricData": "monitoring", "GetMetricData": "monitoring",
            "ListMetrics": "monitoring", "PutMetricAlarm": "monitoring",
            "DescribeAlarms": "monitoring", "DeleteAlarms": "monitoring",
            "GetMetricStatistics": "monitoring", "SetAlarmState": "monitoring",
            # SES actions
            "SendEmail": "ses", "SendRawEmail": "ses",
            "VerifyEmailIdentity": "ses", "VerifyEmailAddress": "ses",
            "ListIdentities": "ses", "DeleteIdentity": "ses",
            "GetSendQuota": "ses", "GetSendStatistics": "ses",
            "ListVerifiedEmailAddresses": "ses",
            # RDS actions
            "CreateDBInstance": "rds", "DeleteDBInstance": "rds", "DescribeDBInstances": "rds",
            "StartDBInstance": "rds", "StopDBInstance": "rds", "RebootDBInstance": "rds",
            "ModifyDBInstance": "rds", "CreateDBCluster": "rds", "DeleteDBCluster": "rds",
            "DescribeDBClusters": "rds", "CreateDBSubnetGroup": "rds", "DescribeDBSubnetGroups": "rds",
            "CreateDBParameterGroup": "rds", "DescribeDBParameterGroups": "rds",
            "DescribeDBEngineVersions": "rds",
            # ElastiCache actions
            "CreateCacheCluster": "elasticache", "DeleteCacheCluster": "elasticache",
            "DescribeCacheClusters": "elasticache", "ModifyCacheCluster": "elasticache",
            "CreateReplicationGroup": "elasticache", "DeleteReplicationGroup": "elasticache",
            "DescribeReplicationGroups": "elasticache",
            "CreateCacheSubnetGroup": "elasticache", "DescribeCacheSubnetGroups": "elasticache",
            "CreateCacheParameterGroup": "elasticache", "DescribeCacheParameterGroups": "elasticache",
            "DescribeCacheEngineVersions": "elasticache",
        }
        if action in action_service_map:
            return action_service_map[action]

    # 4. Check URL path patterns
    path_lower = path.lower()
    if path_lower.startswith("/2015-03-31/functions"):
        return "lambda"
    if path_lower.startswith(("/clusters", "/taskdefinitions", "/tasks", "/services", "/stoptask")):
        return "ecs"
    # smithy-rpc-v2-cbor path: /service/ServiceName/operation/ActionName
    if "/service/" in path_lower and "/operation/" in path_lower:
        if "granite" in path_lower or "cloudwatch" in path_lower:
            return "monitoring"

    # 5. Check host header patterns
    for svc, patterns in SERVICE_PATTERNS.items():
        for hp in patterns.get("host_patterns", []):
            if re.search(hp, host):
                return svc

    # 6. Default to S3 (same as real LocalStack behavior)
    return "s3"


def extract_region(headers: dict) -> str:
    """Extract AWS region from the request."""
    auth = headers.get("authorization", "")
    match = re.search(r"Credential=[^/]+/[^/]+/([^/]+)/", auth)
    if match:
        return match.group(1)
    return "us-east-1"


def extract_account_id(headers: dict) -> str:
    """Extract or generate account ID."""
    return "000000000000"
