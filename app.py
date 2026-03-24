"""
MiniStack — Local AWS Service Emulator.
Single-port ASGI application on port 4566 (configurable via GATEWAY_PORT).
Routes requests to service handlers based on AWS headers, paths, and query parameters.
Compatible with AWS CLI, boto3, and any AWS SDK via --endpoint-url.
"""

import os
import json
import asyncio
import logging
from urllib.parse import parse_qs, urlparse

from core.router import detect_service, extract_region, extract_account_id
from services import s3, sqs, sns, dynamodb, lambda_svc, secretsmanager, cloudwatch_logs
from services import ssm, eventbridge, kinesis, cloudwatch, ses, stepfunctions
from services import ecs, rds, elasticache, glue, athena
from services.iam_sts import handle_iam_request, handle_sts_request

# Configure logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ministack")

# Service handler mapping
SERVICE_HANDLERS = {
    "s3": s3.handle_request,
    "sqs": sqs.handle_request,
    "sns": sns.handle_request,
    "dynamodb": dynamodb.handle_request,
    "lambda": lambda_svc.handle_request,
    "iam": handle_iam_request,
    "sts": handle_sts_request,
    "secretsmanager": secretsmanager.handle_request,
    "logs": cloudwatch_logs.handle_request,
    "ssm": ssm.handle_request,
    "events": eventbridge.handle_request,
    "kinesis": kinesis.handle_request,
    "monitoring": cloudwatch.handle_request,
    "ses": ses.handle_request,
    "states": stepfunctions.handle_request,
    "ecs": ecs.handle_request,
    "rds": rds.handle_request,
    "elasticache": elasticache.handle_request,
    "glue": glue.handle_request,
    "athena": athena.handle_request,
}

BANNER = r"""
  __  __ _       _ ____  _             _
 |  \/  (_)_ __ (_) ___|| |_ __ _  ___| | __
 | |\/| | | '_ \| \___ \| __/ _` |/ __| |/ /
 | |  | | | | | | |___) | || (_| | (__|   <
 |_|  |_|_|_| |_|_|____/ \__\__,_|\___|_|\_\

 Local AWS Service Emulator — Port {port}
 Services: S3, SQS, SNS, DynamoDB, Lambda, IAM, STS, SecretsManager, CloudWatch Logs,
          SSM, EventBridge, Kinesis, CloudWatch, SES, Step Functions,
          ECS, RDS, ElastiCache, Glue, Athena
"""


async def app(scope, receive, send):
    """ASGI application entry point."""
    if scope["type"] == "lifespan":
        await _handle_lifespan(scope, receive, send)
        return

    if scope["type"] != "http":
        return

    # Read request
    method = scope["method"]
    path = scope["path"]
    query_string = scope.get("query_string", b"").decode("utf-8")
    query_params = parse_qs(query_string, keep_blank_values=True)

    headers = {}
    for name, value in scope.get("headers", []):
        headers[name.decode("latin-1").lower()] = value.decode("latin-1")

    body = b""
    while True:
        message = await receive()
        body += message.get("body", b"")
        if not message.get("more_body", False):
            break

    # Health check endpoints
    if path in ("/_localstack/health", "/health", "/_ministack/health"):
        await _send_response(send, 200, {"Content-Type": "application/json"},
            json.dumps({"status": "running", "services": {s: "available" for s in SERVICE_HANDLERS}}).encode())
        return

    # Detect target service
    service = detect_service(method, path, headers, query_params)
    region = extract_region(headers)

    logger.debug(f"{method} {path} -> service={service} region={region}")

    handler = SERVICE_HANDLERS.get(service)
    if not handler:
        await _send_response(send, 400, {"Content-Type": "application/json"},
            json.dumps({"error": f"Unsupported service: {service}"}).encode())
        return

    try:
        status, resp_headers, resp_body = await handler(method, path, headers, body, query_params)
    except Exception as e:
        logger.exception(f"Error handling {service} request: {e}")
        await _send_response(send, 500, {"Content-Type": "application/json"},
            json.dumps({"__type": "InternalError", "message": str(e)}).encode())
        return

    # Add CORS headers (same as LocalStack)
    resp_headers.update({
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "*",
        "x-amzn-requestid": headers.get("x-amzn-requestid", ""),
    })

    await _send_response(send, status, resp_headers, resp_body)


async def _send_response(send, status, headers, body):
    """Send ASGI HTTP response."""
    header_list = [(k.encode("latin-1"), str(v).encode("latin-1")) for k, v in headers.items()]
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": header_list,
    })
    await send({
        "type": "http.response.body",
        "body": body if isinstance(body, bytes) else body.encode("utf-8"),
    })


async def _handle_lifespan(scope, receive, send):
    """Handle ASGI lifespan events."""
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            port = os.environ.get("GATEWAY_PORT", "4566")
            logger.info(BANNER.format(port=port))
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            logger.info("MiniStack shutting down...")
            await send({"type": "lifespan.shutdown.complete"})
            return


def main():
    import uvicorn
    port = int(os.environ.get("GATEWAY_PORT", "4566"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level=LOG_LEVEL.lower())


if __name__ == "__main__":
    main()
