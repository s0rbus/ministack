"""
SQS Service Emulator.
Supports: CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, GetQueueAttributes,
          SetQueueAttributes, PurgeQueue, SendMessage, ReceiveMessage, DeleteMessage,
          ChangeMessageVisibility, SendMessageBatch, DeleteMessageBatch.
"""

import time
import json
import hashlib
import logging
from collections import defaultdict
from urllib.parse import parse_qs

from core.responses import new_uuid, now_iso, md5_hash

logger = logging.getLogger("sqs")

# In-memory queues
# queue_url -> {name, attributes, messages: [{id, body, md5, receipt_handle, sent_at, visible_at, receive_count}], ...}
_queues: dict = {}
_queue_name_to_url: dict = {}

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "4566"


def _queue_url(name: str) -> str:
    host = DEFAULT_HOST
    port = DEFAULT_PORT
    return f"http://{host}:{port}/{ACCOUNT_ID}/{name}"


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    """Handle SQS requests — supports both legacy Query API and modern JSON API."""
    target = headers.get("x-amz-target", "")

    # Modern JSON protocol (boto3 default since ~2023): X-Amz-Target: AmazonSQS.CreateQueue
    if target.startswith("AmazonSQS."):
        action = target.split(".")[-1]
        try:
            data = json.loads(body) if body else {}
        except Exception:
            data = {}
        return _handle_json_action(action, data, path)

    # Legacy Query API: Action=CreateQueue in form body or query string
    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")

    handlers = {
        "CreateQueue": _create_queue,
        "DeleteQueue": _delete_queue,
        "ListQueues": _list_queues,
        "GetQueueUrl": _get_queue_url,
        "GetQueueAttributes": _get_queue_attributes,
        "SetQueueAttributes": _set_queue_attributes,
        "PurgeQueue": _purge_queue,
        "SendMessage": _send_message,
        "ReceiveMessage": _receive_message,
        "DeleteMessage": _delete_message,
        "ChangeMessageVisibility": _change_visibility,
        "SendMessageBatch": _send_message_batch,
        "DeleteMessageBatch": _delete_message_batch,
    }

    handler = handlers.get(action)
    if handler is None:
        return _error("InvalidAction", f"Unknown action: {action}", 400)

    return handler(params, path)


def _handle_json_action(action: str, data: dict, path: str):
    """Handle modern JSON-protocol SQS actions."""

    if action == "CreateQueue":
        name = data.get("QueueName", "")
        if not name:
            return _json_error("MissingParameter", "QueueName is required", 400)
        url = _queue_url(name)
        if url not in _queues:
            _queues[url] = {
                "name": name, "url": url,
                "attributes": {
                    "QueueArn": f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{name}",
                    "CreatedTimestamp": str(int(time.time())),
                    "LastModifiedTimestamp": str(int(time.time())),
                    "VisibilityTimeout": "30",
                    "MaximumMessageSize": "262144",
                    "MessageRetentionPeriod": "345600",
                    "DelaySeconds": "0",
                    "ReceiveMessageWaitTimeSeconds": "0",
                    "ApproximateNumberOfMessages": "0",
                    "ApproximateNumberOfMessagesNotVisible": "0",
                },
                "messages": [], "delayed": [],
            }
            attrs = data.get("Attributes") or data.get("attributes") or {}
            for k, v in attrs.items():
                _queues[url]["attributes"][k] = v
            _queue_name_to_url[name] = url
        return _json_resp({"QueueUrl": url})

    if action == "DeleteQueue":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        if url in _queues:
            name = _queues[url]["name"]
            del _queues[url]
            _queue_name_to_url.pop(name, None)
        return _json_resp({})

    if action == "ListQueues":
        prefix = data.get("QueueNamePrefix", "")
        urls = [u for u, q in _queues.items() if not prefix or q["name"].startswith(prefix)]
        return _json_resp({"QueueUrls": urls})

    if action == "GetQueueUrl":
        name = data.get("QueueName", "")
        url = _queue_name_to_url.get(name)
        if not url:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", f"Queue {name} not found", 400)
        return _json_resp({"QueueUrl": url})

    if action == "SendMessage":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        body_text = data.get("MessageBody", "")
        delay = int(data.get("DelaySeconds", 0) or queue["attributes"].get("DelaySeconds", "0"))
        msg_id = new_uuid()
        md5 = hashlib.md5(body_text.encode()).hexdigest()
        msg = {
            "id": msg_id, "body": body_text, "md5": md5,
            "receipt_handle": None, "sent_at": time.time(),
            "visible_at": time.time() + delay, "receive_count": 0,
            "attributes": {},
        }
        # Parse message attributes
        for attr_name, attr_val in (data.get("MessageAttributes") or {}).items():
            msg["attributes"][attr_name] = attr_val
        queue["messages"].append(msg)
        return _json_resp({"MessageId": msg_id, "MD5OfMessageBody": md5})

    if action == "ReceiveMessage":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        max_msgs = int(data.get("MaxNumberOfMessages", 1))
        vis_timeout = int(data.get("VisibilityTimeout", queue["attributes"].get("VisibilityTimeout", "30")))
        now = time.time()
        visible = [m for m in queue["messages"] if m["visible_at"] <= now]
        to_return = visible[:max_msgs]
        messages = []
        for msg in to_return:
            receipt = new_uuid()
            msg["receipt_handle"] = receipt
            msg["visible_at"] = now + vis_timeout
            msg["receive_count"] += 1
            m = {
                "MessageId": msg["id"],
                "ReceiptHandle": receipt,
                "MD5OfBody": msg["md5"],
                "Body": msg["body"],
            }
            if msg["attributes"]:
                m["MessageAttributes"] = msg["attributes"]
            messages.append(m)
        return _json_resp({"Messages": messages} if messages else {})

    if action == "DeleteMessage":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        receipt = data.get("ReceiptHandle", "")
        queue["messages"] = [m for m in queue["messages"] if m.get("receipt_handle") != receipt]
        return _json_resp({})

    if action == "PurgeQueue":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        queue["messages"].clear()
        return _json_resp({})

    if action == "GetQueueAttributes":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        now = time.time()
        visible = sum(1 for m in queue["messages"] if m["visible_at"] <= now)
        not_visible = sum(1 for m in queue["messages"] if m["visible_at"] > now)
        queue["attributes"]["ApproximateNumberOfMessages"] = str(visible)
        queue["attributes"]["ApproximateNumberOfMessagesNotVisible"] = str(not_visible)
        requested = data.get("AttributeNames", ["All"])
        attrs = {}
        for k, v in queue["attributes"].items():
            if "All" in requested or k in requested:
                attrs[k] = v
        return _json_resp({"Attributes": attrs})

    if action == "SetQueueAttributes":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        for k, v in (data.get("Attributes") or {}).items():
            queue["attributes"][k] = v
        return _json_resp({})

    if action == "ChangeMessageVisibility":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        receipt = data.get("ReceiptHandle", "")
        timeout = int(data.get("VisibilityTimeout", 30))
        for msg in queue["messages"]:
            if msg.get("receipt_handle") == receipt:
                msg["visible_at"] = time.time() + timeout
                break
        return _json_resp({})

    if action == "SendMessageBatch":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        results = []
        for entry in data.get("Entries", []):
            body_text = entry.get("MessageBody", "")
            delay = int(entry.get("DelaySeconds", 0))
            msg_id = new_uuid()
            md5 = hashlib.md5(body_text.encode()).hexdigest()
            queue["messages"].append({
                "id": msg_id, "body": body_text, "md5": md5,
                "receipt_handle": None, "sent_at": time.time(),
                "visible_at": time.time() + delay, "receive_count": 0, "attributes": {},
            })
            results.append({"Id": entry.get("Id"), "MessageId": msg_id, "MD5OfMessageBody": md5})
        return _json_resp({"Successful": results, "Failed": []})

    if action == "DeleteMessageBatch":
        url = data.get("QueueUrl", "") or _url_from_path(path)
        queue = _queues.get(url)
        if not queue:
            return _json_error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
        results = []
        for entry in data.get("Entries", []):
            receipt = entry.get("ReceiptHandle", "")
            queue["messages"] = [m for m in queue["messages"] if m.get("receipt_handle") != receipt]
            results.append({"Id": entry.get("Id")})
        return _json_resp({"Successful": results, "Failed": []})

    return _json_error("InvalidAction", f"Unknown action: {action}", 400)


def _json_resp(data: dict, status: int = 200) -> tuple:
    body = json.dumps(data).encode("utf-8")
    return status, {"Content-Type": "application/x-amz-json-1.0"}, body


def _json_error(code: str, message: str, status: int) -> tuple:
    body = json.dumps({"__type": code, "message": message}).encode("utf-8")
    return status, {"Content-Type": "application/x-amz-json-1.0"}, body


def _create_queue(params: dict, path: str):
    name = _p(params, "QueueName")
    if not name:
        return _error("MissingParameter", "QueueName is required", 400)

    url = _queue_url(name)
    if url not in _queues:
        _queues[url] = {
            "name": name,
            "url": url,
            "attributes": {
                "QueueArn": f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{name}",
                "CreatedTimestamp": str(int(time.time())),
                "LastModifiedTimestamp": str(int(time.time())),
                "VisibilityTimeout": "30",
                "MaximumMessageSize": "262144",
                "MessageRetentionPeriod": "345600",
                "DelaySeconds": "0",
                "ReceiveMessageWaitTimeSeconds": "0",
                "ApproximateNumberOfMessages": "0",
                "ApproximateNumberOfMessagesNotVisible": "0",
            },
            "messages": [],
            "delayed": [],
        }
        # Apply any attributes from the request
        i = 1
        while _p(params, f"Attribute.{i}.Name"):
            attr_name = _p(params, f"Attribute.{i}.Name")
            attr_val = _p(params, f"Attribute.{i}.Value")
            _queues[url]["attributes"][attr_name] = attr_val
            i += 1
        _queue_name_to_url[name] = url

    return _xml(200, "CreateQueueResponse", f"""
        <CreateQueueResult>
            <QueueUrl>{url}</QueueUrl>
        </CreateQueueResult>""")


def _delete_queue(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    if url in _queues:
        name = _queues[url]["name"]
        del _queues[url]
        _queue_name_to_url.pop(name, None)
    return _xml(200, "DeleteQueueResponse", "")


def _list_queues(params: dict, path: str):
    prefix = _p(params, "QueueNamePrefix")
    urls = []
    for url, q in _queues.items():
        if not prefix or q["name"].startswith(prefix):
            urls.append(url)

    members = "".join(f"<QueueUrl>{u}</QueueUrl>" for u in urls)
    return _xml(200, "ListQueuesResponse", f"""
        <ListQueuesResult>
            {members}
        </ListQueuesResult>""")


def _get_queue_url(params: dict, path: str):
    name = _p(params, "QueueName")
    url = _queue_name_to_url.get(name)
    if not url:
        return _error("AWS.SimpleQueueService.NonExistentQueue", f"Queue {name} not found", 400)
    return _xml(200, "GetQueueUrlResponse", f"""
        <GetQueueUrlResult>
            <QueueUrl>{url}</QueueUrl>
        </GetQueueUrlResult>""")


def _get_queue_attributes(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)

    # Update approximate counts
    now = time.time()
    visible = sum(1 for m in queue["messages"] if m["visible_at"] <= now)
    not_visible = sum(1 for m in queue["messages"] if m["visible_at"] > now)
    queue["attributes"]["ApproximateNumberOfMessages"] = str(visible)
    queue["attributes"]["ApproximateNumberOfMessagesNotVisible"] = str(not_visible)

    attrs = ""
    # Check which attributes are requested
    requested = set()
    i = 1
    while _p(params, f"AttributeName.{i}"):
        requested.add(_p(params, f"AttributeName.{i}"))
        i += 1

    for k, v in queue["attributes"].items():
        if not requested or "All" in requested or k in requested:
            attrs += f"<Attribute><Name>{k}</Name><Value>{v}</Value></Attribute>"

    return _xml(200, "GetQueueAttributesResponse", f"""
        <GetQueueAttributesResult>
            {attrs}
        </GetQueueAttributesResult>""")


def _set_queue_attributes(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)

    i = 1
    while _p(params, f"Attribute.{i}.Name"):
        attr_name = _p(params, f"Attribute.{i}.Name")
        attr_val = _p(params, f"Attribute.{i}.Value")
        queue["attributes"][attr_name] = attr_val
        i += 1

    return _xml(200, "SetQueueAttributesResponse", "")


def _purge_queue(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)
    queue["messages"].clear()
    return _xml(200, "PurgeQueueResponse", "")


def _send_message(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)

    body_text = _p(params, "MessageBody")
    delay = int(_p(params, "DelaySeconds") or queue["attributes"].get("DelaySeconds", "0"))
    msg_id = new_uuid()
    md5 = hashlib.md5(body_text.encode()).hexdigest()

    msg = {
        "id": msg_id,
        "body": body_text,
        "md5": md5,
        "receipt_handle": None,
        "sent_at": time.time(),
        "visible_at": time.time() + delay,
        "receive_count": 0,
        "attributes": {},
    }

    # Parse message attributes
    i = 1
    while _p(params, f"MessageAttribute.{i}.Name"):
        attr_name = _p(params, f"MessageAttribute.{i}.Name")
        attr_type = _p(params, f"MessageAttribute.{i}.Value.DataType")
        attr_val = _p(params, f"MessageAttribute.{i}.Value.StringValue")
        msg["attributes"][attr_name] = {"DataType": attr_type, "StringValue": attr_val}
        i += 1

    queue["messages"].append(msg)

    return _xml(200, "SendMessageResponse", f"""
        <SendMessageResult>
            <MessageId>{msg_id}</MessageId>
            <MD5OfMessageBody>{md5}</MD5OfMessageBody>
        </SendMessageResult>""")


def _receive_message(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)

    max_msgs = int(_p(params, "MaxNumberOfMessages") or "1")
    vis_timeout = int(_p(params, "VisibilityTimeout") or queue["attributes"].get("VisibilityTimeout", "30"))
    wait_time = int(_p(params, "WaitTimeSeconds") or "0")

    now = time.time()
    visible = [m for m in queue["messages"] if m["visible_at"] <= now]
    to_return = visible[:max_msgs]

    messages_xml = ""
    for msg in to_return:
        receipt = new_uuid()
        msg["receipt_handle"] = receipt
        msg["visible_at"] = now + vis_timeout
        msg["receive_count"] += 1

        attrs_xml = ""
        for attr_name, attr_val in msg["attributes"].items():
            attrs_xml += f"""<MessageAttribute>
                <Name>{attr_name}</Name>
                <Value>
                    <DataType>{attr_val['DataType']}</DataType>
                    <StringValue>{attr_val['StringValue']}</StringValue>
                </Value>
            </MessageAttribute>"""

        messages_xml += f"""<Message>
            <MessageId>{msg['id']}</MessageId>
            <ReceiptHandle>{receipt}</ReceiptHandle>
            <MD5OfBody>{msg['md5']}</MD5OfBody>
            <Body>{msg['body']}</Body>
            {attrs_xml}
        </Message>"""

    return _xml(200, "ReceiveMessageResponse", f"""
        <ReceiveMessageResult>
            {messages_xml}
        </ReceiveMessageResult>""")


def _delete_message(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)

    receipt = _p(params, "ReceiptHandle")
    queue["messages"] = [m for m in queue["messages"] if m.get("receipt_handle") != receipt]
    return _xml(200, "DeleteMessageResponse", "")


def _change_visibility(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)

    receipt = _p(params, "ReceiptHandle")
    timeout = int(_p(params, "VisibilityTimeout") or "30")

    for msg in queue["messages"]:
        if msg.get("receipt_handle") == receipt:
            msg["visible_at"] = time.time() + timeout
            break

    return _xml(200, "ChangeMessageVisibilityResponse", "")


def _send_message_batch(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)

    results = ""
    i = 1
    while _p(params, f"SendMessageBatchRequestEntry.{i}.Id"):
        entry_id = _p(params, f"SendMessageBatchRequestEntry.{i}.Id")
        body_text = _p(params, f"SendMessageBatchRequestEntry.{i}.MessageBody")
        delay = int(_p(params, f"SendMessageBatchRequestEntry.{i}.DelaySeconds") or "0")
        msg_id = new_uuid()
        md5 = hashlib.md5(body_text.encode()).hexdigest()

        queue["messages"].append({
            "id": msg_id, "body": body_text, "md5": md5,
            "receipt_handle": None, "sent_at": time.time(),
            "visible_at": time.time() + delay, "receive_count": 0, "attributes": {},
        })
        results += f"""<SendMessageBatchResultEntry>
            <Id>{entry_id}</Id><MessageId>{msg_id}</MessageId>
            <MD5OfMessageBody>{md5}</MD5OfMessageBody>
        </SendMessageBatchResultEntry>"""
        i += 1

    return _xml(200, "SendMessageBatchResponse", f"<SendMessageBatchResult>{results}</SendMessageBatchResult>")


def _delete_message_batch(params: dict, path: str):
    url = _p(params, "QueueUrl") or _url_from_path(path)
    queue = _queues.get(url)
    if not queue:
        return _error("AWS.SimpleQueueService.NonExistentQueue", "Queue not found", 400)

    results = ""
    i = 1
    while _p(params, f"DeleteMessageBatchRequestEntry.{i}.Id"):
        entry_id = _p(params, f"DeleteMessageBatchRequestEntry.{i}.Id")
        receipt = _p(params, f"DeleteMessageBatchRequestEntry.{i}.ReceiptHandle")
        queue["messages"] = [m for m in queue["messages"] if m.get("receipt_handle") != receipt]
        results += f"<DeleteMessageBatchResultEntry><Id>{entry_id}</Id></DeleteMessageBatchResultEntry>"
        i += 1

    return _xml(200, "DeleteMessageBatchResponse", f"<DeleteMessageBatchResult>{results}</DeleteMessageBatchResult>")


# --- Helpers ---

def _p(params: dict, key: str, default: str = "") -> str:
    val = params.get(key, [default])
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _url_from_path(path: str) -> str:
    """Extract queue URL from path like /000000000000/queue-name."""
    parts = path.strip("/").split("/")
    if len(parts) >= 2:
        name = parts[-1]
        return _queue_url(name)
    return ""


def _xml(status: int, root_tag: str, inner: str) -> tuple:
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code: str, message: str, status: int) -> tuple:
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
    <Error><Code>{code}</Code><Message>{message}</Message></Error>
    <RequestId>{new_uuid()}</RequestId>
</ErrorResponse>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body
