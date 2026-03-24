"""
CloudWatch Logs Service Emulator.
JSON-based API via X-Amz-Target (Logs_20140328).
Supports: CreateLogGroup, DeleteLogGroup, DescribeLogGroups,
          CreateLogStream, DeleteLogStream, DescribeLogStreams,
          PutLogEvents, GetLogEvents, FilterLogEvents.
"""

import json
import time
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("logs")

_log_groups: dict = {}  # group_name -> {arn, streams: {stream_name: {events: [...], upload_seq}}}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateLogGroup": _create_log_group,
        "DeleteLogGroup": _delete_log_group,
        "DescribeLogGroups": _describe_log_groups,
        "CreateLogStream": _create_log_stream,
        "DeleteLogStream": _delete_log_stream,
        "DescribeLogStreams": _describe_log_streams,
        "PutLogEvents": _put_log_events,
        "GetLogEvents": _get_log_events,
        "FilterLogEvents": _filter_log_events,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidOperationException", f"Unknown: {action}", 400)
    return handler(data)


def _create_log_group(data):
    name = data.get("logGroupName")
    if name in _log_groups:
        return error_response_json("ResourceAlreadyExistsException", f"Log group {name} already exists", 400)
    _log_groups[name] = {
        "arn": f"arn:aws:logs:us-east-1:000000000000:log-group:{name}",
        "creationTime": int(time.time() * 1000),
        "streams": {},
    }
    return json_response({})


def _delete_log_group(data):
    name = data.get("logGroupName")
    _log_groups.pop(name, None)
    return json_response({})


def _describe_log_groups(data):
    prefix = data.get("logGroupNamePrefix", "")
    groups = []
    for name, g in _log_groups.items():
        if name.startswith(prefix):
            groups.append({
                "logGroupName": name,
                "arn": g["arn"],
                "creationTime": g["creationTime"],
                "storedBytes": 0,
            })
    return json_response({"logGroups": groups})


def _create_log_stream(data):
    group = data.get("logGroupName")
    stream = data.get("logStreamName")
    if group not in _log_groups:
        return error_response_json("ResourceNotFoundException", f"Log group {group} not found", 400)
    _log_groups[group]["streams"][stream] = {"events": [], "uploadSequenceToken": "1"}
    return json_response({})


def _delete_log_stream(data):
    group = data.get("logGroupName")
    stream = data.get("logStreamName")
    if group in _log_groups:
        _log_groups[group]["streams"].pop(stream, None)
    return json_response({})


def _describe_log_streams(data):
    group = data.get("logGroupName")
    if group not in _log_groups:
        return error_response_json("ResourceNotFoundException", f"Log group {group} not found", 400)
    streams = []
    for name, s in _log_groups[group]["streams"].items():
        streams.append({
            "logStreamName": name,
            "creationTime": int(time.time() * 1000),
            "storedBytes": sum(len(e.get("message", "")) for e in s["events"]),
            "uploadSequenceToken": s["uploadSequenceToken"],
        })
    return json_response({"logStreams": streams})


def _put_log_events(data):
    group = data.get("logGroupName")
    stream = data.get("logStreamName")
    events = data.get("logEvents", [])

    if group not in _log_groups:
        _log_groups[group] = {"arn": "", "creationTime": int(time.time() * 1000), "streams": {}}
    if stream not in _log_groups[group]["streams"]:
        _log_groups[group]["streams"][stream] = {"events": [], "uploadSequenceToken": "1"}

    s = _log_groups[group]["streams"][stream]
    for e in events:
        s["events"].append({"timestamp": e.get("timestamp", int(time.time() * 1000)), "message": e.get("message", ""), "ingestionTime": int(time.time() * 1000)})

    token = str(int(s["uploadSequenceToken"]) + 1)
    s["uploadSequenceToken"] = token
    return json_response({"nextSequenceToken": token})


def _get_log_events(data):
    group = data.get("logGroupName")
    stream = data.get("logStreamName")
    limit = data.get("limit", 10000)

    if group not in _log_groups or stream not in _log_groups[group]["streams"]:
        return json_response({"events": [], "nextForwardToken": "f/0", "nextBackwardToken": "b/0"})

    events = _log_groups[group]["streams"][stream]["events"][-limit:]
    return json_response({
        "events": events,
        "nextForwardToken": f"f/{len(events)}",
        "nextBackwardToken": "b/0",
    })


def _filter_log_events(data):
    group = data.get("logGroupName")
    pattern = data.get("filterPattern", "").lower()
    limit = data.get("limit", 10000)

    if group not in _log_groups:
        return json_response({"events": [], "searchedLogStreams": []})

    events = []
    searched = []
    for stream_name, s in _log_groups[group]["streams"].items():
        searched.append({"logStreamName": stream_name, "searchedCompletely": True})
        for e in s["events"]:
            if not pattern or pattern in e.get("message", "").lower():
                events.append({**e, "logStreamName": stream_name})
                if len(events) >= limit:
                    break

    return json_response({"events": events, "searchedLogStreams": searched})
