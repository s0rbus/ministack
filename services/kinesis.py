"""
Kinesis Data Streams Emulator.
JSON-based API via X-Amz-Target (Kinesis_20131202).
Supports: CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary,
          ListStreams, PutRecord, PutRecords, GetShardIterator, GetRecords,
          MergeShards, SplitShard, ListShards.
"""

import json
import time
import base64
import hashlib
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("kinesis")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_streams: dict = {}  # stream_name -> {arn, shards: {shard_id: {records: [...]}}, status, retention}
_shard_iterators: dict = {}  # iterator_token -> {stream, shard_id, position}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateStream": _create_stream,
        "DeleteStream": _delete_stream,
        "DescribeStream": _describe_stream,
        "DescribeStreamSummary": _describe_stream_summary,
        "ListStreams": _list_streams,
        "ListShards": _list_shards,
        "PutRecord": _put_record,
        "PutRecords": _put_records,
        "GetShardIterator": _get_shard_iterator,
        "GetRecords": _get_records,
        "IncreaseStreamRetentionPeriod": _increase_retention,
        "DecreaseStreamRetentionPeriod": _decrease_retention,
        "AddTagsToStream": _add_tags,
        "ListTagsForStream": _list_tags,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


def _create_stream(data):
    name = data.get("StreamName")
    shard_count = data.get("ShardCount", 1)
    if not name:
        return error_response_json("ValidationException", "StreamName is required", 400)
    if name in _streams:
        return error_response_json("ResourceInUseException", f"Stream {name} already exists", 400)

    arn = f"arn:aws:kinesis:{REGION}:{ACCOUNT_ID}:stream/{name}"
    shards = {}
    for i in range(shard_count):
        shard_id = f"shardId-{i:012d}"
        shards[shard_id] = {
            "records": [],
            "sequence_counter": 0,
            "tags": {},
        }

    _streams[name] = {
        "StreamName": name,
        "StreamARN": arn,
        "StreamStatus": "ACTIVE",
        "RetentionPeriodHours": 24,
        "shards": shards,
        "tags": {},
        "CreationTimestamp": time.time(),
    }
    return json_response({})


def _delete_stream(data):
    name = data.get("StreamName")
    if name not in _streams:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    del _streams[name]
    return json_response({})


def _describe_stream(data):
    name = data.get("StreamName")
    stream = _streams.get(name)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    return json_response({"StreamDescription": _stream_desc(stream)})


def _describe_stream_summary(data):
    name = data.get("StreamName")
    stream = _streams.get(name)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    desc = _stream_desc(stream)
    desc.pop("Shards", None)
    return json_response({"StreamDescriptionSummary": {**desc, "OpenShardCount": len(stream["shards"])}})


def _list_streams(data):
    names = list(_streams.keys())
    return json_response({"StreamNames": names, "HasMoreStreams": False})


def _list_shards(data):
    name = data.get("StreamName")
    stream = _streams.get(name)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    shards = [_shard_out(sid, s) for sid, s in stream["shards"].items()]
    return json_response({"Shards": shards})


def _put_record(data):
    name = data.get("StreamName")
    stream = _streams.get(name)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)

    partition_key = data.get("PartitionKey", "")
    record_data = data.get("Data", "")

    # Route to shard based on partition key hash
    shard_id = _get_shard_for_key(partition_key, len(stream["shards"]))
    shard = stream["shards"][shard_id]
    shard["sequence_counter"] += 1
    seq = f"{int(time.time() * 1000):020d}{shard['sequence_counter']:010d}"

    shard["records"].append({
        "SequenceNumber": seq,
        "ApproximateArrivalTimestamp": time.time(),
        "Data": record_data,
        "PartitionKey": partition_key,
    })

    return json_response({"ShardId": shard_id, "SequenceNumber": seq})


def _put_records(data):
    name = data.get("StreamName")
    stream = _streams.get(name)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)

    records = data.get("Records", [])
    results = []
    failed = 0

    for rec in records:
        partition_key = rec.get("PartitionKey", "")
        record_data = rec.get("Data", "")
        shard_id = _get_shard_for_key(partition_key, len(stream["shards"]))
        shard = stream["shards"][shard_id]
        shard["sequence_counter"] += 1
        seq = f"{int(time.time() * 1000):020d}{shard['sequence_counter']:010d}"
        shard["records"].append({
            "SequenceNumber": seq,
            "ApproximateArrivalTimestamp": time.time(),
            "Data": record_data,
            "PartitionKey": partition_key,
        })
        results.append({"SequenceNumber": seq, "ShardId": shard_id})

    return json_response({"FailedRecordCount": failed, "Records": results})


def _get_shard_iterator(data):
    name = data.get("StreamName")
    shard_id = data.get("ShardId")
    iterator_type = data.get("ShardIteratorType", "LATEST")
    seq = data.get("StartingSequenceNumber", "")

    stream = _streams.get(name)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    if shard_id not in stream["shards"]:
        return error_response_json("ResourceNotFoundException", f"Shard {shard_id} not found", 400)

    shard = stream["shards"][shard_id]
    if iterator_type == "TRIM_HORIZON":
        position = 0
    elif iterator_type == "LATEST":
        position = len(shard["records"])
    elif iterator_type in ("AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER"):
        records = shard["records"]
        position = next((i for i, r in enumerate(records) if r["SequenceNumber"] >= seq), len(records))
        if iterator_type == "AFTER_SEQUENCE_NUMBER":
            position += 1
    else:
        position = len(shard["records"])

    token = new_uuid()
    _shard_iterators[token] = {"stream": name, "shard_id": shard_id, "position": position}
    return json_response({"ShardIterator": token})


def _get_records(data):
    iterator = data.get("ShardIterator")
    limit = data.get("Limit", 10000)

    state = _shard_iterators.get(iterator)
    if not state:
        return error_response_json("ExpiredIteratorException", "Shard iterator has expired", 400)

    stream = _streams.get(state["stream"])
    if not stream:
        return error_response_json("ResourceNotFoundException", "Stream not found", 400)

    shard = stream["shards"].get(state["shard_id"])
    if not shard:
        return error_response_json("ResourceNotFoundException", "Shard not found", 400)

    pos = state["position"]
    records = shard["records"][pos:pos + limit]
    new_pos = pos + len(records)
    state["position"] = new_pos

    next_token = new_uuid()
    _shard_iterators[next_token] = {"stream": state["stream"], "shard_id": state["shard_id"], "position": new_pos}

    return json_response({
        "Records": records,
        "NextShardIterator": next_token,
        "MillisBehindLatest": 0,
    })


def _increase_retention(data):
    name = data.get("StreamName")
    hours = data.get("RetentionPeriodHours", 24)
    if name in _streams:
        _streams[name]["RetentionPeriodHours"] = hours
    return json_response({})


def _decrease_retention(data):
    name = data.get("StreamName")
    hours = data.get("RetentionPeriodHours", 24)
    if name in _streams:
        _streams[name]["RetentionPeriodHours"] = hours
    return json_response({})


def _add_tags(data):
    name = data.get("StreamName")
    tags = data.get("Tags", {})
    if name in _streams:
        _streams[name]["tags"].update(tags)
    return json_response({})


def _list_tags(data):
    name = data.get("StreamName")
    stream = _streams.get(name)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    tags = [{"Key": k, "Value": v} for k, v in stream["tags"].items()]
    return json_response({"Tags": tags, "HasMoreTags": False})


# --- Helpers ---

def _get_shard_for_key(partition_key: str, shard_count: int) -> str:
    h = int(hashlib.md5(partition_key.encode()).hexdigest(), 16)
    idx = h % shard_count
    return f"shardId-{idx:012d}"


def _shard_out(shard_id, shard):
    return {
        "ShardId": shard_id,
        "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "340282366920938463463374607431768211455"},
        "SequenceNumberRange": {"StartingSequenceNumber": "0"},
    }


def _stream_desc(stream):
    return {
        "StreamName": stream["StreamName"],
        "StreamARN": stream["StreamARN"],
        "StreamStatus": stream["StreamStatus"],
        "RetentionPeriodHours": stream["RetentionPeriodHours"],
        "StreamCreationTimestamp": stream["CreationTimestamp"],
        "Shards": [_shard_out(sid, s) for sid, s in stream["shards"].items()],
        "HasMoreShards": False,
        "EnhancedMonitoring": [{"ShardLevelMetrics": []}],
    }
