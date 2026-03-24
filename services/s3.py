"""
S3 Service Emulator.
Supports: CreateBucket, DeleteBucket, ListBuckets, HeadBucket,
          PutObject, GetObject, DeleteObject, HeadObject, CopyObject,
          ListObjects (v1 & v2), DeleteObjects (batch).
Storage: In-memory (optionally backed by /tmp/localstack-data/s3/).
"""

import os
import re
import logging
from datetime import datetime, timezone
from collections import defaultdict
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring

from core.responses import md5_hash, sha256_hash, now_iso, new_uuid

logger = logging.getLogger("s3")

# In-memory storage
_buckets: dict = {}  # bucket_name -> {"created": iso_str, "objects": {key: {"body": bytes, "metadata": dict, "content_type": str, "etag": str, "last_modified": str, "size": int}}}

DATA_DIR = os.environ.get("S3_DATA_DIR", "/tmp/localstack-data/s3")
PERSIST = os.environ.get("S3_PERSIST", "0") == "1"


def _ensure_bucket(name):
    if name not in _buckets:
        return None
    return _buckets[name]


def _parse_bucket_key(path: str, headers: dict):
    """Extract bucket name and key from path or Host header."""
    host = headers.get("host", "")

    # Virtual-hosted style: bucket.s3.localhost:4566
    vhost_match = re.match(r"^([a-zA-Z0-9.\-_]+)\.s3[\.\-]", host)
    if vhost_match:
        bucket = vhost_match.group(1)
        key = path.lstrip("/")
        return bucket, key

    # Path style: /bucket/key
    parts = path.lstrip("/").split("/", 1)
    bucket = parts[0] if parts else ""
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    """Route S3 requests to handlers."""
    bucket, key = _parse_bucket_key(path, headers)

    # ListBuckets: GET /
    if method == "GET" and not bucket:
        return _list_buckets()

    # Batch delete: POST /?delete
    if method == "POST" and "delete" in query_params:
        return _delete_objects(bucket, body)

    # ListObjects v2: GET /bucket?list-type=2
    if method == "GET" and not key and query_params.get("list-type", [""])[0] == "2":
        return _list_objects_v2(bucket, query_params)

    # ListObjects v1: GET /bucket
    if method == "GET" and not key:
        if "location" in query_params:
            return _get_bucket_location(bucket)
        return _list_objects_v1(bucket, query_params)

    # CreateBucket: PUT /bucket (no key)
    if method == "PUT" and bucket and not key:
        return _create_bucket(bucket, body)

    # DeleteBucket: DELETE /bucket (no key)
    if method == "DELETE" and bucket and not key:
        return _delete_bucket(bucket)

    # HeadBucket: HEAD /bucket
    if method == "HEAD" and bucket and not key:
        return _head_bucket(bucket)

    # PutObject: PUT /bucket/key
    if method == "PUT" and key:
        # CopyObject: has x-amz-copy-source
        if "x-amz-copy-source" in headers:
            return _copy_object(bucket, key, headers)
        return _put_object(bucket, key, body, headers)

    # GetObject: GET /bucket/key
    if method == "GET" and key:
        return _get_object(bucket, key, headers)

    # HeadObject: HEAD /bucket/key
    if method == "HEAD" and key:
        return _head_object(bucket, key)

    # DeleteObject: DELETE /bucket/key
    if method == "DELETE" and key:
        return _delete_object(bucket, key)

    return _error("MethodNotAllowed", f"Method {method} not supported", 405)


def _list_buckets():
    root = Element("ListAllMyBucketsResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    owner = SubElement(root, "Owner")
    SubElement(owner, "ID").text = "owner-id"
    SubElement(owner, "DisplayName").text = "localstack"
    buckets_el = SubElement(root, "Buckets")
    for name, data in _buckets.items():
        b = SubElement(buckets_el, "Bucket")
        SubElement(b, "Name").text = name
        SubElement(b, "CreationDate").text = data["created"]
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "application/xml"}, body


def _create_bucket(name: str, body: bytes):
    if name in _buckets:
        return _error("BucketAlreadyOwnedByYou", f"Bucket {name} already exists", 409)
    _buckets[name] = {"created": now_iso(), "objects": {}}
    if PERSIST:
        os.makedirs(os.path.join(DATA_DIR, name), exist_ok=True)
    return 200, {"Content-Type": "application/xml", "Location": f"/{name}"}, b""


def _delete_bucket(name: str):
    bucket = _ensure_bucket(name)
    if bucket is None:
        return _error("NoSuchBucket", f"Bucket {name} does not exist", 404)
    if bucket["objects"]:
        return _error("BucketNotEmpty", "The bucket is not empty", 409)
    del _buckets[name]
    return 204, {}, b""


def _head_bucket(name: str):
    if name not in _buckets:
        return _error("NoSuchBucket", f"Bucket {name} does not exist", 404)
    return 200, {"Content-Type": "application/xml"}, b""


def _get_bucket_location(name: str):
    if name not in _buckets:
        return _error("NoSuchBucket", f"Bucket {name} does not exist", 404)
    root = Element("LocationConstraint", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    root.text = "us-east-1"
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "application/xml"}, body


def _put_object(bucket_name: str, key: str, body: bytes, headers: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        # Auto-create bucket (LocalStack behavior)
        _buckets[bucket_name] = {"created": now_iso(), "objects": {}}
        bucket = _buckets[bucket_name]

    etag = f'"{md5_hash(body)}"'
    content_type = headers.get("content-type", "application/octet-stream")

    # Extract user metadata (x-amz-meta-*)
    metadata = {}
    for k, v in headers.items():
        if k.lower().startswith("x-amz-meta-"):
            metadata[k] = v

    obj = {
        "body": body,
        "content_type": content_type,
        "etag": etag,
        "last_modified": now_iso(),
        "size": len(body),
        "metadata": metadata,
    }
    bucket["objects"][key] = obj

    if PERSIST:
        _persist_object(bucket_name, key, body)

    return 200, {"ETag": etag, "Content-Type": "application/xml"}, b""


def _get_object(bucket_name: str, key: str, headers: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _error("NoSuchBucket", f"Bucket {bucket_name} does not exist", 404)
    if key not in bucket["objects"]:
        return _error("NoSuchKey", f"The specified key does not exist: {key}", 404)

    obj = bucket["objects"][key]
    resp_headers = {
        "Content-Type": obj["content_type"],
        "ETag": obj["etag"],
        "Last-Modified": obj["last_modified"],
        "Content-Length": str(obj["size"]),
    }
    resp_headers.update(obj["metadata"])
    return 200, resp_headers, obj["body"]


def _head_object(bucket_name: str, key: str):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _error("NoSuchBucket", f"Bucket {bucket_name} does not exist", 404)
    if key not in bucket["objects"]:
        return 404, {}, b""

    obj = bucket["objects"][key]
    resp_headers = {
        "Content-Type": obj["content_type"],
        "ETag": obj["etag"],
        "Last-Modified": obj["last_modified"],
        "Content-Length": str(obj["size"]),
    }
    return 200, resp_headers, b""


def _delete_object(bucket_name: str, key: str):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _error("NoSuchBucket", f"Bucket {bucket_name} does not exist", 404)
    bucket["objects"].pop(key, None)
    return 204, {}, b""


def _copy_object(bucket_name: str, dest_key: str, headers: dict):
    source = headers.get("x-amz-copy-source", "").lstrip("/")
    src_parts = source.split("/", 1)
    if len(src_parts) < 2:
        return _error("InvalidArgument", "Invalid copy source", 400)

    src_bucket_name, src_key = src_parts
    src_bucket = _ensure_bucket(src_bucket_name)
    if src_bucket is None or src_key not in src_bucket["objects"]:
        return _error("NoSuchKey", "Source object not found", 404)

    dest_bucket = _ensure_bucket(bucket_name)
    if dest_bucket is None:
        _buckets[bucket_name] = {"created": now_iso(), "objects": {}}
        dest_bucket = _buckets[bucket_name]

    src_obj = src_bucket["objects"][src_key]
    new_etag = src_obj["etag"]
    dest_bucket["objects"][dest_key] = {
        "body": src_obj["body"],
        "content_type": src_obj["content_type"],
        "etag": new_etag,
        "last_modified": now_iso(),
        "size": src_obj["size"],
        "metadata": dict(src_obj["metadata"]),
    }

    root = Element("CopyObjectResult")
    SubElement(root, "LastModified").text = now_iso()
    SubElement(root, "ETag").text = new_etag
    body = tostring(root, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "application/xml"}, body


def _list_objects_v1(bucket_name: str, query_params: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _error("NoSuchBucket", f"Bucket {bucket_name} does not exist", 404)

    prefix = _qp(query_params, "prefix", "")
    delimiter = _qp(query_params, "delimiter", "")
    max_keys = int(_qp(query_params, "max-keys", "1000"))
    marker = _qp(query_params, "marker", "")

    keys = sorted(k for k in bucket["objects"] if k.startswith(prefix) and k > marker)
    common_prefixes = set()
    contents = []

    for k in keys:
        if delimiter:
            suffix = k[len(prefix):]
            idx = suffix.find(delimiter)
            if idx >= 0:
                common_prefixes.add(prefix + suffix[:idx + len(delimiter)])
                continue
        if len(contents) >= max_keys:
            break
        contents.append(k)

    root = Element("ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    SubElement(root, "Name").text = bucket_name
    SubElement(root, "Prefix").text = prefix
    SubElement(root, "MaxKeys").text = str(max_keys)
    SubElement(root, "IsTruncated").text = "false"

    for k in contents:
        obj = bucket["objects"][k]
        c = SubElement(root, "Contents")
        SubElement(c, "Key").text = k
        SubElement(c, "LastModified").text = obj["last_modified"]
        SubElement(c, "ETag").text = obj["etag"]
        SubElement(c, "Size").text = str(obj["size"])
        SubElement(c, "StorageClass").text = "STANDARD"

    for cp in sorted(common_prefixes):
        cpe = SubElement(root, "CommonPrefixes")
        SubElement(cpe, "Prefix").text = cp

    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "application/xml"}, body


def _list_objects_v2(bucket_name: str, query_params: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _error("NoSuchBucket", f"Bucket {bucket_name} does not exist", 404)

    prefix = _qp(query_params, "prefix", "")
    delimiter = _qp(query_params, "delimiter", "")
    max_keys = int(_qp(query_params, "max-keys", "1000"))
    continuation = _qp(query_params, "continuation-token", "")
    start_after = _qp(query_params, "start-after", "")

    effective_start = continuation or start_after
    keys = sorted(k for k in bucket["objects"] if k.startswith(prefix) and k > effective_start)
    common_prefixes = set()
    contents = []

    for k in keys:
        if delimiter:
            suffix = k[len(prefix):]
            idx = suffix.find(delimiter)
            if idx >= 0:
                common_prefixes.add(prefix + suffix[:idx + len(delimiter)])
                continue
        if len(contents) >= max_keys:
            break
        contents.append(k)

    root = Element("ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    SubElement(root, "Name").text = bucket_name
    SubElement(root, "Prefix").text = prefix
    SubElement(root, "MaxKeys").text = str(max_keys)
    SubElement(root, "KeyCount").text = str(len(contents))
    SubElement(root, "IsTruncated").text = "false"

    for k in contents:
        obj = bucket["objects"][k]
        c = SubElement(root, "Contents")
        SubElement(c, "Key").text = k
        SubElement(c, "LastModified").text = obj["last_modified"]
        SubElement(c, "ETag").text = obj["etag"]
        SubElement(c, "Size").text = str(obj["size"])
        SubElement(c, "StorageClass").text = "STANDARD"

    for cp in sorted(common_prefixes):
        cpe = SubElement(root, "CommonPrefixes")
        SubElement(cpe, "Prefix").text = cp

    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "application/xml"}, body


def _delete_objects(bucket_name: str, body: bytes):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _error("NoSuchBucket", f"Bucket {bucket_name} does not exist", 404)

    root = fromstring(body)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    deleted = []
    for obj_el in root.findall(".//Object", ns) or root.findall(".//Object"):
        key_el = obj_el.find("Key", ns) or obj_el.find("Key")
        if key_el is not None and key_el.text:
            bucket["objects"].pop(key_el.text, None)
            deleted.append(key_el.text)

    resp = Element("DeleteResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    for k in deleted:
        d = SubElement(resp, "Deleted")
        SubElement(d, "Key").text = k

    body_out = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(resp, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "application/xml"}, body_out


def _persist_object(bucket: str, key: str, data: bytes):
    try:
        fpath = os.path.join(DATA_DIR, bucket, key)
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        with open(fpath, "wb") as f:
            f.write(data)
    except Exception as e:
        logger.warning(f"Failed to persist S3 object: {e}")


def _error(code: str, message: str, status: int):
    root = Element("Error")
    SubElement(root, "Code").text = code
    SubElement(root, "Message").text = message
    SubElement(root, "RequestId").text = new_uuid()
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _qp(params: dict, key: str, default: str = "") -> str:
    val = params.get(key, [default])
    if isinstance(val, list):
        return val[0] if val else default
    return val
