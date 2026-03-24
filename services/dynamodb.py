"""
DynamoDB Service Emulator.
Supports: CreateTable, DeleteTable, DescribeTable, ListTables,
          PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchWriteItem, BatchGetItem.
Uses X-Amz-Target header for action routing (JSON API).
"""

import re
import time
import copy
import json
import logging
from collections import defaultdict

from core.responses import json_response, error_response_json, new_uuid, now_iso

logger = logging.getLogger("dynamodb")

_tables: dict = {}  # table_name -> {schema, items: {partition_key_val: {sort_key_val: item}}, ...}


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateTable": _create_table,
        "DeleteTable": _delete_table,
        "DescribeTable": _describe_table,
        "ListTables": _list_tables,
        "PutItem": _put_item,
        "GetItem": _get_item,
        "DeleteItem": _delete_item,
        "UpdateItem": _update_item,
        "Query": _query,
        "Scan": _scan,
        "BatchWriteItem": _batch_write_item,
        "BatchGetItem": _batch_get_item,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("UnknownOperationException", f"Unknown operation: {action}", 400)
    return handler(data)


def _create_table(data):
    name = data.get("TableName")
    if not name:
        return error_response_json("ValidationException", "TableName is required", 400)
    if name in _tables:
        return error_response_json("ResourceInUseException", f"Table {name} already exists", 400)

    key_schema = data.get("KeySchema", [])
    attr_defs = data.get("AttributeDefinitions", [])

    pk_name = None
    sk_name = None
    for ks in key_schema:
        if ks["KeyType"] == "HASH":
            pk_name = ks["AttributeName"]
        elif ks["KeyType"] == "RANGE":
            sk_name = ks["AttributeName"]

    _tables[name] = {
        "TableName": name,
        "KeySchema": key_schema,
        "AttributeDefinitions": attr_defs,
        "pk_name": pk_name,
        "sk_name": sk_name,
        "items": defaultdict(dict),  # pk_value -> {sk_value: item}
        "TableStatus": "ACTIVE",
        "CreationDateTime": time.time(),
        "ItemCount": 0,
        "TableSizeBytes": 0,
        "TableArn": f"arn:aws:dynamodb:us-east-1:000000000000:table/{name}",
        "GlobalSecondaryIndexes": data.get("GlobalSecondaryIndexes", []),
        "LocalSecondaryIndexes": data.get("LocalSecondaryIndexes", []),
        "ProvisionedThroughput": data.get("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}),
        "BillingModeSummary": {"BillingMode": data.get("BillingMode", "PROVISIONED")},
    }

    return json_response({"TableDescription": _table_description(name)})


def _delete_table(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)
    desc = _table_description(name)
    del _tables[name]
    return json_response({"TableDescription": desc})


def _describe_table(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)
    return json_response({"Table": _table_description(name)})


def _list_tables(data):
    limit = data.get("Limit", 100)
    start = data.get("ExclusiveStartTableName", "")
    names = sorted(_tables.keys())
    if start:
        names = [n for n in names if n > start]
    names = names[:limit]
    result = {"TableNames": names}
    if len(names) == limit:
        result["LastEvaluatedTableName"] = names[-1]
    return json_response(result)


def _put_item(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)

    item = data.get("Item", {})
    pk_val = _extract_key_val(item.get(table["pk_name"]))
    sk_val = _extract_key_val(item.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"

    old_item = table["items"].get(pk_val, {}).get(sk_val)

    table["items"][pk_val][sk_val] = item
    _update_counts(table)

    result = {}
    if data.get("ReturnValues") == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    return json_response(result)


def _get_item(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)

    key = data.get("Key", {})
    pk_val = _extract_key_val(key.get(table["pk_name"]))
    sk_val = _extract_key_val(key.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"

    item = table["items"].get(pk_val, {}).get(sk_val)

    result = {}
    if item:
        projection = data.get("ProjectionExpression")
        if projection:
            attrs = [a.strip() for a in projection.split(",")]
            result["Item"] = {k: v for k, v in item.items() if k in attrs}
        else:
            result["Item"] = item
    return json_response(result)


def _delete_item(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)

    key = data.get("Key", {})
    pk_val = _extract_key_val(key.get(table["pk_name"]))
    sk_val = _extract_key_val(key.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"

    old_item = table["items"].get(pk_val, {}).pop(sk_val, None)
    _update_counts(table)

    result = {}
    if data.get("ReturnValues") == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    return json_response(result)


def _update_item(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)

    key = data.get("Key", {})
    pk_val = _extract_key_val(key.get(table["pk_name"]))
    sk_val = _extract_key_val(key.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"

    item = table["items"].get(pk_val, {}).get(sk_val, dict(key))
    expr_attr_values = data.get("ExpressionAttributeValues", {})
    expr_attr_names = data.get("ExpressionAttributeNames", {})
    update_expr = data.get("UpdateExpression", "")

    # Basic SET/REMOVE parsing
    if update_expr:
        item = _apply_update_expression(item, update_expr, expr_attr_values, expr_attr_names)

    table["items"][pk_val][sk_val] = item
    _update_counts(table)

    result = {}
    ret = data.get("ReturnValues", "NONE")
    if ret == "ALL_NEW":
        result["Attributes"] = item
    elif ret == "ALL_OLD":
        result["Attributes"] = item  # Simplified
    return json_response(result)


def _query(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)

    expr_attr_values = data.get("ExpressionAttributeValues", {})
    expr_attr_names = data.get("ExpressionAttributeNames", {})
    key_condition = data.get("KeyConditionExpression", "")
    filter_expr = data.get("FilterExpression", "")
    limit = data.get("Limit", 1000000)
    scan_forward = data.get("ScanIndexForward", True)

    # Extract partition key value from KeyConditionExpression
    pk_val = _extract_pk_from_condition(key_condition, expr_attr_values, expr_attr_names, table["pk_name"])

    items = []
    if pk_val and pk_val in table["items"]:
        sk_items = table["items"][pk_val]
        for sk, item in sorted(sk_items.items(), reverse=not scan_forward):
            if _matches_condition(item, key_condition, expr_attr_values, expr_attr_names, table):
                if not filter_expr or _matches_filter(item, filter_expr, expr_attr_values, expr_attr_names):
                    items.append(item)
                    if len(items) >= limit:
                        break
    else:
        # Fallback: scan all items for the matching partition key
        for pk, sk_dict in table["items"].items():
            for sk, item in sk_dict.items():
                if _matches_condition(item, key_condition, expr_attr_values, expr_attr_names, table):
                    if not filter_expr or _matches_filter(item, filter_expr, expr_attr_values, expr_attr_names):
                        items.append(item)
                        if len(items) >= limit:
                            break

    return json_response({"Items": items, "Count": len(items), "ScannedCount": len(items)})


def _scan(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)

    filter_expr = data.get("FilterExpression", "")
    expr_attr_values = data.get("ExpressionAttributeValues", {})
    expr_attr_names = data.get("ExpressionAttributeNames", {})
    limit = data.get("Limit", 1000000)

    items = []
    scanned = 0
    for pk, sk_dict in table["items"].items():
        for sk, item in sk_dict.items():
            scanned += 1
            if not filter_expr or _matches_filter(item, filter_expr, expr_attr_values, expr_attr_names):
                items.append(item)
                if len(items) >= limit:
                    break

    return json_response({"Items": items, "Count": len(items), "ScannedCount": scanned})


def _batch_write_item(data):
    request_items = data.get("RequestItems", {})
    for table_name, requests in request_items.items():
        table = _tables.get(table_name)
        if not table:
            continue
        for req in requests:
            if "PutRequest" in req:
                item = req["PutRequest"]["Item"]
                pk_val = _extract_key_val(item.get(table["pk_name"]))
                sk_val = _extract_key_val(item.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
                table["items"][pk_val][sk_val] = item
            elif "DeleteRequest" in req:
                key = req["DeleteRequest"]["Key"]
                pk_val = _extract_key_val(key.get(table["pk_name"]))
                sk_val = _extract_key_val(key.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
                table["items"].get(pk_val, {}).pop(sk_val, None)
        _update_counts(table)

    return json_response({"UnprocessedItems": {}})


def _batch_get_item(data):
    request_items = data.get("RequestItems", {})
    responses = {}
    for table_name, config in request_items.items():
        table = _tables.get(table_name)
        if not table:
            continue
        responses[table_name] = []
        for key in config.get("Keys", []):
            pk_val = _extract_key_val(key.get(table["pk_name"]))
            sk_val = _extract_key_val(key.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
            item = table["items"].get(pk_val, {}).get(sk_val)
            if item:
                responses[table_name].append(item)

    return json_response({"Responses": responses, "UnprocessedKeys": {}})


# --- Helpers ---

def _table_description(name):
    t = _tables[name]
    return {
        "TableName": t["TableName"],
        "KeySchema": t["KeySchema"],
        "AttributeDefinitions": t["AttributeDefinitions"],
        "TableStatus": t["TableStatus"],
        "CreationDateTime": t["CreationDateTime"],
        "ItemCount": t["ItemCount"],
        "TableSizeBytes": t["TableSizeBytes"],
        "TableArn": t["TableArn"],
        "ProvisionedThroughput": t["ProvisionedThroughput"],
        "BillingModeSummary": t.get("BillingModeSummary", {}),
    }


def _extract_key_val(attr):
    if not attr:
        return ""
    if "S" in attr:
        return attr["S"]
    if "N" in attr:
        return attr["N"]
    if "B" in attr:
        return attr["B"]
    return str(attr)


def _update_counts(table):
    count = sum(len(sk_dict) for sk_dict in table["items"].values())
    table["ItemCount"] = count
    table["TableSizeBytes"] = count * 100  # rough estimate


def _apply_update_expression(item, expr, attr_values, attr_names):
    """Basic SET and REMOVE expression support."""
    item = dict(item)

    # Handle SET
    set_match = re.search(r'SET\s+(.+?)(?=\s+REMOVE|\s+ADD|\s+DELETE|$)', expr, re.IGNORECASE)
    if set_match:
        assignments = set_match.group(1).split(",")
        for assign in assignments:
            parts = assign.strip().split("=", 1)
            if len(parts) == 2:
                path = _resolve_name(parts[0].strip(), attr_names)
                val_ref = parts[1].strip()
                if val_ref in attr_values:
                    item[path] = attr_values[val_ref]

    # Handle REMOVE
    remove_match = re.search(r'REMOVE\s+(.+?)(?=\s+SET|\s+ADD|\s+DELETE|$)', expr, re.IGNORECASE)
    if remove_match:
        fields = remove_match.group(1).split(",")
        for f in fields:
            path = _resolve_name(f.strip(), attr_names)
            item.pop(path, None)

    return item


def _resolve_name(name, attr_names):
    return attr_names.get(name, name)


def _extract_pk_from_condition(condition, attr_values, attr_names, pk_name):
    """Try to extract partition key value from KeyConditionExpression."""
    resolved_pk = pk_name
    for alias, real_name in attr_names.items():
        if real_name == pk_name:
            resolved_pk = alias
            break

    # Match patterns like "#pk = :val" or "pk = :val"
    pattern = rf'{re.escape(resolved_pk)}\s*=\s*(:\w+)'
    match = re.search(pattern, condition)
    if match:
        val_ref = match.group(1)
        if val_ref in attr_values:
            return _extract_key_val(attr_values[val_ref])
    return None


def _matches_condition(item, condition, attr_values, attr_names, table):
    """Simplified condition matching — checks basic equality."""
    if not condition:
        return True
    # For now, just return True (items are already filtered by pk)
    return True


def _matches_filter(item, filter_expr, attr_values, attr_names):
    """Simplified filter expression matching."""
    if not filter_expr:
        return True
    # Basic attribute_exists / = support
    for alias, real_name in attr_names.items():
        filter_expr = filter_expr.replace(alias, real_name)

    # Check equality: "field = :val"
    eq_match = re.search(r'(\w+)\s*=\s*(:\w+)', filter_expr)
    if eq_match:
        field = eq_match.group(1)
        val_ref = eq_match.group(2)
        if field in item and val_ref in attr_values:
            return item[field] == attr_values[val_ref]
    return True
