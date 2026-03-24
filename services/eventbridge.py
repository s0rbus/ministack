"""
EventBridge Service Emulator.
JSON-based API via X-Amz-Target (AmazonEventBridge).
Supports: CreateEventBus, DeleteEventBus, ListEventBuses,
          PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule,
          PutTargets, RemoveTargets, ListTargetsByRule,
          PutEvents.
"""

import json
import time
import logging
import re

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("events")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_event_buses: dict = {
    "default": {
        "Name": "default",
        "Arn": f"arn:aws:events:{REGION}:{ACCOUNT_ID}:event-bus/default",
        "CreationTime": time.time(),
    }
}
_rules: dict = {}   # rule_name -> rule dict
_targets: dict = {} # rule_name -> [target, ...]
_events_log: list = []  # stored PutEvents entries


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateEventBus": _create_event_bus,
        "DeleteEventBus": _delete_event_bus,
        "ListEventBuses": _list_event_buses,
        "DescribeEventBus": _describe_event_bus,
        "PutRule": _put_rule,
        "DeleteRule": _delete_rule,
        "ListRules": _list_rules,
        "DescribeRule": _describe_rule,
        "EnableRule": _enable_rule,
        "DisableRule": _disable_rule,
        "PutTargets": _put_targets,
        "RemoveTargets": _remove_targets,
        "ListTargetsByRule": _list_targets_by_rule,
        "PutEvents": _put_events,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


def _create_event_bus(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    if name in _event_buses:
        return error_response_json("ResourceAlreadyExistsException", f"Event bus {name} already exists", 400)
    arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:event-bus/{name}"
    _event_buses[name] = {"Name": name, "Arn": arn, "CreationTime": time.time()}
    return json_response({"EventBusArn": arn})


def _delete_event_bus(data):
    name = data.get("Name")
    if name == "default":
        return error_response_json("ValidationException", "Cannot delete the default event bus", 400)
    _event_buses.pop(name, None)
    return json_response({})


def _list_event_buses(data):
    prefix = data.get("NamePrefix", "")
    buses = [b for n, b in _event_buses.items() if n.startswith(prefix)]
    return json_response({"EventBuses": buses})


def _describe_event_bus(data):
    name = data.get("Name", "default")
    bus = _event_buses.get(name)
    if not bus:
        return error_response_json("ResourceNotFoundException", f"Event bus {name} not found", 400)
    return json_response(bus)


def _put_rule(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    bus = data.get("EventBusName", "default")
    arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{name}"
    _rules[name] = {
        "Name": name,
        "Arn": arn,
        "EventBusName": bus,
        "ScheduleExpression": data.get("ScheduleExpression", ""),
        "EventPattern": data.get("EventPattern", ""),
        "State": data.get("State", "ENABLED"),
        "Description": data.get("Description", ""),
        "CreatedBy": ACCOUNT_ID,
    }
    return json_response({"RuleArn": arn})


def _delete_rule(data):
    name = data.get("Name")
    _rules.pop(name, None)
    _targets.pop(name, None)
    return json_response({})


def _list_rules(data):
    prefix = data.get("NamePrefix", "")
    bus = data.get("EventBusName", "default")
    rules = [r for n, r in _rules.items() if n.startswith(prefix) and r.get("EventBusName", "default") == bus]
    return json_response({"Rules": rules})


def _describe_rule(data):
    name = data.get("Name")
    rule = _rules.get(name)
    if not rule:
        return error_response_json("ResourceNotFoundException", f"Rule {name} not found", 400)
    return json_response(rule)


def _enable_rule(data):
    name = data.get("Name")
    if name in _rules:
        _rules[name]["State"] = "ENABLED"
    return json_response({})


def _disable_rule(data):
    name = data.get("Name")
    if name in _rules:
        _rules[name]["State"] = "DISABLED"
    return json_response({})


def _put_targets(data):
    rule = data.get("Rule")
    targets = data.get("Targets", [])
    if rule not in _targets:
        _targets[rule] = []
    existing_ids = {t["Id"] for t in _targets[rule]}
    for t in targets:
        if t["Id"] in existing_ids:
            _targets[rule] = [x for x in _targets[rule] if x["Id"] != t["Id"]]
        _targets[rule].append(t)
    return json_response({"FailedEntryCount": 0, "FailedEntries": []})


def _remove_targets(data):
    rule = data.get("Rule")
    ids = set(data.get("Ids", []))
    if rule in _targets:
        _targets[rule] = [t for t in _targets[rule] if t["Id"] not in ids]
    return json_response({"FailedEntryCount": 0, "FailedEntries": []})


def _list_targets_by_rule(data):
    rule = data.get("Rule")
    targets = _targets.get(rule, [])
    return json_response({"Targets": targets})


def _put_events(data):
    entries = data.get("Entries", [])
    results = []
    for entry in entries:
        event_id = new_uuid()
        _events_log.append({
            "EventId": event_id,
            "Source": entry.get("Source", ""),
            "DetailType": entry.get("DetailType", ""),
            "Detail": entry.get("Detail", "{}"),
            "EventBusName": entry.get("EventBusName", "default"),
            "Time": time.time(),
        })
        results.append({"EventId": event_id})
        logger.debug(f"EventBridge event: {entry.get('Source')} / {entry.get('DetailType')}")
    return json_response({"FailedEntryCount": 0, "Entries": results})
