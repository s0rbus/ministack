"""
SSM Parameter Store Emulator.
JSON-based API via X-Amz-Target (AmazonSSM).
Supports: PutParameter, GetParameter, GetParameters, GetParametersByPath,
          DeleteParameter, DeleteParameters, DescribeParameters.
"""

import json
import time
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("ssm")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_parameters: dict = {}  # name -> {Name, Value, Type, Version, ARN, LastModifiedDate, DataType}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "PutParameter": _put_parameter,
        "GetParameter": _get_parameter,
        "GetParameters": _get_parameters,
        "GetParametersByPath": _get_parameters_by_path,
        "DeleteParameter": _delete_parameter,
        "DeleteParameters": _delete_parameters,
        "DescribeParameters": _describe_parameters,
        "GetParameterHistory": _get_parameter_history,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


def _put_parameter(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)

    overwrite = data.get("Overwrite", False)
    if name in _parameters and not overwrite:
        return error_response_json("ParameterAlreadyExists", f"Parameter {name} already exists", 400)

    version = (_parameters[name]["Version"] + 1) if name in _parameters else 1
    arn = f"arn:aws:ssm:{REGION}:{ACCOUNT_ID}:parameter{name}"
    _parameters[name] = {
        "Name": name,
        "Value": data.get("Value", ""),
        "Type": data.get("Type", "String"),
        "Version": version,
        "ARN": arn,
        "LastModifiedDate": time.time(),
        "DataType": data.get("DataType", "text"),
        "Description": data.get("Description", ""),
        "Tier": data.get("Tier", "Standard"),
    }
    return json_response({"Version": version, "Tier": "Standard"})


def _get_parameter(data):
    name = data.get("Name")
    param = _parameters.get(name)
    if not param:
        return error_response_json("ParameterNotFound", f"Parameter {name} not found", 400)
    return json_response({"Parameter": _param_out(param)})


def _get_parameters(data):
    names = data.get("Names", [])
    params = []
    invalid = []
    for name in names:
        p = _parameters.get(name)
        if p:
            params.append(_param_out(p))
        else:
            invalid.append(name)
    return json_response({"Parameters": params, "InvalidParameters": invalid})


def _get_parameters_by_path(data):
    path = data.get("Path", "/")
    recursive = data.get("Recursive", False)
    results = []
    for name, param in _parameters.items():
        if recursive:
            if name.startswith(path):
                results.append(_param_out(param))
        else:
            # Non-recursive: only direct children (no further slashes after path)
            if name.startswith(path):
                suffix = name[len(path):]
                if "/" not in suffix.lstrip("/"):
                    results.append(_param_out(param))
    return json_response({"Parameters": results})


def _delete_parameter(data):
    name = data.get("Name")
    if name not in _parameters:
        return error_response_json("ParameterNotFound", f"Parameter {name} not found", 400)
    del _parameters[name]
    return json_response({})


def _delete_parameters(data):
    names = data.get("Names", [])
    deleted = []
    invalid = []
    for name in names:
        if name in _parameters:
            del _parameters[name]
            deleted.append(name)
        else:
            invalid.append(name)
    return json_response({"DeletedParameters": deleted, "InvalidParameters": invalid})


def _describe_parameters(data):
    filters = data.get("ParameterFilters", [])
    results = []
    for name, param in _parameters.items():
        results.append({
            "Name": param["Name"],
            "Type": param["Type"],
            "Version": param["Version"],
            "LastModifiedDate": param["LastModifiedDate"],
            "ARN": param["ARN"],
            "DataType": param["DataType"],
        })
    return json_response({"Parameters": results})


def _get_parameter_history(data):
    name = data.get("Name")
    param = _parameters.get(name)
    if not param:
        return error_response_json("ParameterNotFound", f"Parameter {name} not found", 400)
    return json_response({"Parameters": [_param_out(param)]})


def _param_out(param):
    return {
        "Name": param["Name"],
        "Type": param["Type"],
        "Value": param["Value"],
        "Version": param["Version"],
        "ARN": param["ARN"],
        "LastModifiedDate": param["LastModifiedDate"],
        "DataType": param["DataType"],
    }
