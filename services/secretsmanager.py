"""
SecretsManager Service Emulator.
JSON-based API via X-Amz-Target.
Supports: CreateSecret, GetSecretValue, ListSecrets, DeleteSecret,
          UpdateSecret, DescribeSecret, PutSecretValue.
"""

import json
import time
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("secretsmanager")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_secrets: dict = {}  # name -> {arn, name, secret_string, secret_binary, version_id, created, updated, versions}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateSecret": _create_secret,
        "GetSecretValue": _get_secret_value,
        "ListSecrets": _list_secrets,
        "DeleteSecret": _delete_secret,
        "UpdateSecret": _update_secret,
        "DescribeSecret": _describe_secret,
        "PutSecretValue": _put_secret_value,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidRequestException", f"Unknown action: {action}", 400)
    return handler(data)


def _create_secret(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidParameterException", "Name required", 400)
    if name in _secrets:
        return error_response_json("ResourceExistsException", f"Secret {name} already exists", 400)

    arn = f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_ID}:secret:{name}-{new_uuid()[:6]}"
    version_id = new_uuid()
    _secrets[name] = {
        "ARN": arn, "Name": name,
        "SecretString": data.get("SecretString", ""),
        "SecretBinary": data.get("SecretBinary"),
        "VersionId": version_id,
        "CreatedDate": time.time(),
        "LastChangedDate": time.time(),
        "VersionIdsToStages": {version_id: ["AWSCURRENT"]},
        "Description": data.get("Description", ""),
    }
    return json_response({"ARN": arn, "Name": name, "VersionId": version_id})


def _get_secret_value(data):
    name = data.get("SecretId")
    secret = _secrets.get(name)
    if not secret:
        # Try by ARN
        for s in _secrets.values():
            if s["ARN"] == name:
                secret = s
                break
    if not secret:
        return error_response_json("ResourceNotFoundException", f"Secret {name} not found", 400)

    result = {
        "ARN": secret["ARN"], "Name": secret["Name"],
        "VersionId": secret["VersionId"],
        "VersionStages": ["AWSCURRENT"],
        "CreatedDate": secret["CreatedDate"],
    }
    if secret.get("SecretString"):
        result["SecretString"] = secret["SecretString"]
    if secret.get("SecretBinary"):
        result["SecretBinary"] = secret["SecretBinary"]
    return json_response(result)


def _list_secrets(data):
    secrets_list = []
    for s in _secrets.values():
        secrets_list.append({
            "ARN": s["ARN"], "Name": s["Name"],
            "LastChangedDate": s["LastChangedDate"],
            "Description": s.get("Description", ""),
        })
    return json_response({"SecretList": secrets_list})


def _delete_secret(data):
    name = data.get("SecretId")
    secret = _secrets.pop(name, None)
    if not secret:
        return error_response_json("ResourceNotFoundException", f"Secret {name} not found", 400)
    return json_response({"ARN": secret["ARN"], "Name": secret["Name"], "DeletionDate": time.time()})


def _update_secret(data):
    name = data.get("SecretId")
    secret = _secrets.get(name)
    if not secret:
        return error_response_json("ResourceNotFoundException", f"Secret {name} not found", 400)
    if "SecretString" in data:
        secret["SecretString"] = data["SecretString"]
    if "SecretBinary" in data:
        secret["SecretBinary"] = data["SecretBinary"]
    version_id = new_uuid()
    secret["VersionId"] = version_id
    secret["LastChangedDate"] = time.time()
    return json_response({"ARN": secret["ARN"], "Name": name, "VersionId": version_id})


def _describe_secret(data):
    name = data.get("SecretId")
    secret = _secrets.get(name)
    if not secret:
        return error_response_json("ResourceNotFoundException", f"Secret {name} not found", 400)
    return json_response({
        "ARN": secret["ARN"], "Name": secret["Name"],
        "Description": secret.get("Description", ""),
        "LastChangedDate": secret["LastChangedDate"],
        "CreatedDate": secret["CreatedDate"],
        "VersionIdsToStages": secret["VersionIdsToStages"],
    })


def _put_secret_value(data):
    name = data.get("SecretId")
    secret = _secrets.get(name)
    if not secret:
        return error_response_json("ResourceNotFoundException", f"Secret {name} not found", 400)
    if "SecretString" in data:
        secret["SecretString"] = data["SecretString"]
    if "SecretBinary" in data:
        secret["SecretBinary"] = data["SecretBinary"]
    version_id = new_uuid()
    secret["VersionId"] = version_id
    secret["LastChangedDate"] = time.time()
    secret["VersionIdsToStages"][version_id] = ["AWSCURRENT"]
    return json_response({"ARN": secret["ARN"], "Name": name, "VersionId": version_id, "VersionStages": ["AWSCURRENT"]})
