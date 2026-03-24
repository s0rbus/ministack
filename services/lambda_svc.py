"""
Lambda Service Emulator.
Supports: CreateFunction, DeleteFunction, GetFunction, ListFunctions,
          Invoke, UpdateFunctionCode, UpdateFunctionConfiguration.
Functions are stored in-memory. Invocation runs code in a subprocess (Python only) or returns mock results.
"""

import os
import json
import time
import base64
import zipfile
import tempfile
import subprocess
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("lambda")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_functions: dict = {}  # function_name -> {config, code_zip, code_dir}


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    """Route Lambda REST API requests."""

    # Parse path: /2015-03-31/functions[/name[/invocations|/code|/configuration]]
    parts = path.rstrip("/").split("/")
    # parts: ['', '2015-03-31', 'functions', ...]

    if len(parts) < 3:
        return error_response_json("InvalidRequest", "Invalid path", 400)

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # POST /2015-03-31/functions — CreateFunction
    if method == "POST" and len(parts) == 3:
        return _create_function(data)

    # GET /2015-03-31/functions — ListFunctions
    if method == "GET" and len(parts) == 3:
        return _list_functions()

    func_name = parts[3] if len(parts) > 3 else None

    if not func_name:
        return error_response_json("InvalidRequest", "Missing function name", 400)

    # POST /2015-03-31/functions/{name}/invocations — Invoke
    if method == "POST" and len(parts) >= 5 and parts[4] == "invocations":
        return _invoke(func_name, data, headers)

    # GET /2015-03-31/functions/{name} — GetFunction
    if method == "GET" and len(parts) == 4:
        return _get_function(func_name)

    # GET /2015-03-31/functions/{name}/configuration
    if method == "GET" and len(parts) >= 5 and parts[4] == "configuration":
        return _get_function_config(func_name)

    # DELETE /2015-03-31/functions/{name}
    if method == "DELETE" and len(parts) == 4:
        return _delete_function(func_name)

    # PUT /2015-03-31/functions/{name}/code
    if method == "PUT" and len(parts) >= 5 and parts[4] == "code":
        return _update_code(func_name, data)

    # PUT /2015-03-31/functions/{name}/configuration
    if method == "PUT" and len(parts) >= 5 and parts[4] == "configuration":
        return _update_config(func_name, data)

    return error_response_json("InvalidRequest", f"Unhandled Lambda path: {path}", 400)


def _create_function(data):
    name = data.get("FunctionName")
    if not name:
        return error_response_json("InvalidParameterValueException", "FunctionName required", 400)
    if name in _functions:
        return error_response_json("ResourceConflictException", f"Function {name} already exists", 409)

    func_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
    config = {
        "FunctionName": name,
        "FunctionArn": func_arn,
        "Runtime": data.get("Runtime", "python3.9"),
        "Role": data.get("Role", f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-role"),
        "Handler": data.get("Handler", "index.handler"),
        "CodeSize": 0,
        "Description": data.get("Description", ""),
        "Timeout": data.get("Timeout", 3),
        "MemorySize": data.get("MemorySize", 128),
        "LastModified": time.strftime("%Y-%m-%dT%H:%M:%S.000+0000"),
        "Version": "$LATEST",
        "State": "Active",
        "PackageType": "Zip",
        "Environment": data.get("Environment", {"Variables": {}}),
        "Layers": data.get("Layers", []),
    }

    code_zip = None
    code_data = data.get("Code", {})
    if "ZipFile" in code_data:
        code_zip = base64.b64decode(code_data["ZipFile"])
        config["CodeSize"] = len(code_zip)
        config["CodeSha256"] = base64.b64encode(b"fakehash").decode()

    _functions[name] = {"config": config, "code_zip": code_zip}

    return json_response(config, 201)


def _get_function(name):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    func = _functions[name]
    return json_response({
        "Configuration": func["config"],
        "Code": {"RepositoryType": "S3", "Location": ""},
    })


def _get_function_config(name):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    return json_response(_functions[name]["config"])


def _list_functions():
    funcs = [f["config"] for f in _functions.values()]
    return json_response({"Functions": funcs})


def _delete_function(name):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    del _functions[name]
    return 204, {}, b""


def _update_code(name, data):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    if "ZipFile" in data:
        code_zip = base64.b64decode(data["ZipFile"])
        _functions[name]["code_zip"] = code_zip
        _functions[name]["config"]["CodeSize"] = len(code_zip)
    _functions[name]["config"]["LastModified"] = time.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    return json_response(_functions[name]["config"])


def _update_config(name, data):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    config = _functions[name]["config"]
    for key in ["Runtime", "Handler", "Description", "Timeout", "MemorySize", "Role", "Environment", "Layers"]:
        if key in data:
            config[key] = data[key]
    config["LastModified"] = time.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    return json_response(config)


def _invoke(name, event, headers):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)

    func = _functions[name]
    invocation_type = headers.get("x-amz-invocation-type", "RequestResponse")

    if invocation_type == "Event":
        # Async — just acknowledge
        return 202, {"X-Amz-Function-Error": ""}, b""

    # Try to actually execute the function if we have the code
    result = _execute_function(func, event)

    resp_headers = {
        "Content-Type": "application/json",
        "X-Amz-Executed-Version": "$LATEST",
    }
    if result.get("error"):
        resp_headers["X-Amz-Function-Error"] = "Unhandled"

    return 200, resp_headers, json.dumps(result.get("body", {})).encode("utf-8")


def _execute_function(func, event):
    """Attempt to execute a Python Lambda function."""
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response — no code deployed"}}

    handler = func["config"]["Handler"]
    runtime = func["config"]["Runtime"]
    env_vars = func["config"].get("Environment", {}).get("Variables", {})

    if not runtime.startswith("python"):
        return {"body": {"statusCode": 200, "body": f"Mock response — {runtime} not supported for local execution"}}

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "code.zip")
            with open(zip_path, "wb") as f:
                f.write(code_zip)

            code_dir = os.path.join(tmpdir, "code")
            os.makedirs(code_dir)
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(code_dir)

            module_name, func_name = handler.rsplit(".", 1)
            event_json = json.dumps(event)

            script = f"""
import sys, json
sys.path.insert(0, '{code_dir}')
import {module_name}
event = json.loads('''{event_json}''')
context = type('Context', (), {{'function_name': '{func["config"]["FunctionName"]}', 'memory_limit_in_mb': {func["config"]["MemorySize"]}, 'invoked_function_arn': '{func["config"]["FunctionArn"]}', 'aws_request_id': '{new_uuid()}'}})()
result = {module_name}.{func_name}(event, context)
print(json.dumps(result))
"""
            env = dict(os.environ)
            env.update(env_vars)

            proc = subprocess.run(
                ["python3", "-c", script],
                capture_output=True, text=True, timeout=func["config"]["Timeout"],
                env=env,
            )

            if proc.returncode == 0:
                try:
                    return {"body": json.loads(proc.stdout.strip())}
                except json.JSONDecodeError:
                    return {"body": proc.stdout.strip()}
            else:
                return {"body": {"errorMessage": proc.stderr.strip(), "errorType": "RuntimeError"}, "error": True}

    except subprocess.TimeoutExpired:
        return {"body": {"errorMessage": "Task timed out", "errorType": "TimeoutError"}, "error": True}
    except Exception as e:
        logger.error(f"Lambda execution error: {e}")
        return {"body": {"errorMessage": str(e), "errorType": type(e).__name__}, "error": True}
