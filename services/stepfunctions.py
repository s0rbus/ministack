"""
Step Functions Service Emulator.
JSON-based API via X-Amz-Target (AWSStepFunctions).
Supports: CreateStateMachine, DeleteStateMachine, DescribeStateMachine, ListStateMachines,
          StartExecution, StopExecution, DescribeExecution, ListExecutions,
          GetExecutionHistory.
Executions are stored in-memory; no actual state machine logic is run.
"""

import json
import time
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("states")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_state_machines: dict = {}  # arn -> state machine dict
_executions: dict = {}       # arn -> execution dict


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateStateMachine": _create_state_machine,
        "DeleteStateMachine": _delete_state_machine,
        "DescribeStateMachine": _describe_state_machine,
        "UpdateStateMachine": _update_state_machine,
        "ListStateMachines": _list_state_machines,
        "StartExecution": _start_execution,
        "StopExecution": _stop_execution,
        "DescribeExecution": _describe_execution,
        "ListExecutions": _list_executions,
        "GetExecutionHistory": _get_execution_history,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


def _create_state_machine(data):
    name = data.get("name")
    if not name:
        return error_response_json("ValidationException", "name is required", 400)

    arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{name}"
    if arn in _state_machines:
        return error_response_json("StateMachineAlreadyExists", f"State machine {name} already exists", 400)

    _state_machines[arn] = {
        "stateMachineArn": arn,
        "name": name,
        "definition": data.get("definition", "{}"),
        "roleArn": data.get("roleArn", f"arn:aws:iam::{ACCOUNT_ID}:role/StepFunctionsRole"),
        "type": data.get("type", "STANDARD"),
        "creationDate": time.time(),
        "status": "ACTIVE",
        "loggingConfiguration": data.get("loggingConfiguration", {"level": "OFF", "includeExecutionData": False}),
        "tags": data.get("tags", []),
    }
    return json_response({"stateMachineArn": arn, "creationDate": time.time()})


def _delete_state_machine(data):
    arn = data.get("stateMachineArn")
    if arn not in _state_machines:
        return error_response_json("StateMachineDoesNotExist", f"State machine {arn} not found", 400)
    del _state_machines[arn]
    return json_response({})


def _describe_state_machine(data):
    arn = data.get("stateMachineArn")
    sm = _state_machines.get(arn)
    if not sm:
        return error_response_json("StateMachineDoesNotExist", f"State machine {arn} not found", 400)
    return json_response(sm)


def _update_state_machine(data):
    arn = data.get("stateMachineArn")
    sm = _state_machines.get(arn)
    if not sm:
        return error_response_json("StateMachineDoesNotExist", f"State machine {arn} not found", 400)
    if "definition" in data:
        sm["definition"] = data["definition"]
    if "roleArn" in data:
        sm["roleArn"] = data["roleArn"]
    return json_response({"updateDate": time.time()})


def _list_state_machines(data):
    machines = [
        {"stateMachineArn": sm["stateMachineArn"], "name": sm["name"],
         "type": sm["type"], "creationDate": sm["creationDate"]}
        for sm in _state_machines.values()
    ]
    return json_response({"stateMachines": machines})


def _start_execution(data):
    sm_arn = data.get("stateMachineArn")
    if sm_arn not in _state_machines:
        return error_response_json("StateMachineDoesNotExist", f"State machine {sm_arn} not found", 400)

    name = data.get("name") or new_uuid()
    exec_arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:execution:{_state_machines[sm_arn]['name']}:{name}"

    _executions[exec_arn] = {
        "executionArn": exec_arn,
        "stateMachineArn": sm_arn,
        "name": name,
        "status": "RUNNING",
        "startDate": time.time(),
        "stopDate": None,
        "input": data.get("input", "{}"),
        "output": None,
        "events": [
            {"id": 1, "type": "ExecutionStarted", "timestamp": time.time(),
             "executionStartedEventDetails": {"input": data.get("input", "{}")}},
        ],
    }
    logger.info(f"Step Functions execution started: {exec_arn}")
    return json_response({"executionArn": exec_arn, "startDate": time.time()})


def _stop_execution(data):
    exec_arn = data.get("executionArn")
    execution = _executions.get(exec_arn)
    if not execution:
        return error_response_json("ExecutionDoesNotExist", f"Execution {exec_arn} not found", 400)
    execution["status"] = "ABORTED"
    execution["stopDate"] = time.time()
    execution["events"].append({
        "id": len(execution["events"]) + 1,
        "type": "ExecutionAborted",
        "timestamp": time.time(),
        "executionAbortedEventDetails": {
            "error": data.get("error", ""),
            "cause": data.get("cause", ""),
        },
    })
    return json_response({"stopDate": execution["stopDate"]})


def _describe_execution(data):
    exec_arn = data.get("executionArn")
    execution = _executions.get(exec_arn)
    if not execution:
        return error_response_json("ExecutionDoesNotExist", f"Execution {exec_arn} not found", 400)
    result = dict(execution)
    result.pop("events", None)
    return json_response(result)


def _list_executions(data):
    sm_arn = data.get("stateMachineArn")
    status_filter = data.get("statusFilter")
    execs = []
    for exec_arn, ex in _executions.items():
        if sm_arn and ex["stateMachineArn"] != sm_arn:
            continue
        if status_filter and ex["status"] != status_filter:
            continue
        execs.append({
            "executionArn": ex["executionArn"],
            "stateMachineArn": ex["stateMachineArn"],
            "name": ex["name"],
            "status": ex["status"],
            "startDate": ex["startDate"],
            "stopDate": ex.get("stopDate"),
        })
    return json_response({"executions": execs})


def _get_execution_history(data):
    exec_arn = data.get("executionArn")
    execution = _executions.get(exec_arn)
    if not execution:
        return error_response_json("ExecutionDoesNotExist", f"Execution {exec_arn} not found", 400)
    return json_response({"events": execution["events"]})
