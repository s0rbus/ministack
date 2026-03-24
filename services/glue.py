"""
Glue Service Emulator.
JSON-based API via X-Amz-Target (AWSGlue).
Supports full Data Catalog: Databases, Tables, Partitions, Connections, Crawlers, Jobs, JobRuns.
Job execution runs Python scripts via subprocess (same approach as Lambda).
"""

import os
import json
import time
import subprocess
import tempfile
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("glue")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_databases: dict = {}   # db_name -> {Name, Description, LocationUri, Parameters, CreateTime}
_tables: dict = {}      # db_name/table_name -> table dict
_partitions: dict = {}  # db_name/table_name -> [partition, ...]
_connections: dict = {}
_crawlers: dict = {}
_jobs: dict = {}
_job_runs: dict = {}    # job_name -> [run, ...]


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        # Databases
        "CreateDatabase": _create_database,
        "DeleteDatabase": _delete_database,
        "GetDatabase": _get_database,
        "GetDatabases": _get_databases,
        "UpdateDatabase": _update_database,
        # Tables
        "CreateTable": _create_table,
        "DeleteTable": _delete_table,
        "GetTable": _get_table,
        "GetTables": _get_tables,
        "UpdateTable": _update_table,
        "BatchDeleteTable": _batch_delete_table,
        # Partitions
        "CreatePartition": _create_partition,
        "DeletePartition": _delete_partition,
        "GetPartition": _get_partition,
        "GetPartitions": _get_partitions,
        "BatchCreatePartition": _batch_create_partition,
        # Connections
        "CreateConnection": _create_connection,
        "DeleteConnection": _delete_connection,
        "GetConnection": _get_connection,
        "GetConnections": _get_connections,
        # Crawlers
        "CreateCrawler": _create_crawler,
        "DeleteCrawler": _delete_crawler,
        "GetCrawler": _get_crawler,
        "GetCrawlers": _get_crawlers,
        "StartCrawler": _start_crawler,
        "StopCrawler": _stop_crawler,
        # Jobs
        "CreateJob": _create_job,
        "DeleteJob": _delete_job,
        "GetJob": _get_job,
        "GetJobs": _get_jobs,
        "UpdateJob": _update_job,
        "StartJobRun": _start_job_run,
        "GetJobRun": _get_job_run,
        "GetJobRuns": _get_job_runs,
        "BatchStopJobRun": _batch_stop_job_run,
        # Tags
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "GetTags": _get_tags,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown Glue action: {action}", 400)
    return handler(data)


# ---- Databases ----

def _create_database(data):
    db_input = data.get("DatabaseInput", {})
    name = db_input.get("Name")
    if not name:
        return error_response_json("InvalidInputException", "DatabaseInput.Name is required", 400)
    if name in _databases:
        return error_response_json("AlreadyExistsException", f"Database {name} already exists", 400)
    _databases[name] = {
        "Name": name,
        "Description": db_input.get("Description", ""),
        "LocationUri": db_input.get("LocationUri", ""),
        "Parameters": db_input.get("Parameters", {}),
        "CreateTime": time.time(),
        "CatalogId": ACCOUNT_ID,
    }
    return json_response({})


def _delete_database(data):
    name = data.get("Name")
    if name not in _databases:
        return error_response_json("EntityNotFoundException", f"Database {name} not found", 400)
    del _databases[name]
    # Remove all tables in this database
    keys_to_del = [k for k in _tables if k.startswith(f"{name}/")]
    for k in keys_to_del:
        del _tables[k]
    return json_response({})


def _get_database(data):
    name = data.get("Name")
    db = _databases.get(name)
    if not db:
        return error_response_json("EntityNotFoundException", f"Database {name} not found", 400)
    return json_response({"Database": db})


def _get_databases(data):
    return json_response({"DatabaseList": list(_databases.values())})


def _update_database(data):
    name = data.get("Name")
    db_input = data.get("DatabaseInput", {})
    if name not in _databases:
        return error_response_json("EntityNotFoundException", f"Database {name} not found", 400)
    _databases[name].update({k: v for k, v in db_input.items() if k != "Name"})
    return json_response({})


# ---- Tables ----

def _create_table(data):
    db_name = data.get("DatabaseName")
    table_input = data.get("TableInput", {})
    name = table_input.get("Name")
    key = f"{db_name}/{name}"
    if key in _tables:
        return error_response_json("AlreadyExistsException", f"Table {name} already exists", 400)
    _tables[key] = {
        "Name": name,
        "DatabaseName": db_name,
        "Description": table_input.get("Description", ""),
        "Owner": table_input.get("Owner", ""),
        "CreateTime": time.time(),
        "UpdateTime": time.time(),
        "LastAccessTime": time.time(),
        "StorageDescriptor": table_input.get("StorageDescriptor", {}),
        "PartitionKeys": table_input.get("PartitionKeys", []),
        "TableType": table_input.get("TableType", "EXTERNAL_TABLE"),
        "Parameters": table_input.get("Parameters", {}),
        "IsRegisteredWithLakeFormation": False,
        "CatalogId": ACCOUNT_ID,
    }
    return json_response({})


def _delete_table(data):
    db_name = data.get("DatabaseName")
    name = data.get("Name")
    key = f"{db_name}/{name}"
    _tables.pop(key, None)
    return json_response({})


def _get_table(data):
    db_name = data.get("DatabaseName")
    name = data.get("Name")
    key = f"{db_name}/{name}"
    table = _tables.get(key)
    if not table:
        return error_response_json("EntityNotFoundException", f"Table {name} not found in {db_name}", 400)
    return json_response({"Table": table})


def _get_tables(data):
    db_name = data.get("DatabaseName")
    tables = [t for k, t in _tables.items() if k.startswith(f"{db_name}/")]
    return json_response({"TableList": tables})


def _update_table(data):
    db_name = data.get("DatabaseName")
    table_input = data.get("TableInput", {})
    name = table_input.get("Name")
    key = f"{db_name}/{name}"
    if key not in _tables:
        return error_response_json("EntityNotFoundException", f"Table {name} not found", 400)
    _tables[key].update({k: v for k, v in table_input.items() if k != "Name"})
    _tables[key]["UpdateTime"] = time.time()
    return json_response({})


def _batch_delete_table(data):
    db_name = data.get("DatabaseName")
    names = data.get("TablesToDelete", [])
    errors = []
    for name in names:
        key = f"{db_name}/{name}"
        if key not in _tables:
            errors.append({"TableName": name, "ErrorDetail": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "Table not found"}})
        else:
            del _tables[key]
    return json_response({"Errors": errors})


# ---- Partitions ----

def _create_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    partition_input = data.get("PartitionInput", {})
    key = f"{db_name}/{table_name}"
    if key not in _partitions:
        _partitions[key] = []
    _partitions[key].append({**partition_input, "CreationTime": time.time(), "LastAccessTime": time.time()})
    return json_response({})


def _delete_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    values = data.get("PartitionValues", [])
    key = f"{db_name}/{table_name}"
    if key in _partitions:
        _partitions[key] = [p for p in _partitions[key] if p.get("Values") != values]
    return json_response({})


def _get_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    values = data.get("PartitionValues", [])
    key = f"{db_name}/{table_name}"
    for p in _partitions.get(key, []):
        if p.get("Values") == values:
            return json_response({"Partition": p})
    return error_response_json("EntityNotFoundException", "Partition not found", 400)


def _get_partitions(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    key = f"{db_name}/{table_name}"
    return json_response({"Partitions": _partitions.get(key, [])})


def _batch_create_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    key = f"{db_name}/{table_name}"
    if key not in _partitions:
        _partitions[key] = []
    for pi in data.get("PartitionInputList", []):
        _partitions[key].append({**pi, "CreationTime": time.time()})
    return json_response({"Errors": []})


# ---- Connections ----

def _create_connection(data):
    conn_input = data.get("ConnectionInput", {})
    name = conn_input.get("Name")
    _connections[name] = {**conn_input, "CreationTime": time.time(), "LastUpdatedTime": time.time()}
    return json_response({})


def _delete_connection(data):
    _connections.pop(data.get("ConnectionName"), None)
    return json_response({})


def _get_connection(data):
    name = data.get("Name")
    conn = _connections.get(name)
    if not conn:
        return error_response_json("EntityNotFoundException", f"Connection {name} not found", 400)
    return json_response({"Connection": conn})


def _get_connections(data):
    return json_response({"ConnectionList": list(_connections.values())})


# ---- Crawlers ----

def _create_crawler(data):
    name = data.get("Name")
    schedule = data.get("Schedule", "")
    # Schedule can be passed as a string (cron expression) or omitted
    schedule_struct = {"ScheduleExpression": schedule} if schedule else {}
    _crawlers[name] = {
        "Name": name,
        "Role": data.get("Role", ""),
        "DatabaseName": data.get("DatabaseName", ""),
        "Description": data.get("Description", ""),
        "Targets": data.get("Targets", {}),
        "Schedule": schedule_struct,
        "State": "READY",
        "CrawlElapsedTime": 0,
        "CreationTime": time.time(),
        "LastUpdated": time.time(),
        "LastCrawl": None,
        "Version": 1,
        "Configuration": data.get("Configuration", ""),
        "CrawlerSecurityConfiguration": data.get("CrawlerSecurityConfiguration", ""),
    }
    return json_response({})


def _delete_crawler(data):
    _crawlers.pop(data.get("Name"), None)
    return json_response({})


def _get_crawler(data):
    name = data.get("Name")
    crawler = _crawlers.get(name)
    if not crawler:
        return error_response_json("EntityNotFoundException", f"Crawler {name} not found", 400)
    return json_response({"Crawler": crawler})


def _get_crawlers(data):
    return json_response({"Crawlers": list(_crawlers.values())})


def _start_crawler(data):
    name = data.get("Name")
    if name not in _crawlers:
        return error_response_json("EntityNotFoundException", f"Crawler {name} not found", 400)
    _crawlers[name]["State"] = "RUNNING"
    # Immediately mark as ready (no actual crawling)
    _crawlers[name]["State"] = "READY"
    _crawlers[name]["LastCrawl"] = {"Status": "SUCCEEDED", "StartTime": time.time(), "EndTime": time.time()}
    return json_response({})


def _stop_crawler(data):
    name = data.get("Name")
    if name in _crawlers:
        _crawlers[name]["State"] = "READY"
    return json_response({})


# ---- Jobs ----

def _create_job(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidInputException", "Name is required", 400)
    _jobs[name] = {
        "Name": name,
        "Description": data.get("Description", ""),
        "Role": data.get("Role", ""),
        "Command": data.get("Command", {}),
        "DefaultArguments": data.get("DefaultArguments", {}),
        "Connections": data.get("Connections", {}),
        "MaxRetries": data.get("MaxRetries", 0),
        "Timeout": data.get("Timeout", 2880),
        "GlueVersion": data.get("GlueVersion", "3.0"),
        "NumberOfWorkers": data.get("NumberOfWorkers", 2),
        "WorkerType": data.get("WorkerType", "G.1X"),
        "CreatedOn": time.time(),
        "LastModifiedOn": time.time(),
    }
    _job_runs[name] = []
    return json_response({"Name": name})


def _delete_job(data):
    name = data.get("JobName")
    _jobs.pop(name, None)
    _job_runs.pop(name, None)
    return json_response({"JobName": name})


def _get_job(data):
    name = data.get("JobName")
    job = _jobs.get(name)
    if not job:
        return error_response_json("EntityNotFoundException", f"Job {name} not found", 400)
    return json_response({"Job": job})


def _get_jobs(data):
    return json_response({"Jobs": list(_jobs.values())})


def _update_job(data):
    name = data.get("JobName")
    job_update = data.get("JobUpdate", {})
    if name not in _jobs:
        return error_response_json("EntityNotFoundException", f"Job {name} not found", 400)
    _jobs[name].update(job_update)
    _jobs[name]["LastModifiedOn"] = time.time()
    return json_response({"JobName": name})


def _start_job_run(data):
    job_name = data.get("JobName")
    if job_name not in _jobs:
        return error_response_json("EntityNotFoundException", f"Job {job_name} not found", 400)

    run_id = new_uuid()
    job = _jobs[job_name]
    args = {**job.get("DefaultArguments", {}), **data.get("Arguments", {})}

    run = {
        "Id": run_id,
        "JobName": job_name,
        "StartedOn": time.time(),
        "LastModifiedOn": time.time(),
        "CompletedOn": None,
        "JobRunState": "RUNNING",
        "Arguments": args,
        "ErrorMessage": "",
        "ExecutionTime": 0,
        "Attempt": 0,
        "GlueVersion": job.get("GlueVersion", "3.0"),
    }

    # Try to actually run the script if it's a Python shell job
    script_location = job.get("Command", {}).get("ScriptLocation", "")
    if script_location and script_location.endswith(".py") and os.path.exists(script_location):
        try:
            env = dict(os.environ)
            env.update({k.lstrip("--"): v for k, v in args.items() if k.startswith("--")})
            proc = subprocess.run(
                ["python3", script_location],
                capture_output=True, text=True, timeout=job.get("Timeout", 300),
                env=env,
            )
            if proc.returncode == 0:
                run["JobRunState"] = "SUCCEEDED"
            else:
                run["JobRunState"] = "FAILED"
                run["ErrorMessage"] = proc.stderr[:500]
        except Exception as e:
            run["JobRunState"] = "FAILED"
            run["ErrorMessage"] = str(e)
    else:
        # Mock success
        run["JobRunState"] = "SUCCEEDED"

    run["CompletedOn"] = time.time()
    run["ExecutionTime"] = int(run["CompletedOn"] - run["StartedOn"])
    _job_runs[job_name].append(run)
    return json_response({"JobRunId": run_id})


def _get_job_run(data):
    job_name = data.get("JobName")
    run_id = data.get("RunId")
    for run in _job_runs.get(job_name, []):
        if run["Id"] == run_id:
            return json_response({"JobRun": run})
    return error_response_json("EntityNotFoundException", f"Job run {run_id} not found", 400)


def _get_job_runs(data):
    job_name = data.get("JobName")
    return json_response({"JobRuns": _job_runs.get(job_name, [])})


def _batch_stop_job_run(data):
    job_name = data.get("JobName")
    run_ids = data.get("JobRunIds", [])
    errors = []
    for run_id in run_ids:
        found = False
        for run in _job_runs.get(job_name, []):
            if run["Id"] == run_id:
                run["JobRunState"] = "STOPPED"
                found = True
                break
        if not found:
            errors.append({"JobName": job_name, "JobRunId": run_id, "ErrorDetail": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "Run not found"}})
    return json_response({"SuccessfulSubmissions": [{"JobName": job_name, "JobRunId": r} for r in run_ids if r not in [e["JobRunId"] for e in errors]], "Errors": errors})


# ---- Tags ----
_tags: dict = {}

def _tag_resource(data):
    arn = data.get("ResourceArn", "")
    _tags[arn] = {**_tags.get(arn, {}), **data.get("TagsToAdd", {})}
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceArn", "")
    for key in data.get("TagsToRemove", []):
        _tags.get(arn, {}).pop(key, None)
    return json_response({})


def _get_tags(data):
    arn = data.get("ResourceArn", "")
    return json_response({"Tags": _tags.get(arn, {})})
