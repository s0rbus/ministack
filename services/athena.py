"""
Athena Service Emulator.
JSON-based API via X-Amz-Target (AmazonAthena).
Uses DuckDB to actually execute SQL queries against S3 data (CSV/JSON/Parquet).
Supports: StartQueryExecution, GetQueryExecution, GetQueryResults,
          StopQueryExecution, ListQueryExecutions,
          CreateWorkGroup, DeleteWorkGroup, GetWorkGroup, ListWorkGroups,
          CreateNamedQuery, DeleteNamedQuery, GetNamedQuery, ListNamedQueries.
"""

import os
import json
import time
import logging
import threading

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("athena")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"
S3_DATA_DIR = os.environ.get("S3_DATA_DIR", "/tmp/localstack-data/s3")

_executions: dict = {}   # query_execution_id -> execution dict
_workgroups: dict = {
    "primary": {
        "Name": "primary",
        "State": "ENABLED",
        "Description": "Primary workgroup",
        "CreationTime": time.time(),
        "Configuration": {"ResultConfiguration": {"OutputLocation": "s3://athena-results/"}},
    }
}
_named_queries: dict = {}

# Try to import DuckDB
try:
    import duckdb
    _duckdb_available = True
    logger.info("Athena: DuckDB available — real SQL execution enabled")
except ImportError:
    _duckdb_available = False
    logger.warning("Athena: DuckDB not available — install with: pip install duckdb")


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "StartQueryExecution": _start_query_execution,
        "GetQueryExecution": _get_query_execution,
        "GetQueryResults": _get_query_results,
        "StopQueryExecution": _stop_query_execution,
        "ListQueryExecutions": _list_query_executions,
        "CreateWorkGroup": _create_workgroup,
        "DeleteWorkGroup": _delete_workgroup,
        "GetWorkGroup": _get_workgroup,
        "ListWorkGroups": _list_workgroups,
        "UpdateWorkGroup": _update_workgroup,
        "CreateNamedQuery": _create_named_query,
        "DeleteNamedQuery": _delete_named_query,
        "GetNamedQuery": _get_named_query,
        "ListNamedQueries": _list_named_queries,
        "BatchGetNamedQuery": _batch_get_named_query,
        "BatchGetQueryExecution": _batch_get_query_execution,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown Athena action: {action}", 400)
    return handler(data)


def _start_query_execution(data):
    query = data.get("QueryString", "")
    query_id = new_uuid()
    workgroup = data.get("WorkGroup", "primary")
    output_location = (
        data.get("ResultConfiguration", {}).get("OutputLocation")
        or _workgroups.get(workgroup, {}).get("Configuration", {}).get("ResultConfiguration", {}).get("OutputLocation", "s3://athena-results/")
    )
    db = data.get("QueryExecutionContext", {}).get("Database", "default")

    execution = {
        "QueryExecutionId": query_id,
        "Query": query,
        "StatementType": _detect_statement_type(query),
        "ResultConfiguration": {"OutputLocation": f"{output_location}{query_id}.csv"},
        "QueryExecutionContext": {"Database": db},
        "Status": {
            "State": "RUNNING",
            "SubmissionDateTime": time.time(),
            "CompletionDateTime": None,
            "StateChangeReason": "",
        },
        "Statistics": {"EngineExecutionTimeInMillis": 0, "DataScannedInBytes": 0},
        "WorkGroup": workgroup,
        "_results": None,
        "_error": None,
    }
    _executions[query_id] = execution

    # Run query in background thread
    thread = threading.Thread(target=_execute_query, args=(query_id, query, db), daemon=True)
    thread.start()

    return json_response({"QueryExecutionId": query_id})


def _execute_query(query_id, query, database):
    """Execute query using DuckDB in a background thread."""
    execution = _executions.get(query_id)
    if not execution:
        return

    start = time.time()
    try:
        if _duckdb_available:
            results = _run_duckdb(query, database)
        else:
            results = _mock_query_results(query)

        execution["_results"] = results
        execution["Status"]["State"] = "SUCCEEDED"
        execution["Statistics"]["EngineExecutionTimeInMillis"] = int((time.time() - start) * 1000)
        execution["Statistics"]["DataScannedInBytes"] = sum(
            len(str(row)) for row in results.get("rows", [])
        )
    except Exception as e:
        logger.error(f"Athena query {query_id} failed: {e}")
        execution["Status"]["State"] = "FAILED"
        execution["Status"]["StateChangeReason"] = str(e)
        execution["_error"] = str(e)

    execution["Status"]["CompletionDateTime"] = time.time()


def _run_duckdb(query, database):
    """Execute SQL via DuckDB, with S3 data directory available."""
    import duckdb

    conn = duckdb.connect(":memory:")

    # Rewrite s3:// paths to local filesystem paths before executing
    rewritten = _rewrite_s3_paths(query)

    try:
        result = conn.execute(rewritten)
        columns = [desc[0] for desc in result.description] if result.description else []
        rows = result.fetchall()
        conn.close()
        return {"columns": columns, "rows": [list(r) for r in rows]}
    except Exception as e:
        conn.close()
        raise e


def _rewrite_s3_paths(query):
    """Replace s3://bucket/key references with local file paths."""
    import re
    def replace_s3(match):
        s3_path = match.group(1)
        parts = s3_path.lstrip("s3://").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        local_path = os.path.join(S3_DATA_DIR, bucket, key)
        return f"'{local_path}'"
    return re.sub(r"'(s3://[^']+)'", replace_s3, query)


def _mock_query_results(query):
    """Return mock results when DuckDB is not available."""
    query_upper = query.strip().upper()
    if query_upper.startswith("SELECT"):
        return {"columns": ["result"], "rows": [["mock_value"]]}
    return {"columns": [], "rows": []}


def _detect_statement_type(query):
    q = query.strip().upper()
    if q.startswith("SELECT"):
        return "DML"
    if q.startswith(("CREATE", "DROP", "ALTER")):
        return "DDL"
    return "DML"


def _get_query_execution(data):
    query_id = data.get("QueryExecutionId")
    execution = _executions.get(query_id)
    if not execution:
        return error_response_json("InvalidRequestException", f"Query {query_id} not found", 400)
    return json_response({"QueryExecution": _execution_out(execution)})


def _get_query_results(data):
    query_id = data.get("QueryExecutionId")
    execution = _executions.get(query_id)
    if not execution:
        return error_response_json("InvalidRequestException", f"Query {query_id} not found", 400)

    if execution["Status"]["State"] != "SUCCEEDED":
        return error_response_json("InvalidRequestException",
            f"Query is in state {execution['Status']['State']}", 400)

    results = execution.get("_results") or {"columns": [], "rows": []}
    columns = results.get("columns", [])
    rows = results.get("rows", [])

    # Format as Athena ResultSet
    result_rows = []
    # Header row
    result_rows.append({"Data": [{"VarCharValue": col} for col in columns]})
    # Data rows
    for row in rows:
        result_rows.append({"Data": [{"VarCharValue": str(v) if v is not None else ""} for v in row]})

    return json_response({
        "QueryExecution": _execution_out(execution),
        "ResultSet": {
            "Rows": result_rows,
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {"Name": col, "Type": "varchar", "Label": col, "Nullable": "NULLABLE",
                     "CaseSensitive": False, "Precision": 0, "Scale": 0}
                    for col in columns
                ]
            },
        },
        "UpdateCount": 0,
    })


def _stop_query_execution(data):
    query_id = data.get("QueryExecutionId")
    execution = _executions.get(query_id)
    if execution and execution["Status"]["State"] == "RUNNING":
        execution["Status"]["State"] = "CANCELLED"
        execution["Status"]["CompletionDateTime"] = time.time()
    return json_response({})


def _list_query_executions(data):
    workgroup = data.get("WorkGroup", "primary")
    ids = [qid for qid, ex in _executions.items() if ex.get("WorkGroup") == workgroup]
    return json_response({"QueryExecutionIds": ids})


def _create_workgroup(data):
    name = data.get("Name")
    if name in _workgroups:
        return error_response_json("InvalidRequestException", f"WorkGroup {name} already exists", 400)
    _workgroups[name] = {
        "Name": name,
        "State": "ENABLED",
        "Description": data.get("Description", ""),
        "CreationTime": time.time(),
        "Configuration": data.get("Configuration", {}),
    }
    return json_response({})


def _delete_workgroup(data):
    name = data.get("WorkGroup")
    if name == "primary":
        return error_response_json("InvalidRequestException", "Cannot delete primary workgroup", 400)
    _workgroups.pop(name, None)
    return json_response({})


def _get_workgroup(data):
    name = data.get("WorkGroup")
    wg = _workgroups.get(name)
    if not wg:
        return error_response_json("InvalidRequestException", f"WorkGroup {name} not found", 400)
    return json_response({"WorkGroup": wg})


def _list_workgroups(data):
    return json_response({"WorkGroups": [{"Name": wg["Name"], "State": wg["State"]} for wg in _workgroups.values()]})


def _update_workgroup(data):
    name = data.get("WorkGroup")
    wg = _workgroups.get(name)
    if not wg:
        return error_response_json("InvalidRequestException", f"WorkGroup {name} not found", 400)
    if "ConfigurationUpdates" in data:
        wg["Configuration"].update(data["ConfigurationUpdates"])
    if "Description" in data:
        wg["Description"] = data["Description"]
    return json_response({})


def _create_named_query(data):
    query_id = new_uuid()
    _named_queries[query_id] = {
        "NamedQueryId": query_id,
        "Name": data.get("Name", ""),
        "Description": data.get("Description", ""),
        "Database": data.get("Database", "default"),
        "QueryString": data.get("QueryString", ""),
        "WorkGroup": data.get("WorkGroup", "primary"),
    }
    return json_response({"NamedQueryId": query_id})


def _delete_named_query(data):
    _named_queries.pop(data.get("NamedQueryId"), None)
    return json_response({})


def _get_named_query(data):
    query_id = data.get("NamedQueryId")
    nq = _named_queries.get(query_id)
    if not nq:
        return error_response_json("InvalidRequestException", f"Named query {query_id} not found", 400)
    return json_response({"NamedQuery": nq})


def _list_named_queries(data):
    workgroup = data.get("WorkGroup", "primary")
    ids = [qid for qid, nq in _named_queries.items() if nq.get("WorkGroup") == workgroup]
    return json_response({"NamedQueryIds": ids})


def _batch_get_named_query(data):
    ids = data.get("NamedQueryIds", [])
    queries = [_named_queries[qid] for qid in ids if qid in _named_queries]
    unprocessed = [qid for qid in ids if qid not in _named_queries]
    return json_response({"NamedQueries": queries, "UnprocessedNamedQueryIds": unprocessed})


def _batch_get_query_execution(data):
    ids = data.get("QueryExecutionIds", [])
    execs = [_execution_out(_executions[qid]) for qid in ids if qid in _executions]
    unprocessed = [qid for qid in ids if qid not in _executions]
    return json_response({"QueryExecutions": execs, "UnprocessedQueryExecutionIds": unprocessed})


def _execution_out(ex):
    return {k: v for k, v in ex.items() if not k.startswith("_")}
