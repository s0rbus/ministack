"""
CloudWatch Metrics Service Emulator.
Supports legacy Query API (form-encoded), smithy-rpc-v2-cbor (botocore 1.42+),
and JSON/X-Amz-Target protocol (botocore 1.42 with endpoint_url).
Operations: PutMetricData, GetMetricStatistics, ListMetrics,
            PutMetricAlarm, DescribeAlarms, DeleteAlarms,
            EnableAlarmActions, DisableAlarmActions, SetAlarmState.
"""

import re
import json
import time
import logging
from urllib.parse import parse_qs
from collections import defaultdict

from core.responses import new_uuid

logger = logging.getLogger("cloudwatch")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_metrics: dict = defaultdict(list)
_alarms: dict = {}


async def handle_request(method, path, headers, body, query_params):
    content_type = headers.get("content-type", "")
    target = headers.get("x-amz-target", "")
    is_cbor = "cbor" in content_type or "cbor" in headers.get("smithy-protocol", "")
    is_json = (not is_cbor) and ("json" in content_type or bool(target))

    params = dict(query_params)
    cbor_data = {}

    if body:
        if is_cbor:
            try:
                import cbor2
                cbor_data = cbor2.loads(body) or {}
            except Exception as e:
                logger.error(f"CBOR decode error: {e}")
                cbor_data = {}
        elif is_json:
            try:
                cbor_data = json.loads(body) or {}
            except Exception as e:
                logger.error(f"JSON decode error: {e}")
                cbor_data = {}
        else:
            for k, v in parse_qs(body.decode("utf-8", errors="replace")).items():
                params[k] = v

    # Extract action: X-Amz-Target first, then URL path, then form params
    action = ""
    if target and "." in target:
        action = target.split(".")[-1]
    if not action:
        m = re.search(r"/operation/([^/?]+)", path)
        if m:
            action = m.group(1)
    if not action:
        action = _p(params, "Action")

    handlers = {
        "PutMetricData": _put_metric_data,
        "GetMetricStatistics": _get_metric_statistics,
        "ListMetrics": _list_metrics,
        "PutMetricAlarm": _put_metric_alarm,
        "DescribeAlarms": _describe_alarms,
        "DeleteAlarms": _delete_alarms,
        "EnableAlarmActions": _enable_alarm_actions,
        "DisableAlarmActions": _disable_alarm_actions,
        "SetAlarmState": _set_alarm_state,
    }

    handler = handlers.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown action: {action}", 400, is_cbor or is_json)
    return handler(params, cbor_data, is_cbor, is_json)


def _put_metric_data(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        namespace = cbor_data.get("Namespace", "")
        for md in cbor_data.get("MetricData", []):
            metric_name = md.get("MetricName", "")
            value = float(md.get("Value", 0))
            unit = md.get("Unit", "None")
            ts = time.time()
            dims = {d["Name"]: d["Value"] for d in md.get("Dimensions", [])}
            _metrics[(namespace, metric_name, _dims_key(dims))].append(
                {"Timestamp": ts, "Value": value, "Unit": unit, "Dimensions": dims}
            )
    else:
        namespace = _p(params, "Namespace")
        i = 1
        while _p(params, f"MetricData.member.{i}.MetricName"):
            metric_name = _p(params, f"MetricData.member.{i}.MetricName")
            value = float(_p(params, f"MetricData.member.{i}.Value") or "0")
            unit = _p(params, f"MetricData.member.{i}.Unit") or "None"
            ts_str = _p(params, f"MetricData.member.{i}.Timestamp")
            ts = float(ts_str) if ts_str else time.time()
            dims = {}
            j = 1
            while _p(params, f"MetricData.member.{i}.Dimensions.member.{j}.Name"):
                dims[_p(params, f"MetricData.member.{i}.Dimensions.member.{j}.Name")] = \
                    _p(params, f"MetricData.member.{i}.Dimensions.member.{j}.Value")
                j += 1
            _metrics[(namespace, metric_name, _dims_key(dims))].append(
                {"Timestamp": ts, "Value": value, "Unit": unit, "Dimensions": dims}
            )
            i += 1

    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "PutMetricDataResponse", "")


def _list_metrics(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        namespace = cbor_data.get("Namespace")
        metric_name = cbor_data.get("MetricName")
    else:
        namespace = _p(params, "Namespace")
        metric_name = _p(params, "MetricName")

    seen = set()
    result = []
    for (ns, mn, dk), points in _metrics.items():
        if namespace and ns != namespace:
            continue
        if metric_name and mn != metric_name:
            continue
        key = (ns, mn, dk)
        if key in seen:
            continue
        seen.add(key)
        dims = [{"Name": k, "Value": v} for k, v in (points[0].get("Dimensions", {}) if points else {}).items()]
        result.append({"Namespace": ns, "MetricName": mn, "Dimensions": dims})

    if is_cbor:
        return _cbor_ok({"Metrics": result})
    if is_json:
        return _json_ok({"Metrics": result})

    members = ""
    for item in result:
        dims_xml = "".join(f"<member><Name>{d['Name']}</Name><Value>{d['Value']}</Value></member>"
                           for d in item["Dimensions"])
        members += f"<member><Namespace>{item['Namespace']}</Namespace><MetricName>{item['MetricName']}</MetricName><Dimensions>{dims_xml}</Dimensions></member>"
    return _xml(200, "ListMetricsResponse", f"<ListMetricsResult><Metrics>{members}</Metrics></ListMetricsResult>")


def _get_metric_statistics(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        namespace = cbor_data.get("Namespace")
        metric_name = cbor_data.get("MetricName")
        period = int(cbor_data.get("Period") or 60)
    else:
        namespace = _p(params, "Namespace")
        metric_name = _p(params, "MetricName")
        period = int(_p(params, "Period") or 60)

    all_points = []
    for (ns, mn, dk), points in _metrics.items():
        if ns == namespace and mn == metric_name:
            all_points.extend(points)

    if is_cbor or is_json:
        datapoints = []
        buckets = defaultdict(list)
        for pt in all_points:
            buckets[int(pt["Timestamp"] // period) * period].append(pt["Value"])
        for ts, values in sorted(buckets.items()):
            datapoints.append({
                "Timestamp": ts, "SampleCount": len(values),
                "Sum": sum(values), "Average": sum(values) / len(values),
                "Minimum": min(values), "Maximum": max(values),
                "Unit": all_points[0]["Unit"] if all_points else "None",
            })
        if is_cbor:
            return _cbor_ok({"Datapoints": datapoints, "Label": metric_name})
        return _json_ok({"Datapoints": datapoints, "Label": metric_name})

    if not all_points:
        return _xml(200, "GetMetricStatisticsResponse",
                    f"<GetMetricStatisticsResult><Datapoints/><Label>{metric_name}</Label></GetMetricStatisticsResult>")
    buckets = defaultdict(list)
    for pt in all_points:
        buckets[int(pt["Timestamp"] // period) * period].append(pt["Value"])
    dps = ""
    for ts, values in sorted(buckets.items()):
        dps += (f"<member><Timestamp>{ts}</Timestamp><SampleCount>{len(values)}</SampleCount>"
                f"<Sum>{sum(values)}</Sum><Average>{sum(values)/len(values):.6f}</Average>"
                f"<Minimum>{min(values)}</Minimum><Maximum>{max(values)}</Maximum>"
                f"<Unit>{all_points[0]['Unit']}</Unit></member>")
    return _xml(200, "GetMetricStatisticsResponse",
                f"<GetMetricStatisticsResult><Datapoints>{dps}</Datapoints><Label>{metric_name}</Label></GetMetricStatisticsResult>")


def _put_metric_alarm(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        name = cbor_data.get("AlarmName", "")
        alarm = {
            "AlarmName": name,
            "AlarmArn": f"arn:aws:cloudwatch:{REGION}:{ACCOUNT_ID}:alarm:{name}",
            "AlarmDescription": cbor_data.get("AlarmDescription"),
            "MetricName": cbor_data.get("MetricName"),
            "Namespace": cbor_data.get("Namespace"),
            "Statistic": cbor_data.get("Statistic", "Average"),
            "Period": int(cbor_data.get("Period", 60)),
            "EvaluationPeriods": int(cbor_data.get("EvaluationPeriods", 1)),
            "Threshold": float(cbor_data.get("Threshold", 0)),
            "ComparisonOperator": cbor_data.get("ComparisonOperator"),
            "StateValue": "OK",
            "StateReason": "Threshold Crossed",
            "ActionsEnabled": cbor_data.get("ActionsEnabled", True),
        }
    else:
        name = _p(params, "AlarmName")
        alarm = {
            "AlarmName": name,
            "AlarmArn": f"arn:aws:cloudwatch:{REGION}:{ACCOUNT_ID}:alarm:{name}",
            "AlarmDescription": _p(params, "AlarmDescription"),
            "MetricName": _p(params, "MetricName"),
            "Namespace": _p(params, "Namespace"),
            "Statistic": _p(params, "Statistic") or "Average",
            "Period": int(_p(params, "Period") or "60"),
            "EvaluationPeriods": int(_p(params, "EvaluationPeriods") or "1"),
            "Threshold": float(_p(params, "Threshold") or "0"),
            "ComparisonOperator": _p(params, "ComparisonOperator"),
            "StateValue": "OK",
            "StateReason": "Threshold Crossed",
            "ActionsEnabled": _p(params, "ActionsEnabled") != "false",
        }
    _alarms[name] = alarm
    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "PutMetricAlarmResponse", "")


def _describe_alarms(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        names = cbor_data.get("AlarmNames", [])
        prefix = cbor_data.get("AlarmNamePrefix")
        state = cbor_data.get("StateValue")
    else:
        names = [_p(params, f"AlarmNames.member.{i}") for i in range(1, 100)
                 if _p(params, f"AlarmNames.member.{i}")]
        prefix = _p(params, "AlarmNamePrefix")
        state = _p(params, "StateValue")

    result = []
    for name, alarm in _alarms.items():
        if names and name not in names:
            continue
        if prefix and not name.startswith(prefix):
            continue
        if state and alarm["StateValue"] != state:
            continue
        result.append(alarm)

    if is_cbor:
        return _cbor_ok({"MetricAlarms": result})
    if is_json:
        return _json_ok({"MetricAlarms": result})

    members = "".join(
        f"<member><AlarmName>{a['AlarmName']}</AlarmName><AlarmArn>{a['AlarmArn']}</AlarmArn>"
        f"<StateValue>{a['StateValue']}</StateValue><MetricName>{a['MetricName']}</MetricName>"
        f"<Namespace>{a['Namespace']}</Namespace><Threshold>{a['Threshold']}</Threshold>"
        f"<ComparisonOperator>{a['ComparisonOperator']}</ComparisonOperator></member>"
        for a in result
    )
    return _xml(200, "DescribeAlarmsResponse",
                f"<DescribeAlarmsResult><MetricAlarms>{members}</MetricAlarms></DescribeAlarmsResult>")


def _delete_alarms(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        for name in cbor_data.get("AlarmNames", []):
            _alarms.pop(name, None)
    else:
        i = 1
        while _p(params, f"AlarmNames.member.{i}"):
            _alarms.pop(_p(params, f"AlarmNames.member.{i}"), None)
            i += 1
    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "DeleteAlarmsResponse", "")


def _enable_alarm_actions(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        names = cbor_data.get("AlarmNames", [])
    else:
        names = [_p(params, f"AlarmNames.member.{i}") for i in range(1, 100)
                 if _p(params, f"AlarmNames.member.{i}")]
    for name in names:
        if name in _alarms:
            _alarms[name]["ActionsEnabled"] = True
    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "EnableAlarmActionsResponse", "")


def _disable_alarm_actions(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        names = cbor_data.get("AlarmNames", [])
    else:
        names = [_p(params, f"AlarmNames.member.{i}") for i in range(1, 100)
                 if _p(params, f"AlarmNames.member.{i}")]
    for name in names:
        if name in _alarms:
            _alarms[name]["ActionsEnabled"] = False
    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "DisableAlarmActionsResponse", "")


def _set_alarm_state(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        name = cbor_data.get("AlarmName", "")
        state = cbor_data.get("StateValue", "")
        reason = cbor_data.get("StateReason", "")
    else:
        name = _p(params, "AlarmName")
        state = _p(params, "StateValue")
        reason = _p(params, "StateReason")
    if name in _alarms:
        _alarms[name]["StateValue"] = state
        _alarms[name]["StateReason"] = reason
    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "SetAlarmStateResponse", "")


# --- Helpers ---

def _dims_key(dims: dict) -> str:
    return "|".join(f"{k}={v}" for k, v in sorted(dims.items()))


def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _cbor_ok(data: dict):
    try:
        import cbor2
        body = cbor2.dumps(data)
    except Exception:
        body = json.dumps(data).encode()
    return 200, {"Content-Type": "application/cbor", "smithy-protocol": "rpc-v2-cbor"}, body


def _json_ok(data: dict):
    return 200, {"Content-Type": "application/json"}, json.dumps(data).encode()


def _xml(status, root_tag, inner):
    body = (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<{root_tag} xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">\n'
            f'    {inner}\n'
            f'    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>\n'
            f'</{root_tag}>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status, is_cbor=False):
    if is_cbor:
        try:
            import cbor2
            body = cbor2.dumps({"__type": code, "message": message})
        except Exception:
            body = json.dumps({"__type": code, "message": message}).encode()
        return status, {"Content-Type": "application/cbor"}, body
    body = (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<ErrorResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">\n'
            f'    <Error><Code>{code}</Code><Message>{message}</Message></Error>\n'
            f'    <RequestId>{new_uuid()}</RequestId>\n'
            f'</ErrorResponse>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body
