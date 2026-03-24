"""
RDS Service Emulator.
Query API (Action=...) for control plane + optional Docker-based real Postgres/MySQL.
Supports: CreateDBInstance, DeleteDBInstance, DescribeDBInstances,
          CreateDBCluster, DeleteDBCluster, DescribeDBClusters,
          CreateDBSubnetGroup, DescribeDBSubnetGroups,
          CreateDBParameterGroup, DescribeDBParameterGroups,
          StartDBInstance, StopDBInstance, RebootDBInstance,
          ListTagsForResource, AddTagsToResource.

When Docker is available, CreateDBInstance spins up a real Postgres/MySQL container
and returns the actual host:port as the endpoint.
"""

import os
import time
import json
import logging
from urllib.parse import parse_qs

from core.responses import new_uuid

logger = logging.getLogger("rds")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"
BASE_PORT = int(os.environ.get("RDS_BASE_PORT", "15432"))

_instances: dict = {}       # db_instance_id -> instance dict
_clusters: dict = {}        # db_cluster_id -> cluster dict
_subnet_groups: dict = {}
_param_groups: dict = {}
_port_counter = [BASE_PORT]

_docker = None
def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env()
        except Exception:
            pass
    return _docker


async def handle_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")

    handlers = {
        "CreateDBInstance": _create_db_instance,
        "DeleteDBInstance": _delete_db_instance,
        "DescribeDBInstances": _describe_db_instances,
        "StartDBInstance": _start_db_instance,
        "StopDBInstance": _stop_db_instance,
        "RebootDBInstance": _reboot_db_instance,
        "ModifyDBInstance": _modify_db_instance,
        "CreateDBCluster": _create_db_cluster,
        "DeleteDBCluster": _delete_db_cluster,
        "DescribeDBClusters": _describe_db_clusters,
        "CreateDBSubnetGroup": _create_subnet_group,
        "DescribeDBSubnetGroups": _describe_subnet_groups,
        "DeleteDBSubnetGroup": _delete_subnet_group,
        "CreateDBParameterGroup": _create_param_group,
        "DescribeDBParameterGroups": _describe_param_groups,
        "DeleteDBParameterGroup": _delete_param_group,
        "ListTagsForResource": _list_tags,
        "AddTagsToResource": _add_tags,
        "RemoveTagsFromResource": _remove_tags,
        "DescribeDBEngineVersions": _describe_engine_versions,
        "DescribeOrderableDBInstanceOptions": _describe_orderable_options,
    }

    handler = handlers.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown RDS action: {action}", 400)
    return handler(params)


def _create_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    engine = _p(p, "Engine") or "postgres"
    engine_version = _p(p, "EngineVersion") or ("15.3" if "postgres" in engine else "8.0.33")
    db_class = _p(p, "DBInstanceClass") or "db.t3.micro"
    master_user = _p(p, "MasterUsername") or "admin"
    master_pass = _p(p, "MasterUserPassword") or "password"
    db_name = _p(p, "DBName") or "mydb"
    port = int(_p(p, "Port") or ("5432" if "postgres" in engine else "3306"))
    allocated_storage = int(_p(p, "AllocatedStorage") or "20")

    if db_id in _instances:
        return _error("DBInstanceAlreadyExists", f"DB instance {db_id} already exists", 400)

    arn = f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:db:{db_id}"
    endpoint_host = "localhost"
    endpoint_port = port
    docker_container_id = None

    # Try to spin up a real DB container
    docker_client = _get_docker()
    if docker_client:
        host_port = _port_counter[0]
        _port_counter[0] += 1
        endpoint_port = host_port

        if "postgres" in engine or "aurora-postgresql" in engine:
            image = f"postgres:{engine_version.split('.')[0]}-alpine"
            env = {
                "POSTGRES_USER": master_user,
                "POSTGRES_PASSWORD": master_pass,
                "POSTGRES_DB": db_name,
            }
            container_port = 5432
        elif "mysql" in engine or "aurora-mysql" in engine or "mariadb" in engine:
            image = "mysql:8" if "mysql" in engine else "mariadb:latest"
            env = {
                "MYSQL_ROOT_PASSWORD": master_pass,
                "MYSQL_DATABASE": db_name,
                "MYSQL_USER": master_user,
                "MYSQL_PASSWORD": master_pass,
            }
            container_port = 3306
        else:
            image = None

        if image:
            try:
                container = docker_client.containers.run(
                    image, detach=True,
                    environment=env,
                    ports={f"{container_port}/tcp": host_port},
                    name=f"ministack-rds-{db_id}",
                    labels={"ministack": "rds", "db_id": db_id},
                )
                docker_container_id = container.id
                logger.info(f"RDS: started {engine} container for {db_id} on port {host_port}")
            except Exception as e:
                logger.warning(f"RDS: Docker failed for {db_id}: {e}")

    instance = {
        "DBInstanceIdentifier": db_id,
        "DBInstanceClass": db_class,
        "Engine": engine,
        "EngineVersion": engine_version,
        "DBInstanceStatus": "available",
        "MasterUsername": master_user,
        "DBName": db_name,
        "Endpoint": {"Address": endpoint_host, "Port": endpoint_port, "HostedZoneId": "Z2R2ITUGPM61AM"},
        "AllocatedStorage": allocated_storage,
        "InstanceCreateTime": time.time(),
        "PreferredBackupWindow": "03:00-04:00",
        "BackupRetentionPeriod": int(_p(p, "BackupRetentionPeriod") or "1"),
        "DBSecurityGroups": [],
        "VpcSecurityGroups": [],
        "DBParameterGroups": [{"DBParameterGroupName": f"default.{engine}{engine_version[:3]}", "ParameterApplyStatus": "in-sync"}],
        "AvailabilityZone": f"{REGION}a",
        "DBSubnetGroup": {"DBSubnetGroupName": _p(p, "DBSubnetGroupName") or "default", "SubnetGroupStatus": "Complete", "Subnets": []},
        "PreferredMaintenanceWindow": "sun:05:00-sun:06:00",
        "PendingModifiedValues": {},
        "MultiAZ": _p(p, "MultiAZ") == "true",
        "AutoMinorVersionUpgrade": True,
        "ReadReplicaDBInstanceIdentifiers": [],
        "LicenseModel": "general-public-license",
        "OptionGroupMemberships": [],
        "PubliclyAccessible": _p(p, "PubliclyAccessible") == "true",
        "StorageType": _p(p, "StorageType") or "gp2",
        "DbInstancePort": endpoint_port,
        "StorageEncrypted": _p(p, "StorageEncrypted") == "true",
        "DbiResourceId": f"db-{new_uuid()[:8].upper()}",
        "CACertificateIdentifier": "rds-ca-2019",
        "DomainMemberships": [],
        "CopyTagsToSnapshot": False,
        "MonitoringInterval": 0,
        "DBInstanceArn": arn,
        "IAMDatabaseAuthenticationEnabled": False,
        "PerformanceInsightsEnabled": False,
        "DeletionProtection": _p(p, "DeletionProtection") == "true",
        "TagList": [],
        "_docker_container_id": docker_container_id,
    }
    _instances[db_id] = instance
    return _xml_instance_response("CreateDBInstanceResponse", "CreateDBInstanceResult", instance)


def _delete_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _instances.get(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DB instance {db_id} not found", 404)

    # Stop Docker container
    docker_client = _get_docker()
    if docker_client and instance.get("_docker_container_id"):
        try:
            container = docker_client.containers.get(instance["_docker_container_id"])
            container.stop(timeout=5)
            container.remove()
            logger.info(f"RDS: removed container for {db_id}")
        except Exception as e:
            logger.warning(f"RDS: failed to remove container for {db_id}: {e}")

    instance["DBInstanceStatus"] = "deleting"
    del _instances[db_id]
    return _xml_instance_response("DeleteDBInstanceResponse", "DeleteDBInstanceResult", instance)


def _describe_db_instances(p):
    db_id = _p(p, "DBInstanceIdentifier")
    if db_id:
        instance = _instances.get(db_id)
        if not instance:
            return _error("DBInstanceNotFound", f"DB instance {db_id} not found", 404)
        instances = [instance]
    else:
        instances = list(_instances.values())

    members = "".join(_instance_xml(i) for i in instances)
    return _xml(200, "DescribeDBInstancesResponse",
        f"<DescribeDBInstancesResult><DBInstances>{members}</DBInstances></DescribeDBInstancesResult>")


def _start_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _instances.get(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DB instance {db_id} not found", 404)
    instance["DBInstanceStatus"] = "available"
    return _xml_instance_response("StartDBInstanceResponse", "StartDBInstanceResult", instance)


def _stop_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _instances.get(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DB instance {db_id} not found", 404)
    instance["DBInstanceStatus"] = "stopped"
    return _xml_instance_response("StopDBInstanceResponse", "StopDBInstanceResult", instance)


def _reboot_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _instances.get(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DB instance {db_id} not found", 404)
    instance["DBInstanceStatus"] = "available"
    return _xml_instance_response("RebootDBInstanceResponse", "RebootDBInstanceResult", instance)


def _modify_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _instances.get(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DB instance {db_id} not found", 404)
    if _p(p, "DBInstanceClass"):
        instance["DBInstanceClass"] = _p(p, "DBInstanceClass")
    if _p(p, "AllocatedStorage"):
        instance["AllocatedStorage"] = int(_p(p, "AllocatedStorage"))
    return _xml_instance_response("ModifyDBInstanceResponse", "ModifyDBInstanceResult", instance)


def _create_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    engine = _p(p, "Engine") or "aurora-postgresql"
    arn = f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:cluster:{cluster_id}"
    _clusters[cluster_id] = {
        "DBClusterIdentifier": cluster_id,
        "DBClusterArn": arn,
        "Engine": engine,
        "EngineVersion": _p(p, "EngineVersion") or "15.3",
        "Status": "available",
        "MasterUsername": _p(p, "MasterUsername") or "admin",
        "DatabaseName": _p(p, "DatabaseName") or "",
        "Endpoint": f"{cluster_id}.cluster-{new_uuid()[:8]}.{REGION}.rds.amazonaws.com",
        "ReaderEndpoint": f"{cluster_id}.cluster-ro-{new_uuid()[:8]}.{REGION}.rds.amazonaws.com",
        "Port": int(_p(p, "Port") or "5432"),
        "MultiAZ": False,
        "DBClusterMembers": [],
        "ClusterCreateTime": time.time(),
    }
    members = _cluster_xml(_clusters[cluster_id])
    return _xml(200, "CreateDBClusterResponse", f"<CreateDBClusterResult><DBCluster>{members}</DBCluster></CreateDBClusterResult>")


def _delete_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _clusters.pop(cluster_id, None)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"Cluster {cluster_id} not found", 404)
    members = _cluster_xml(cluster)
    return _xml(200, "DeleteDBClusterResponse", f"<DeleteDBClusterResult><DBCluster>{members}</DBCluster></DeleteDBClusterResult>")


def _describe_db_clusters(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    if cluster_id:
        cluster = _clusters.get(cluster_id)
        if not cluster:
            return _error("DBClusterNotFoundFault", f"Cluster {cluster_id} not found", 404)
        clusters = [cluster]
    else:
        clusters = list(_clusters.values())
    members = "".join(f"<member>{_cluster_xml(c)}</member>" for c in clusters)
    return _xml(200, "DescribeDBClustersResponse",
        f"<DescribeDBClustersResult><DBClusters>{members}</DBClusters></DescribeDBClustersResult>")


def _create_subnet_group(p):
    name = _p(p, "DBSubnetGroupName")
    _subnet_groups[name] = {"DBSubnetGroupName": name, "DBSubnetGroupDescription": _p(p, "DBSubnetGroupDescription"), "Subnets": []}
    return _xml(200, "CreateDBSubnetGroupResponse",
        f"<CreateDBSubnetGroupResult><DBSubnetGroup><DBSubnetGroupName>{name}</DBSubnetGroupName></DBSubnetGroup></CreateDBSubnetGroupResult>")


def _describe_subnet_groups(p):
    name = _p(p, "DBSubnetGroupName")
    groups = [_subnet_groups[name]] if name and name in _subnet_groups else list(_subnet_groups.values())
    members = "".join(f"<member><DBSubnetGroupName>{g['DBSubnetGroupName']}</DBSubnetGroupName></member>" for g in groups)
    return _xml(200, "DescribeDBSubnetGroupsResponse",
        f"<DescribeDBSubnetGroupsResult><DBSubnetGroups>{members}</DBSubnetGroups></DescribeDBSubnetGroupsResult>")


def _delete_subnet_group(p):
    _subnet_groups.pop(_p(p, "DBSubnetGroupName"), None)
    return _xml(200, "DeleteDBSubnetGroupResponse", "")


def _create_param_group(p):
    name = _p(p, "DBParameterGroupName")
    _param_groups[name] = {"DBParameterGroupName": name, "DBParameterGroupFamily": _p(p, "DBParameterGroupFamily"), "Description": _p(p, "Description")}
    return _xml(200, "CreateDBParameterGroupResponse",
        f"<CreateDBParameterGroupResult><DBParameterGroup><DBParameterGroupName>{name}</DBParameterGroupName></DBParameterGroup></CreateDBParameterGroupResult>")


def _describe_param_groups(p):
    name = _p(p, "DBParameterGroupName")
    groups = [_param_groups[name]] if name and name in _param_groups else list(_param_groups.values())
    members = "".join(f"<member><DBParameterGroupName>{g['DBParameterGroupName']}</DBParameterGroupName></member>" for g in groups)
    return _xml(200, "DescribeDBParameterGroupsResponse",
        f"<DescribeDBParameterGroupsResult><DBParameterGroups>{members}</DBParameterGroups></DescribeDBParameterGroupsResult>")


def _delete_param_group(p):
    _param_groups.pop(_p(p, "DBParameterGroupName"), None)
    return _xml(200, "DeleteDBParameterGroupResponse", "")


def _list_tags(p):
    return _xml(200, "ListTagsForResourceResponse", "<ListTagsForResourceResult><TagList/></ListTagsForResourceResult>")


def _add_tags(p):
    return _xml(200, "AddTagsToResourceResponse", "")


def _remove_tags(p):
    return _xml(200, "RemoveTagsFromResourceResponse", "")


def _describe_engine_versions(p):
    engine = _p(p, "Engine") or "postgres"
    versions = {
        "postgres": ["15.3", "14.8", "13.11", "12.15"],
        "mysql": ["8.0.33", "8.0.28", "5.7.43"],
        "mariadb": ["10.6.14", "10.5.21"],
        "aurora-postgresql": ["15.3", "14.8"],
        "aurora-mysql": ["8.0.mysql_aurora.3.03.0"],
    }
    members = ""
    for v in versions.get(engine, ["15.3"]):
        members += f"<member><Engine>{engine}</Engine><EngineVersion>{v}</EngineVersion><DBParameterGroupFamily>{engine}{v[:3]}</DBParameterGroupFamily><DBEngineDescription>{engine}</DBEngineDescription><DBEngineVersionDescription>{engine} {v}</DBEngineVersionDescription></member>"
    return _xml(200, "DescribeDBEngineVersionsResponse",
        f"<DescribeDBEngineVersionsResult><DBEngineVersions>{members}</DBEngineVersions></DescribeDBEngineVersionsResult>")


def _describe_orderable_options(p):
    return _xml(200, "DescribeOrderableDBInstanceOptionsResponse",
        "<DescribeOrderableDBInstanceOptionsResult><OrderableDBInstanceOptions/></DescribeOrderableDBInstanceOptionsResult>")


# ---- XML helpers ----

def _instance_xml(i):
    ep = i.get("Endpoint", {})
    return f"""<member>
        <DBInstanceIdentifier>{i['DBInstanceIdentifier']}</DBInstanceIdentifier>
        <DBInstanceClass>{i['DBInstanceClass']}</DBInstanceClass>
        <Engine>{i['Engine']}</Engine>
        <EngineVersion>{i['EngineVersion']}</EngineVersion>
        <DBInstanceStatus>{i['DBInstanceStatus']}</DBInstanceStatus>
        <MasterUsername>{i['MasterUsername']}</MasterUsername>
        <DBName>{i.get('DBName','')}</DBName>
        <Endpoint>
            <Address>{ep.get('Address','localhost')}</Address>
            <Port>{ep.get('Port',5432)}</Port>
        </Endpoint>
        <AllocatedStorage>{i['AllocatedStorage']}</AllocatedStorage>
        <MultiAZ>{str(i['MultiAZ']).lower()}</MultiAZ>
        <StorageType>{i['StorageType']}</StorageType>
        <DBInstanceArn>{i['DBInstanceArn']}</DBInstanceArn>
    </member>"""


def _cluster_xml(c):
    return f"""<DBClusterIdentifier>{c['DBClusterIdentifier']}</DBClusterIdentifier>
        <DBClusterArn>{c['DBClusterArn']}</DBClusterArn>
        <Engine>{c['Engine']}</Engine>
        <EngineVersion>{c['EngineVersion']}</EngineVersion>
        <Status>{c['Status']}</Status>
        <Endpoint>{c['Endpoint']}</Endpoint>
        <ReaderEndpoint>{c['ReaderEndpoint']}</ReaderEndpoint>
        <Port>{c['Port']}</Port>"""


def _xml_instance_response(root_tag, result_tag, instance):
    return _xml(200, root_tag, f"<{result_tag}><DBInstance>{_instance_xml(instance)}</DBInstance></{result_tag}>")


def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _xml(status, root_tag, inner):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
    <Error><Code>{code}</Code><Message>{message}</Message></Error>
    <RequestId>{new_uuid()}</RequestId>
</ErrorResponse>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body
