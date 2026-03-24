"""
ElastiCache Service Emulator.
Query API (Action=...) for control plane.
Supports: CreateCacheCluster, DeleteCacheCluster, DescribeCacheClusters,
          CreateReplicationGroup, DeleteReplicationGroup, DescribeReplicationGroups,
          CreateCacheSubnetGroup, DescribeCacheSubnetGroups,
          CreateCacheParameterGroup, DescribeCacheParameterGroups,
          ListTagsForResource, AddTagsToResource.

When Docker is available, CreateCacheCluster spins up a real Redis/Memcached container.
Otherwise returns localhost:6379 (assumes Redis sidecar in docker-compose).
"""

import os
import time
import logging
from urllib.parse import parse_qs

from core.responses import new_uuid

logger = logging.getLogger("elasticache")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"
REDIS_DEFAULT_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_DEFAULT_PORT = int(os.environ.get("REDIS_PORT", "6379"))
BASE_PORT = int(os.environ.get("ELASTICACHE_BASE_PORT", "16379"))

_clusters: dict = {}
_replication_groups: dict = {}
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
        "CreateCacheCluster": _create_cache_cluster,
        "DeleteCacheCluster": _delete_cache_cluster,
        "DescribeCacheClusters": _describe_cache_clusters,
        "ModifyCacheCluster": _modify_cache_cluster,
        "RebootCacheCluster": _reboot_cache_cluster,
        "CreateReplicationGroup": _create_replication_group,
        "DeleteReplicationGroup": _delete_replication_group,
        "DescribeReplicationGroups": _describe_replication_groups,
        "ModifyReplicationGroup": _modify_replication_group,
        "CreateCacheSubnetGroup": _create_subnet_group,
        "DescribeCacheSubnetGroups": _describe_subnet_groups,
        "DeleteCacheSubnetGroup": _delete_subnet_group,
        "CreateCacheParameterGroup": _create_param_group,
        "DescribeCacheParameterGroups": _describe_param_groups,
        "DeleteCacheParameterGroup": _delete_param_group,
        "DescribeCacheEngineVersions": _describe_engine_versions,
        "ListTagsForResource": _list_tags,
        "AddTagsToResource": _add_tags,
        "RemoveTagsFromResource": _remove_tags,
    }

    handler = handlers.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown ElastiCache action: {action}", 400)
    return handler(params)


def _create_cache_cluster(p):
    cluster_id = _p(p, "CacheClusterId")
    engine = _p(p, "Engine") or "redis"
    engine_version = _p(p, "EngineVersion") or ("7.0.12" if engine == "redis" else "1.6.17")
    node_type = _p(p, "CacheNodeType") or "cache.t3.micro"
    num_nodes = int(_p(p, "NumCacheNodes") or "1")

    if cluster_id in _clusters:
        return _error("CacheClusterAlreadyExists", f"Cluster {cluster_id} already exists", 400)

    arn = f"arn:aws:elasticache:{REGION}:{ACCOUNT_ID}:cluster:{cluster_id}"
    endpoint_host = REDIS_DEFAULT_HOST
    endpoint_port = REDIS_DEFAULT_PORT if engine == "redis" else 11211
    docker_container_id = None

    # Try to spin up a real container
    docker_client = _get_docker()
    if docker_client:
        host_port = _port_counter[0]
        _port_counter[0] += 1
        endpoint_host = "localhost"
        endpoint_port = host_port

        if engine == "redis":
            image = f"redis:{engine_version.split('.')[0]}-alpine"
            container_port = 6379
        else:
            image = f"memcached:{engine_version}-alpine"
            container_port = 11211

        try:
            container = docker_client.containers.run(
                image, detach=True,
                ports={f"{container_port}/tcp": host_port},
                name=f"ministack-elasticache-{cluster_id}",
                labels={"ministack": "elasticache", "cluster_id": cluster_id},
            )
            docker_container_id = container.id
            logger.info(f"ElastiCache: started {engine} container for {cluster_id} on port {host_port}")
        except Exception as e:
            logger.warning(f"ElastiCache: Docker failed for {cluster_id}: {e}")
            endpoint_host = REDIS_DEFAULT_HOST
            endpoint_port = REDIS_DEFAULT_PORT

    _clusters[cluster_id] = {
        "CacheClusterId": cluster_id,
        "CacheClusterArn": arn,
        "CacheClusterStatus": "available",
        "Engine": engine,
        "EngineVersion": engine_version,
        "CacheNodeType": node_type,
        "NumCacheNodes": num_nodes,
        "CacheClusterCreateTime": time.time(),
        "PreferredAvailabilityZone": f"{REGION}a",
        "CacheParameterGroup": {"CacheParameterGroupName": f"default.{engine}{engine_version[:3]}", "ParameterApplyStatus": "in-sync"},
        "CacheSubnetGroupName": _p(p, "CacheSubnetGroupName") or "default",
        "AutoMinorVersionUpgrade": True,
        "SecurityGroups": [],
        "ReplicationGroupId": "",
        "SnapshotRetentionLimit": 0,
        "SnapshotWindow": "05:00-06:00",
        "CacheNodes": [
            {
                "CacheNodeId": f"{i:04d}",
                "CacheNodeStatus": "available",
                "CacheNodeCreateTime": time.time(),
                "Endpoint": {"Address": endpoint_host, "Port": endpoint_port},
                "ParameterGroupStatus": "in-sync",
                "SourceCacheNodeId": "",
            }
            for i in range(1, num_nodes + 1)
        ],
        "_docker_container_id": docker_container_id,
        "_endpoint": {"Address": endpoint_host, "Port": endpoint_port},
    }
    return _xml_cluster_response("CreateCacheClusterResponse", "CreateCacheClusterResult", _clusters[cluster_id])


def _delete_cache_cluster(p):
    cluster_id = _p(p, "CacheClusterId")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("CacheClusterNotFound", f"Cluster {cluster_id} not found", 404)

    docker_client = _get_docker()
    if docker_client and cluster.get("_docker_container_id"):
        try:
            container = docker_client.containers.get(cluster["_docker_container_id"])
            container.stop(timeout=5)
            container.remove()
        except Exception as e:
            logger.warning(f"ElastiCache: failed to remove container for {cluster_id}: {e}")

    cluster["CacheClusterStatus"] = "deleting"
    del _clusters[cluster_id]
    return _xml_cluster_response("DeleteCacheClusterResponse", "DeleteCacheClusterResult", cluster)


def _describe_cache_clusters(p):
    cluster_id = _p(p, "CacheClusterId")
    if cluster_id:
        cluster = _clusters.get(cluster_id)
        if not cluster:
            return _error("CacheClusterNotFound", f"Cluster {cluster_id} not found", 404)
        clusters = [cluster]
    else:
        clusters = list(_clusters.values())
    members = "".join(_cluster_xml(c) for c in clusters)
    return _xml(200, "DescribeCacheClustersResponse",
        f"<DescribeCacheClustersResult><CacheClusters>{members}</CacheClusters></DescribeCacheClustersResult>")


def _modify_cache_cluster(p):
    cluster_id = _p(p, "CacheClusterId")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("CacheClusterNotFound", f"Cluster {cluster_id} not found", 404)
    if _p(p, "NumCacheNodes"):
        cluster["NumCacheNodes"] = int(_p(p, "NumCacheNodes"))
    return _xml_cluster_response("ModifyCacheClusterResponse", "ModifyCacheClusterResult", cluster)


def _reboot_cache_cluster(p):
    cluster_id = _p(p, "CacheClusterId")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("CacheClusterNotFound", f"Cluster {cluster_id} not found", 404)
    return _xml_cluster_response("RebootCacheClusterResponse", "RebootCacheClusterResult", cluster)


def _create_replication_group(p):
    rg_id = _p(p, "ReplicationGroupId")
    desc = _p(p, "ReplicationGroupDescription")
    arn = f"arn:aws:elasticache:{REGION}:{ACCOUNT_ID}:replicationgroup:{rg_id}"
    endpoint_host = REDIS_DEFAULT_HOST
    endpoint_port = REDIS_DEFAULT_PORT

    _replication_groups[rg_id] = {
        "ReplicationGroupId": rg_id,
        "Description": desc,
        "Status": "available",
        "MemberClusters": [],
        "NodeGroups": [{"NodeGroupId": "0001", "Status": "available",
                        "PrimaryEndpoint": {"Address": endpoint_host, "Port": endpoint_port},
                        "ReaderEndpoint": {"Address": endpoint_host, "Port": endpoint_port}}],
        "SnapshotRetentionLimit": 0,
        "SnapshotWindow": "05:00-06:00",
        "ClusterEnabled": False,
        "CacheNodeType": _p(p, "CacheNodeType") or "cache.t3.micro",
        "AuthTokenEnabled": False,
        "TransitEncryptionEnabled": False,
        "AtRestEncryptionEnabled": False,
        "ARN": arn,
    }
    members = _rg_xml(_replication_groups[rg_id])
    return _xml(200, "CreateReplicationGroupResponse",
        f"<CreateReplicationGroupResult><ReplicationGroup>{members}</ReplicationGroup></CreateReplicationGroupResult>")


def _delete_replication_group(p):
    rg_id = _p(p, "ReplicationGroupId")
    rg = _replication_groups.pop(rg_id, None)
    if not rg:
        return _error("ReplicationGroupNotFoundFault", f"Replication group {rg_id} not found", 404)
    members = _rg_xml(rg)
    return _xml(200, "DeleteReplicationGroupResponse",
        f"<DeleteReplicationGroupResult><ReplicationGroup>{members}</ReplicationGroup></DeleteReplicationGroupResult>")


def _describe_replication_groups(p):
    rg_id = _p(p, "ReplicationGroupId")
    if rg_id:
        rg = _replication_groups.get(rg_id)
        if not rg:
            return _error("ReplicationGroupNotFoundFault", f"Replication group {rg_id} not found", 404)
        groups = [rg]
    else:
        groups = list(_replication_groups.values())
    members = "".join(f"<member>{_rg_xml(g)}</member>" for g in groups)
    return _xml(200, "DescribeReplicationGroupsResponse",
        f"<DescribeReplicationGroupsResult><ReplicationGroups>{members}</ReplicationGroups></DescribeReplicationGroupsResult>")


def _modify_replication_group(p):
    rg_id = _p(p, "ReplicationGroupId")
    rg = _replication_groups.get(rg_id)
    if not rg:
        return _error("ReplicationGroupNotFoundFault", f"Replication group {rg_id} not found", 404)
    return _xml(200, "ModifyReplicationGroupResponse",
        f"<ModifyReplicationGroupResult><ReplicationGroup>{_rg_xml(rg)}</ReplicationGroup></ModifyReplicationGroupResult>")


def _create_subnet_group(p):
    name = _p(p, "CacheSubnetGroupName")
    _subnet_groups[name] = {"CacheSubnetGroupName": name, "CacheSubnetGroupDescription": _p(p, "CacheSubnetGroupDescription")}
    return _xml(200, "CreateCacheSubnetGroupResponse",
        f"<CreateCacheSubnetGroupResult><CacheSubnetGroup><CacheSubnetGroupName>{name}</CacheSubnetGroupName></CacheSubnetGroup></CreateCacheSubnetGroupResult>")


def _describe_subnet_groups(p):
    name = _p(p, "CacheSubnetGroupName")
    groups = [_subnet_groups[name]] if name and name in _subnet_groups else list(_subnet_groups.values())
    members = "".join(f"<member><CacheSubnetGroupName>{g['CacheSubnetGroupName']}</CacheSubnetGroupName></member>" for g in groups)
    return _xml(200, "DescribeCacheSubnetGroupsResponse",
        f"<DescribeCacheSubnetGroupsResult><CacheSubnetGroups>{members}</CacheSubnetGroups></DescribeCacheSubnetGroupsResult>")


def _delete_subnet_group(p):
    _subnet_groups.pop(_p(p, "CacheSubnetGroupName"), None)
    return _xml(200, "DeleteCacheSubnetGroupResponse", "")


def _create_param_group(p):
    name = _p(p, "CacheParameterGroupName")
    _param_groups[name] = {"CacheParameterGroupName": name, "CacheParameterGroupFamily": _p(p, "CacheParameterGroupFamily")}
    return _xml(200, "CreateCacheParameterGroupResponse",
        f"<CreateCacheParameterGroupResult><CacheParameterGroup><CacheParameterGroupName>{name}</CacheParameterGroupName></CacheParameterGroup></CreateCacheParameterGroupResult>")


def _describe_param_groups(p):
    name = _p(p, "CacheParameterGroupName")
    groups = [_param_groups[name]] if name and name in _param_groups else list(_param_groups.values())
    members = "".join(f"<member><CacheParameterGroupName>{g['CacheParameterGroupName']}</CacheParameterGroupName></member>" for g in groups)
    return _xml(200, "DescribeCacheParameterGroupsResponse",
        f"<DescribeCacheParameterGroupsResult><CacheParameterGroups>{members}</CacheParameterGroups></DescribeCacheParameterGroupsResult>")


def _delete_param_group(p):
    _param_groups.pop(_p(p, "CacheParameterGroupName"), None)
    return _xml(200, "DeleteCacheParameterGroupResponse", "")


def _describe_engine_versions(p):
    engine = _p(p, "Engine") or "redis"
    versions = {"redis": ["7.0.12", "6.2.14", "5.0.6"], "memcached": ["1.6.17", "1.6.12"]}
    members = "".join(
        f"<member><Engine>{engine}</Engine><EngineVersion>{v}</EngineVersion><CacheParameterGroupFamily>{engine}{v[:3]}</CacheParameterGroupFamily></member>"
        for v in versions.get(engine, ["7.0.12"])
    )
    return _xml(200, "DescribeCacheEngineVersionsResponse",
        f"<DescribeCacheEngineVersionsResult><CacheEngineVersions>{members}</CacheEngineVersions></DescribeCacheEngineVersionsResult>")


def _list_tags(p):
    return _xml(200, "ListTagsForResourceResponse", "<ListTagsForResourceResult><TagList/></ListTagsForResourceResult>")


def _add_tags(p):
    return _xml(200, "AddTagsToResourceResponse", "<AddTagsToResourceResult><TagList/></AddTagsToResourceResult>")


def _remove_tags(p):
    return _xml(200, "RemoveTagsFromResourceResponse", "")


# ---- XML helpers ----

def _cluster_xml(c):
    ep = c.get("_endpoint", {})
    nodes_xml = ""
    for node in c.get("CacheNodes", []):
        nep = node.get("Endpoint", {})
        nodes_xml += f"""<member>
            <CacheNodeId>{node['CacheNodeId']}</CacheNodeId>
            <CacheNodeStatus>{node['CacheNodeStatus']}</CacheNodeStatus>
            <Endpoint><Address>{nep.get('Address','localhost')}</Address><Port>{nep.get('Port',6379)}</Port></Endpoint>
        </member>"""
    return f"""<member>
        <CacheClusterId>{c['CacheClusterId']}</CacheClusterId>
        <CacheClusterStatus>{c['CacheClusterStatus']}</CacheClusterStatus>
        <Engine>{c['Engine']}</Engine>
        <EngineVersion>{c['EngineVersion']}</EngineVersion>
        <CacheNodeType>{c['CacheNodeType']}</CacheNodeType>
        <NumCacheNodes>{c['NumCacheNodes']}</NumCacheNodes>
        <CacheClusterArn>{c['CacheClusterArn']}</CacheClusterArn>
        <CacheNodes>{nodes_xml}</CacheNodes>
    </member>"""


def _rg_xml(rg):
    return f"""<ReplicationGroupId>{rg['ReplicationGroupId']}</ReplicationGroupId>
        <Description>{rg['Description']}</Description>
        <Status>{rg['Status']}</Status>
        <ARN>{rg['ARN']}</ARN>"""


def _xml_cluster_response(root_tag, result_tag, cluster):
    return _xml(200, root_tag, f"<{result_tag}><CacheCluster>{_cluster_xml(cluster)}</CacheCluster></{result_tag}>")


def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _xml(status, root_tag, inner):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://elasticache.amazonaws.com/doc/2015-02-02/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://elasticache.amazonaws.com/doc/2015-02-02/">
    <Error><Code>{code}</Code><Message>{message}</Message></Error>
    <RequestId>{new_uuid()}</RequestId>
</ErrorResponse>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body
