"""
IAM & STS Service Emulators.
IAM: CreateUser, GetUser, ListUsers, DeleteUser, CreateRole, GetRole, ListRoles,
     DeleteRole, CreatePolicy, AttachRolePolicy, PutRolePolicy, CreateAccessKey.
STS: GetCallerIdentity, AssumeRole, GetSessionToken.
"""

import time
import json
import logging
from urllib.parse import parse_qs

from core.responses import new_uuid

logger = logging.getLogger("iam")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_users: dict = {}
_roles: dict = {}
_policies: dict = {}
_access_keys: dict = {}


# ========== IAM ==========

async def handle_iam_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")

    handlers = {
        "CreateUser": _create_user,
        "GetUser": _get_user,
        "ListUsers": _list_users,
        "DeleteUser": _delete_user,
        "CreateRole": _create_role,
        "GetRole": _get_role,
        "ListRoles": _list_roles,
        "DeleteRole": _delete_role,
        "CreatePolicy": _create_policy,
        "AttachRolePolicy": _attach_role_policy,
        "PutRolePolicy": _put_role_policy,
        "CreateAccessKey": _create_access_key,
        "ListAccessKeys": _list_access_keys,
    }

    handler = handlers.get(action)
    if not handler:
        return _xml(200, "ErrorResponse", f"<Error><Code>InvalidAction</Code><Message>Unknown: {action}</Message></Error>", ns="iam")
    return handler(params)


def _create_user(p):
    name = _p(p, "UserName")
    arn = f"arn:aws:iam::{ACCOUNT_ID}:user/{name}"
    _users[name] = {"UserName": name, "Arn": arn, "UserId": new_uuid()[:20].upper(), "CreateDate": _now(), "Path": _p(p, "Path") or "/"}
    return _xml(200, "CreateUserResponse", f"<CreateUserResult><User>{_user_xml(name)}</User></CreateUserResult>", ns="iam")


def _get_user(p):
    name = _p(p, "UserName") or "default"
    if name not in _users:
        _users[name] = {"UserName": name, "Arn": f"arn:aws:iam::{ACCOUNT_ID}:user/{name}", "UserId": new_uuid()[:20].upper(), "CreateDate": _now(), "Path": "/"}
    return _xml(200, "GetUserResponse", f"<GetUserResult><User>{_user_xml(name)}</User></GetUserResult>", ns="iam")


def _list_users(p):
    members = "".join(f"<member>{_user_xml(n)}</member>" for n in _users)
    return _xml(200, "ListUsersResponse", f"<ListUsersResult><Users>{members}</Users><IsTruncated>false</IsTruncated></ListUsersResult>", ns="iam")


def _delete_user(p):
    _users.pop(_p(p, "UserName"), None)
    return _xml(200, "DeleteUserResponse", "", ns="iam")


def _create_role(p):
    name = _p(p, "RoleName")
    arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{name}"
    _roles[name] = {"RoleName": name, "Arn": arn, "RoleId": new_uuid()[:20].upper(), "CreateDate": _now(), "Path": _p(p, "Path") or "/", "AssumeRolePolicyDocument": _p(p, "AssumeRolePolicyDocument"), "Policies": []}
    return _xml(200, "CreateRoleResponse", f"<CreateRoleResult><Role>{_role_xml(name)}</Role></CreateRoleResult>", ns="iam")


def _get_role(p):
    name = _p(p, "RoleName")
    if name not in _roles:
        return _xml(404, "ErrorResponse", f"<Error><Code>NoSuchEntity</Code><Message>Role {name} not found</Message></Error>", ns="iam")
    return _xml(200, "GetRoleResponse", f"<GetRoleResult><Role>{_role_xml(name)}</Role></GetRoleResult>", ns="iam")


def _list_roles(p):
    members = "".join(f"<member>{_role_xml(n)}</member>" for n in _roles)
    return _xml(200, "ListRolesResponse", f"<ListRolesResult><Roles>{members}</Roles><IsTruncated>false</IsTruncated></ListRolesResult>", ns="iam")


def _delete_role(p):
    _roles.pop(_p(p, "RoleName"), None)
    return _xml(200, "DeleteRoleResponse", "", ns="iam")


def _create_policy(p):
    name = _p(p, "PolicyName")
    arn = f"arn:aws:iam::{ACCOUNT_ID}:policy/{name}"
    _policies[arn] = {"PolicyName": name, "Arn": arn, "PolicyId": new_uuid()[:20].upper(), "CreateDate": _now()}
    return _xml(200, "CreatePolicyResponse", f"""<CreatePolicyResult><Policy>
        <PolicyName>{name}</PolicyName><Arn>{arn}</Arn>
    </Policy></CreatePolicyResult>""", ns="iam")


def _attach_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_arn = _p(p, "PolicyArn")
    if role_name in _roles:
        _roles[role_name]["Policies"].append(policy_arn)
    return _xml(200, "AttachRolePolicyResponse", "", ns="iam")


def _put_role_policy(p):
    return _xml(200, "PutRolePolicyResponse", "", ns="iam")


def _create_access_key(p):
    user = _p(p, "UserName") or "default"
    key_id = f"AKIA{''.join(new_uuid().replace('-','')[:16].upper())}"
    secret = new_uuid().replace("-", "") + new_uuid().replace("-", "")[:8]
    _access_keys[key_id] = {"UserName": user, "AccessKeyId": key_id, "SecretAccessKey": secret, "Status": "Active", "CreateDate": _now()}
    return _xml(200, "CreateAccessKeyResponse", f"""<CreateAccessKeyResult><AccessKey>
        <UserName>{user}</UserName><AccessKeyId>{key_id}</AccessKeyId>
        <SecretAccessKey>{secret}</SecretAccessKey><Status>Active</Status>
    </AccessKey></CreateAccessKeyResult>""", ns="iam")


def _list_access_keys(p):
    user = _p(p, "UserName") or "default"
    members = ""
    for k, v in _access_keys.items():
        if v["UserName"] == user:
            members += f"<member><AccessKeyId>{k}</AccessKeyId><Status>{v['Status']}</Status><UserName>{user}</UserName></member>"
    return _xml(200, "ListAccessKeysResponse", f"<ListAccessKeysResult><AccessKeyMetadata>{members}</AccessKeyMetadata></ListAccessKeysResult>", ns="iam")


def _user_xml(name):
    u = _users[name]
    return f"<UserName>{u['UserName']}</UserName><UserId>{u['UserId']}</UserId><Arn>{u['Arn']}</Arn><Path>{u['Path']}</Path><CreateDate>{u['CreateDate']}</CreateDate>"


def _role_xml(name):
    r = _roles[name]
    return f"<RoleName>{r['RoleName']}</RoleName><RoleId>{r['RoleId']}</RoleId><Arn>{r['Arn']}</Arn><Path>{r['Path']}</Path><CreateDate>{r['CreateDate']}</CreateDate>"


# ========== STS ==========

async def handle_sts_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")

    if action == "GetCallerIdentity":
        return _xml(200, "GetCallerIdentityResponse", f"""<GetCallerIdentityResult>
            <Arn>arn:aws:iam::{ACCOUNT_ID}:root</Arn>
            <UserId>{ACCOUNT_ID}</UserId>
            <Account>{ACCOUNT_ID}</Account>
        </GetCallerIdentityResult>""", ns="sts")

    if action == "AssumeRole":
        role_arn = _p(params, "RoleArn")
        session = _p(params, "RoleSessionName")
        return _xml(200, "AssumeRoleResponse", f"""<AssumeRoleResult>
            <Credentials>
                <AccessKeyId>ASIAEXAMPLE</AccessKeyId>
                <SecretAccessKey>secretkey</SecretAccessKey>
                <SessionToken>sessiontoken{new_uuid()}</SessionToken>
                <Expiration>{_now()}</Expiration>
            </Credentials>
            <AssumedRoleUser>
                <AssumedRoleId>AROA:{session}</AssumedRoleId>
                <Arn>{role_arn}</Arn>
            </AssumedRoleUser>
        </AssumeRoleResult>""", ns="sts")

    if action == "GetSessionToken":
        return _xml(200, "GetSessionTokenResponse", f"""<GetSessionTokenResult>
            <Credentials>
                <AccessKeyId>ASIAEXAMPLE</AccessKeyId>
                <SecretAccessKey>secretkey</SecretAccessKey>
                <SessionToken>sessiontoken{new_uuid()}</SessionToken>
                <Expiration>{_now()}</Expiration>
            </Credentials>
        </GetSessionTokenResult>""", ns="sts")

    return _xml(400, "ErrorResponse", f"<Error><Code>InvalidAction</Code><Message>Unknown: {action}</Message></Error>", ns="sts")


# ========== Shared helpers ==========

def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _xml(status, root_tag, inner, ns="iam"):
    ns_url = {
        "iam": "https://iam.amazonaws.com/doc/2010-05-08/",
        "sts": "https://sts.amazonaws.com/doc/2011-06-15/",
    }.get(ns, "")
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="{ns_url}">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body
