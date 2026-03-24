"""
SES (Simple Email Service) Emulator.
Query API (Action=...) via POST form body.
Supports: SendEmail, SendRawEmail, VerifyEmailIdentity, VerifyEmailAddress,
          ListIdentities, GetIdentityVerificationAttributes, DeleteIdentity,
          GetSendQuota, GetSendStatistics.
All emails are stored in-memory (not actually sent).
"""

import time
import logging
from urllib.parse import parse_qs

from core.responses import new_uuid

logger = logging.getLogger("ses")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_identities: dict = {}   # email/domain -> {verified, type}
_sent_emails: list = []  # log of sent emails


async def handle_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")

    handlers = {
        "SendEmail": _send_email,
        "SendRawEmail": _send_raw_email,
        "VerifyEmailIdentity": _verify_email_identity,
        "VerifyEmailAddress": _verify_email_identity,  # legacy alias
        "ListIdentities": _list_identities,
        "GetIdentityVerificationAttributes": _get_identity_verification_attributes,
        "DeleteIdentity": _delete_identity,
        "GetSendQuota": _get_send_quota,
        "GetSendStatistics": _get_send_statistics,
        "ListVerifiedEmailAddresses": _list_verified_emails,
    }

    handler = handlers.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown action: {action}", 400)
    return handler(params)


def _send_email(params):
    source = _p(params, "Source")
    subject = _p(params, "Message.Subject.Data")
    body_text = _p(params, "Message.Body.Text.Data")
    body_html = _p(params, "Message.Body.Html.Data")

    # Collect To/CC/BCC addresses
    to_addrs = _collect_list(params, "Destination.ToAddresses.member")
    cc_addrs = _collect_list(params, "Destination.CcAddresses.member")
    bcc_addrs = _collect_list(params, "Destination.BccAddresses.member")

    msg_id = f"{new_uuid()}@email.amazonses.com"
    _sent_emails.append({
        "MessageId": msg_id,
        "Source": source,
        "To": to_addrs,
        "CC": cc_addrs,
        "BCC": bcc_addrs,
        "Subject": subject,
        "BodyText": body_text,
        "BodyHtml": body_html,
        "Timestamp": time.time(),
    })
    logger.info(f"SES SendEmail: {source} -> {to_addrs} | {subject}")
    return _xml(200, "SendEmailResponse", f"<SendEmailResult><MessageId>{msg_id}</MessageId></SendEmailResult>")


def _send_raw_email(params):
    raw_message = _p(params, "RawMessage.Data")
    msg_id = f"{new_uuid()}@email.amazonses.com"
    _sent_emails.append({
        "MessageId": msg_id,
        "RawMessage": raw_message,
        "Timestamp": time.time(),
    })
    logger.info(f"SES SendRawEmail: {msg_id}")
    return _xml(200, "SendRawEmailResponse", f"<SendRawEmailResult><MessageId>{msg_id}</MessageId></SendRawEmailResult>")


def _verify_email_identity(params):
    email = _p(params, "EmailAddress")
    _identities[email] = {"VerificationStatus": "Success", "Type": "EmailAddress"}
    return _xml(200, "VerifyEmailIdentityResponse", "<VerifyEmailIdentityResult/>")


def _list_identities(params):
    identity_type = _p(params, "IdentityType")
    members = ""
    for identity, info in _identities.items():
        if not identity_type or info["Type"] == identity_type:
            members += f"<member>{identity}</member>"
    return _xml(200, "ListIdentitiesResponse", f"<ListIdentitiesResult><Identities>{members}</Identities></ListIdentitiesResult>")


def _get_identity_verification_attributes(params):
    identities = _collect_list(params, "Identities.member")
    entries = ""
    for identity in identities:
        info = _identities.get(identity, {"VerificationStatus": "Pending"})
        entries += f"""<entry>
            <key>{identity}</key>
            <value><VerificationStatus>{info['VerificationStatus']}</VerificationStatus></value>
        </entry>"""
    return _xml(200, "GetIdentityVerificationAttributesResponse",
        f"<GetIdentityVerificationAttributesResult><VerificationAttributes>{entries}</VerificationAttributes></GetIdentityVerificationAttributesResult>")


def _delete_identity(params):
    identity = _p(params, "Identity")
    _identities.pop(identity, None)
    return _xml(200, "DeleteIdentityResponse", "")


def _get_send_quota(params):
    return _xml(200, "GetSendQuotaResponse", """<GetSendQuotaResult>
        <Max24HourSend>50000.0</Max24HourSend>
        <MaxSendRate>14.0</MaxSendRate>
        <SentLast24Hours>0.0</SentLast24Hours>
    </GetSendQuotaResult>""")


def _get_send_statistics(params):
    members = ""
    for email in _sent_emails[-100:]:
        members += f"""<member>
            <Timestamp>{email['Timestamp']}</Timestamp>
            <DeliveryAttempts>1</DeliveryAttempts>
            <Bounces>0</Bounces>
            <Complaints>0</Complaints>
            <Rejects>0</Rejects>
        </member>"""
    return _xml(200, "GetSendStatisticsResponse", f"<GetSendStatisticsResult><SendDataPoints>{members}</SendDataPoints></GetSendStatisticsResult>")


def _list_verified_emails(params):
    members = "".join(f"<member>{e}</member>" for e in _identities if _identities[e]["Type"] == "EmailAddress")
    return _xml(200, "ListVerifiedEmailAddressesResponse",
        f"<ListVerifiedEmailAddressesResult><VerifiedEmailAddresses>{members}</VerifiedEmailAddresses></ListVerifiedEmailAddressesResult>")


# --- Helpers ---

def _collect_list(params, prefix):
    result = []
    i = 1
    while _p(params, f"{prefix}.{i}"):
        result.append(_p(params, f"{prefix}.{i}"))
        i += 1
    return result


def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _xml(status, root_tag, inner):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://ses.amazonaws.com/doc/2010-12-01/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://ses.amazonaws.com/doc/2010-12-01/">
    <Error><Code>{code}</Code><Message>{message}</Message></Error>
    <RequestId>{new_uuid()}</RequestId>
</ErrorResponse>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body
