# Changelog

All notable changes to MiniStack will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [0.1.0] — 2024-03-24

Initial public release. Built as a free, open-source alternative to LocalStack.

### Services Added

**Core (9 services)**
- S3 — CreateBucket, DeleteBucket, ListBuckets, HeadBucket, PutObject, GetObject, DeleteObject, HeadObject, CopyObject, ListObjects v1/v2, DeleteObjects (batch), optional disk persistence
- SQS — Full queue lifecycle, send/receive/delete, visibility timeout, batch operations, both Query API and JSON protocol
- SNS — Topics, subscriptions, publish
- DynamoDB — Tables, PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchWriteItem, BatchGetItem
- Lambda — CRUD + actual Python function execution via subprocess
- IAM — Users, roles, policies, access keys
- STS — GetCallerIdentity, AssumeRole, GetSessionToken
- SecretsManager — Full secret lifecycle
- CloudWatch Logs — Log groups, streams, PutLogEvents, GetLogEvents, FilterLogEvents

**Extended (6 services)**
- SSM Parameter Store — PutParameter, GetParameter, GetParametersByPath, DeleteParameter
- EventBridge — Event buses, rules, targets, PutEvents
- Kinesis — Streams, shards, PutRecord, PutRecords, GetShardIterator, GetRecords
- CloudWatch Metrics — PutMetricData, GetMetricStatistics, ListMetrics, alarms
- SES — SendEmail, SendRawEmail, identity verification (emails stored, not sent)
- Step Functions — State machines, executions, history

**Infrastructure (5 services)**
- ECS — Clusters, task definitions, services, RunTask with real Docker container execution
- RDS — CreateDBInstance spins up real Postgres/MySQL Docker containers with actual endpoints
- ElastiCache — CreateCacheCluster spins up real Redis/Memcached Docker containers
- Glue — Full Data Catalog (databases, tables, partitions), crawlers, jobs with Python execution
- Athena — Real SQL execution via DuckDB, s3:// path rewriting to local files

### Infrastructure
- Single ASGI app on port 4566 (LocalStack-compatible)
- Docker Compose with Redis sidecar
- Multi-arch Docker image (amd64 + arm64)
- GitHub Actions CI (test on every push/PR)
- GitHub Actions Docker publish (on tag)
- 54 integration tests, all passing
- MIT license

---

## Roadmap

### [0.2.0] — Planned
- API Gateway (REST + HTTP APIs)
- SNS → SQS fan-out delivery
- DynamoDB transactions (TransactWriteItems, TransactGetItems)
- S3 multipart upload
- SQS FIFO queues

### [0.3.0] — Planned
- Cognito (user pools, sign-up/sign-in)
- Step Functions ASL interpreter (actually run state machines)
- Route53 (hosted zones, record sets)
- ACM (certificate management)
- Firehose
