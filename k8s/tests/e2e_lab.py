#!/usr/bin/env python3
"""End-to-end test of the NiFi multi-market lab on Kubernetes (run on the lab host).

Automates the walkthrough of docs/GUIDE_K8S_VI.md through public APIs only:

  1. Staging: build MARKET_RUNTIME / kafka-to-postgres (+ LOCAL_AUDIT), run an
     event Kafka VN (trips-staging) -> PostgreSQL VN schema staging.
  2. Save Version v1 through Registry Client gitlab-staging (Project Access
     Token nifi-staging-writer) -> GitLab branch staging; snapshot guard.
  3. Least privilege: writer PrAT cannot merge to protected main, reader PrAT
     cannot write; MR staging -> main merged by a Maintainer PrAT.
  4. VN and INDO import the approved main SHA M1 (reader PrATs), keep their own
     Vault-synced shared-runtime, add LOCAL_AUDIT, process one event each.
  5. v2 marker -> M2; VN Change Version to M2 while INDO stays on M1; then VN
     rollback to M1. Provenance proves flow.release and local.market per run.
  6. Re-sync shared-runtime from Vault with the ServiceAccount Job while the
     VN flow runs (NiFi stops/restarts affected components), then process data.
  7. NiFi VN native HashiCorpVaultParameterProvider with Vault Authentication
     KUBERNETES (pod ServiceAccount token) -> context vault-preview.

Options:
  --reset       remove MARKET_RUNTIME (+ provider exercise) from all NiFi first
  --reset-only  only clean up, then exit
Needs: k8s/scripts/gitlab-bootstrap.py --release-token (Maintainer PrAT for merges);
       a fresh GitLab project (gitlab-bootstrap.py --recreate-project) if the
       flow kafka-to-postgres already exists in GitLab.
"""

import json
import pathlib
import subprocess
import sys
import time
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "k8s/nifi/base"))
from nifi_k8s_sync import NiFi, HttpError, http  # noqa: E402

ENV = dict(
    line.split("=", 1)
    for line in (ROOT / ".env").read_text().splitlines()
    if line and not line.startswith("#") and "=" in line
)
IP = ENV["HOST_IP"]
PRIVATE = ROOT / ".private" / "k8s"
GITLAB = f"http://gitlab.{IP}.sslip.io:31080/api/v4"
PROJECT = urllib.parse.quote("data360/flows-nifi", safe="")
SNAPSHOT = "flows/shared/kafka-to-postgres.json"
RUN = time.strftime("%m%d%H%M%S")
MARKETS = {
    # market: kafka ns, topic, postgres ns, schema, registry client, branch, local.market
    "stag": (
        "kafka-vn",
        "trips-staging",
        "postgres-vn",
        "staging",
        "gitlab-staging",
        "staging",
        "STAG",
    ),
    "vn": ("kafka-vn", "trips", "postgres-vn", "market", "gitlab-main", "main", "VN"),
    "indo": (
        "kafka-indo",
        "trips",
        "postgres-indo",
        "market",
        "gitlab-main",
        "main",
        "INDO",
    ),
}
SCHEMA_TEXT = json.dumps(
    {
        "type": "record",
        "name": "TripEvent",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "event_time", "type": "string"},
        ],
    },
    indent=2,
)
EVIDENCE = {"run": RUN, "steps": []}


def step(message, **data):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)
    EVIDENCE["steps"].append({"message": message, **data})


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def wait_for(predicate, message, timeout=120, delay=2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(delay)
    raise AssertionError("Timed out: " + message)


def kubectl(*args, stdin=None):
    return subprocess.run(
        ["kubectl", *args], input=stdin, capture_output=True, text=True, check=True
    ).stdout.strip()


def produce(market, event_id):
    kafka_ns, topic = MARKETS[market][:2]
    record = json.dumps(
        {
            "event_id": event_id,
            "customer_id": "C001",
            "amount": 125.5,
            "event_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    )
    kubectl(
        "-n",
        kafka_ns,
        "exec",
        "-i",
        "kafka-0",
        "--",
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        topic,
        stdin=record + "\n",
    )


def db_rows(pg_ns, schema, event_id):
    out = kubectl(
        "-n",
        pg_ns,
        "exec",
        "postgres-0",
        "--",
        "psql",
        "-U",
        "admin",
        "-d",
        "market",
        "-Atc",
        f"SELECT count(*) FROM {schema}.trip_events WHERE event_id = '{event_id}'",
    )
    return int(out)


def vault_read_root(market, path):
    root = json.loads((PRIVATE / f"vault-{market}-init.json").read_text())["root_token"]
    return http(
        f"http://vault-{market}.{IP}.sslip.io:31080/v1/{path}",
        headers={"X-Vault-Token": root},
    )["data"]


def gitlab(token, path, method="GET", data=None):
    return http(GITLAB + path, method, data, {"PRIVATE-TOKEN": token})


# ---------------------------------------------------------------- NiFi helpers
class Instance:
    def __init__(self, market):
        self.market = market
        self.api = NiFi(
            f"https://nifi-{market}.{IP}.sslip.io:31443/nifi-api",
            "admin",
            ENV["NIFI_PASSWORD"],
        ).login()
        self.root = self.api.get("/flow/process-groups/root")["processGroupFlow"]["id"]
        self._types = {}

    # -- lookup
    def flow(self, pg):
        return self.api.get(f"/flow/process-groups/{pg}")["processGroupFlow"]["flow"]

    def child_pg(self, parent, name):
        return next(
            (
                p
                for p in self.flow(parent)["processGroups"]
                if p["component"]["name"] == name
            ),
            None,
        )

    def context_id(self, name):
        contexts = self.api.get("/flow/parameter-contexts")["parameterContexts"]
        return next(
            (c["component"]["id"] for c in contexts if c["component"]["name"] == name),
            None,
        )

    def registry_id(self, name):
        clients = self.api.get("/controller/registry-clients")["registries"]
        return next(c["id"] for c in clients if c["component"]["name"] == name)

    def bundle(self, kind, type_name):
        if kind not in self._types:
            path, key = {
                "processor": ("/flow/processor-types", "processorTypes"),
                "service": ("/flow/controller-service-types", "controllerServiceTypes"),
                "provider": (
                    "/flow/parameter-provider-types",
                    "parameterProviderTypes",
                ),
            }[kind]
            self._types[kind] = {
                t["type"]: t["bundle"] for t in self.api.get(path)[key]
            }
        return self._types[kind][type_name]

    # -- creation
    def create_pg(self, parent, name, context=None, x=0, y=0):
        component = {"name": name, "position": {"x": x, "y": y}}
        if context:
            component["parameterContext"] = {"id": context}
        return self.api.post(
            f"/process-groups/{parent}/process-groups",
            {"revision": {"version": 0}, "component": component},
        )["id"]

    def configure(self, path, entity, properties=None, dynamic=None, config=None):
        resolved = NiFi.resolve_properties(
            entity["component"].get("descriptors")
            or entity["component"]["config"]["descriptors"],
            properties or {},
        )
        resolved.update(dynamic or {})
        component = {"id": entity["id"]}
        if "config" in entity["component"]:
            component["config"] = {"properties": resolved, **(config or {})}
        else:
            component["properties"] = resolved
        result = self.api.put(
            path, {"revision": entity["revision"], "component": component}
        )
        return result

    def create_service(self, pg, type_name, name, properties, controller_level=False):
        path = (
            "/controller/controller-services"
            if controller_level
            else f"/process-groups/{pg}/controller-services"
        )
        entity = self.api.post(
            path,
            {
                "revision": {"version": 0},
                "component": {
                    "type": type_name,
                    "bundle": self.bundle("service", type_name),
                    "name": name,
                },
            },
        )
        return entity, properties

    def set_service_properties(self, entity, properties, dynamic=None):
        entity = self.api.get("/controller-services/" + entity["id"])
        return self.configure(
            "/controller-services/" + entity["id"], entity, properties, dynamic
        )

    def create_processor(
        self, pg, type_name, name, x, y, properties=None, dynamic=None, terminate=()
    ):
        entity = self.api.post(
            f"/process-groups/{pg}/processors",
            {
                "revision": {"version": 0},
                "component": {
                    "type": type_name,
                    "bundle": self.bundle("processor", type_name),
                    "name": name,
                    "position": {"x": x, "y": y},
                },
            },
        )
        return self.configure(
            "/processors/" + entity["id"],
            entity,
            properties,
            dynamic,
            {
                "autoTerminatedRelationships": list(terminate),
                "concurrentlySchedulableTaskCount": 1,
            },
        )

    def create_port(self, pg, kind, name, x, y):
        return self.api.post(
            f"/process-groups/{pg}/{kind}-ports",
            {
                "revision": {"version": 0},
                "component": {"name": name, "position": {"x": x, "y": y}},
            },
        )

    def connect(self, pg, source, destination, relationships, name):
        def ref(entity, kind):
            return {
                "id": entity["id"],
                "groupId": entity["component"]["parentGroupId"],
                "type": kind,
            }

        return self.api.post(
            f"/process-groups/{pg}/connections",
            {
                "revision": {"version": 0},
                "component": {
                    "name": name,
                    "source": ref(*source),
                    "destination": ref(*destination),
                    "selectedRelationships": relationships,
                    "flowFileExpiration": "0 sec",
                    "backPressureObjectThreshold": 10000,
                    "backPressureDataSizeThreshold": "1 GB",
                },
            },
        )

    # -- run state
    def services_state(self, pg, state):
        self.api.put(
            f"/flow/process-groups/{pg}/controller-services", {"id": pg, "state": state}
        )
        wanted = "ENABLED" if state == "ENABLED" else "DISABLED"

        def done():
            services = self.api.get(
                f"/flow/process-groups/{pg}/controller-services?includeAncestorGroups=false"
                "&includeDescendantGroups=true"
            )["controllerServices"]
            invalid = [
                s["component"]["name"]
                for s in services
                if s["component"].get("validationErrors")
            ]
            require(
                state != "ENABLED" or not invalid,
                f"invalid services {invalid}: "
                + json.dumps(
                    [s["component"].get("validationErrors") for s in services]
                ),
            )
            return all(s["component"]["state"] == wanted for s in services)

        wait_for(done, f"services {state} in {pg}", 120)

    def pg_state(self, pg, state):
        self.api.put(f"/flow/process-groups/{pg}", {"id": pg, "state": state})

    def processor_state(self, processor_id, state):
        entity = self.api.get("/processors/" + processor_id)
        self.api.put(
            f"/processors/{processor_id}/run-status",
            {"revision": entity["revision"], "state": state},
        )

    def status(self, pg):
        return self.api.get(f"/flow/process-groups/{pg}/status?recursive=true")[
            "processGroupStatus"
        ]["aggregateSnapshot"]

    def drain_and_stop(self, market_runtime, shared, consumer_id):
        self.processor_state(consumer_id, "STOPPED")
        wait_for(
            lambda: self.status(market_runtime)["flowFilesQueued"] == 0,
            "queues drained",
            120,
        )
        self.pg_state(shared, "STOPPED")
        wait_for(
            lambda: self.status(shared)["activeThreadCount"] == 0,
            "shared PG threads stopped",
            60,
        )

    def invalid_components(self, pg):
        bad = []
        flow = self.flow(pg)
        for processor in flow["processors"]:
            if processor["component"].get("validationErrors"):
                bad.append(
                    (
                        processor["component"]["name"],
                        processor["component"]["validationErrors"],
                    )
                )
        for child in flow["processGroups"]:
            bad += self.invalid_components(child["id"])
        return bad

    # -- version control
    def save_version(self, pg, registry, branch, comments, new=False):
        entity = self.api.get("/versions/process-groups/" + pg)
        versioned = {
            "registryId": registry,
            "branch": branch,
            "bucketId": "shared",
            "comments": comments,
            "action": "COMMIT",
        }
        if new:
            versioned |= {
                "flowName": "kafka-to-postgres",
                "description": "Shared Kafka to PostgreSQL flow",
            }
        else:
            versioned["flowId"] = entity["versionControlInformation"]["flowId"]
        result = self.api.post(
            "/versions/process-groups/" + pg,
            {
                "processGroupRevision": entity["processGroupRevision"],
                "versionedFlow": versioned,
            },
        )
        return result["versionControlInformation"]

    def versions(self, registry, branch):
        query = urllib.parse.urlencode({"branch": branch})
        data = self.api.get(
            f"/flow/registries/{registry}/buckets/shared/flows/kafka-to-postgres/versions?{query}"
        )
        return [
            v["versionedFlowSnapshotMetadata"]
            for v in data["versionedFlowSnapshotMetadataSet"]
        ]

    def vci(self, pg):
        return self.api.get("/versions/process-groups/" + pg).get(
            "versionControlInformation"
        )

    def import_flow(self, parent, registry, version, branch="main"):
        query = urllib.parse.urlencode(
            {"parameterContextHandlingStrategy": "KEEP_EXISTING"}
        )
        return self.api.post(
            f"/process-groups/{parent}/process-groups?{query}",
            {
                "revision": {"version": 0},
                "component": {
                    "position": {"x": 0, "y": 0},
                    "versionControlInformation": {
                        "registryId": registry,
                        "bucketId": "shared",
                        "flowId": "kafka-to-postgres",
                        "version": version,
                        "branch": branch,
                    },
                },
            },
        )["id"]

    def change_version(self, pg, version):
        entity = self.api.get("/versions/process-groups/" + pg)
        info = dict(entity["versionControlInformation"])
        info["version"] = version
        request = self.api.post(
            "/versions/update-requests/process-groups/" + pg,
            {
                "processGroupRevision": entity["processGroupRevision"],
                "versionControlInformation": info,
            },
        )["request"]
        self.api.wait_request(
            f"/versions/update-requests/{request['requestId']}", attempts=240
        )

    def processor_by_name(self, pg, name):
        return next(
            p for p in self.flow(pg)["processors"] if p["component"]["name"] == name
        )

    def provenance(self, processor_id):
        """Provenance events of one processor, newest first (ProcessorID is an indexed field)."""
        query = self.api.post(
            "/provenance",
            {
                "provenance": {
                    "request": {
                        "maxResults": 50,
                        "searchTerms": {
                            "ProcessorID": {"value": processor_id, "inverse": False}
                        },
                    }
                }
            },
        )
        ident = query["provenance"]["id"]
        try:
            result = wait_for(
                lambda: (lambda p: p if p["finished"] else None)(
                    self.api.get(f"/provenance/{ident}")["provenance"]
                ),
                "provenance query",
                60,
                1,
            )
        finally:
            self.api.delete(f"/provenance/{ident}")
        return sorted(
            result["results"]["provenanceEvents"],
            key=lambda e: e["eventId"],
            reverse=True,
        )

    def last_event_id(self, processor_id):
        events = self.provenance(processor_id)
        return events[0]["eventId"] if events else -1

    def attributes_after(self, processor_id, after_event_id):
        """Attributes of the newest event of the processor recorded after after_event_id."""
        events = [
            e for e in self.provenance(processor_id) if e["eventId"] > after_event_id
        ]
        if not events:
            return None
        detail = self.api.get(f"/provenance-events/{events[0]['eventId']}")[
            "provenanceEvent"
        ]
        return {a["name"]: a["value"] for a in detail["attributes"]} | {
            "_event": detail["eventType"],
            "_event_id": detail["eventId"],
        }

    # -- cleanup
    def reset(self):
        runtime = self.child_pg(self.root, "MARKET_RUNTIME")
        if runtime:
            pg = runtime["id"]
            self.pg_state(pg, "STOPPED")
            wait_for(
                lambda: self.status(pg)["activeThreadCount"] == 0, "threads stopped", 60
            )
            self.services_state(pg, "DISABLED")
            request = self.api.post(
                f"/process-groups/{pg}/empty-all-connections-requests", {}
            )["dropRequest"]
            self.api.wait_request(
                f"/process-groups/{pg}/empty-all-connections-requests/{request['id']}",
                key="dropRequest",
            )
            entity = self.api.get("/process-groups/" + pg)
            self.api.delete(
                f"/process-groups/{pg}?version={entity['revision']['version']}"
            )
            step(f"{self.market}: removed MARKET_RUNTIME")
        if self.market == "vn":
            for provider in self.api.get("/flow/parameter-providers")[
                "parameterProviders"
            ]:
                if provider["component"]["name"] == "VaultK8sProvider":
                    context = self.context_id("vault-preview")
                    if context:
                        entity = self.api.get("/parameter-contexts/" + context)
                        self.api.delete(
                            f"/parameter-contexts/{context}?version={entity['revision']['version']}"
                        )
                    self.api.delete(
                        f"/parameter-providers/{provider['id']}?version={provider['revision']['version']}"
                    )
            for service in self.api.get("/flow/controller/controller-services")[
                "controllerServices"
            ]:
                if service["component"]["name"] == "VaultK8sClient":
                    self.api.put(
                        f"/controller-services/{service['id']}/run-status",
                        {"revision": service["revision"], "state": "DISABLED"},
                    )
                    wait_for(
                        lambda: self.api.get("/controller-services/" + service["id"])[
                            "component"
                        ]["state"]
                        == "DISABLED",
                        "vault client disabled",
                        60,
                    )
                    entity = self.api.get("/controller-services/" + service["id"])
                    self.api.delete(
                        f"/controller-services/{service['id']}?version={entity['revision']['version']}"
                    )
            step("vn: removed native Vault provider exercise (if present)")


# ------------------------------------------------------------- flow builders
def build_shared(nifi, parent):
    """Shared PG kafka-to-postgres: services, processors and output ports (guide 7.2-7.5)."""
    context = nifi.context_id("shared-runtime")
    require(
        context,
        f"{nifi.market}: shared-runtime context missing; run nifi-sync.sh params",
    )
    pg = nifi.create_pg(parent, "kafka-to-postgres", context, 0, 0)
    kafka, props = nifi.create_service(
        pg,
        "org.apache.nifi.kafka.service.Kafka3ConnectionService",
        "KafkaConnection",
        None,
    )
    nifi.set_service_properties(
        kafka,
        {"Bootstrap Servers": "#{kafka.bootstrap}", "Security Protocol": "PLAINTEXT"},
    )
    pool, _ = nifi.create_service(
        pg, "org.apache.nifi.dbcp.DBCPConnectionPool", "PostgresPool", None
    )
    nifi.set_service_properties(
        pool,
        {
            "Database Connection URL": "#{db.url}",
            "Database Driver Class Name": "org.postgresql.Driver",
            "Database Driver Location(s)": "/opt/nifi/drivers/postgresql.jar",
            "Database User": "#{db.username}",
            "Password": "#{db.password}",
        },
    )
    reader, _ = nifi.create_service(
        pg, "org.apache.nifi.json.JsonTreeReader", "TripJsonReader", None
    )
    nifi.set_service_properties(
        reader,
        {
            "Schema Access Strategy": "Use 'Schema Text' Property",
            "Schema Text": SCHEMA_TEXT,
        },
    )
    consume = nifi.create_processor(
        pg,
        "org.apache.nifi.kafka.processors.ConsumeKafka",
        "ConsumeKafka",
        0,
        0,
        {
            "Kafka Connection Service": kafka["id"],
            "Topics": "#{kafka.topic}",
            "Group ID": "#{kafka.group}",
            "Auto Offset Reset": "earliest",
            "Commit Offsets": "true",
            "Processing Strategy": "FLOW_FILE",
        },
    )
    stamp = nifi.create_processor(
        pg,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "StampFlowVersion",
        0,
        250,
        dynamic={"flow.release": "v1"},
    )
    put = nifi.create_processor(
        pg,
        "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDatabaseRecord",
        0,
        500,
        {
            "Record Reader": reader["id"],
            "Database Connection Pooling Service": pool["id"],
            "Database Type": "PostgreSQL",
            "Statement Type": "UPSERT",
            "Schema Name": "#{db.schema}",
            "Table Name": "#{db.table}",
            "Update Keys": "event_id",
            "Unmatched Field Behavior": "Fail on Unmatched Fields",
            "Rollback On Failure": "false",
        },
    )
    success = nifi.create_port(pg, "output", "success_out", 500, 500)
    error = nifi.create_port(pg, "output", "error_out", 500, 750)
    nifi.connect(
        pg, (consume, "PROCESSOR"), (stamp, "PROCESSOR"), ["success"], "KAFKA_TO_STAMP"
    )
    nifi.connect(
        pg, (stamp, "PROCESSOR"), (put, "PROCESSOR"), ["success"], "STAMP_TO_DB"
    )
    nifi.connect(
        pg, (put, "PROCESSOR"), (success, "OUTPUT_PORT"), ["success"], "DB_SUCCESS"
    )
    nifi.connect(
        pg, (put, "PROCESSOR"), (error, "OUTPUT_PORT"), ["retry", "failure"], "DB_ERROR"
    )
    return pg


def build_local(nifi, parent, shared, local_market):
    """LOCAL_AUDIT (guide 7.6) and the two parent connections (7.7). Never versioned."""
    local = nifi.create_pg(parent, "LOCAL_AUDIT", None, 700, 0)
    in_success = nifi.create_port(local, "input", "in_success", 0, 0)
    in_error = nifi.create_port(local, "input", "in_error", 0, 400)
    tag = nifi.create_processor(
        local,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "LocalMarketTag",
        400,
        0,
        dynamic={"local.market": local_market},
    )
    audit = nifi.create_processor(
        local,
        "org.apache.nifi.processors.standard.LogAttribute",
        "AuditSuccess",
        400,
        250,
        {"Log Payload": "false"},
        terminate=["success"],
    )
    funnel = nifi.api.post(
        f"/process-groups/{local}/funnels",
        {"revision": {"version": 0}, "component": {"position": {"x": 400, "y": 450}}},
    )
    nifi.connect(
        local, (in_success, "INPUT_PORT"), (tag, "PROCESSOR"), [], "IN_SUCCESS"
    )
    nifi.connect(
        local, (tag, "PROCESSOR"), (audit, "PROCESSOR"), ["success"], "TAG_TO_AUDIT"
    )
    nifi.connect(
        local, (in_error, "INPUT_PORT"), (funnel, "FUNNEL"), [], "FAILED_RECORDS"
    )
    ports = nifi.flow(shared)["outputPorts"]
    out = {p["component"]["name"]: p for p in ports}
    nifi.connect(
        parent,
        (out["success_out"], "OUTPUT_PORT"),
        (in_success, "INPUT_PORT"),
        [],
        "SHARED_SUCCESS_TO_LOCAL",
    )
    nifi.connect(
        parent,
        (out["error_out"], "OUTPUT_PORT"),
        (in_error, "INPUT_PORT"),
        [],
        "SHARED_ERROR_TO_LOCAL",
    )
    return local, audit["id"]


def start_runtime(nifi, runtime, shared):
    nifi.services_state(shared, "ENABLED")
    try:
        # Processor validation is cached; give it a moment after services become ENABLED.
        wait_for(
            lambda: not nifi.invalid_components(runtime), "components valid", 60, 3
        )
    except AssertionError:
        raise AssertionError(
            f"{nifi.market}: invalid components {nifi.invalid_components(runtime)}"
        )
    nifi.pg_state(runtime, "RUNNING")


def prove_event(nifi, market, event_id, audit_id, release):
    kafka_ns, topic, pg_ns, schema = MARKETS[market][:4]
    before = nifi.last_event_id(audit_id)
    produce(market, event_id)
    wait_for(
        lambda: db_rows(pg_ns, schema, event_id) == 1,
        f"{event_id} in {pg_ns}/{schema}",
        120,
        3,
    )
    attributes = wait_for(
        lambda: nifi.attributes_after(audit_id, before), "audit provenance", 90, 3
    )
    require(
        attributes.get("flow.release") == release
        and attributes.get("local.market") == MARKETS[market][6],
        f"{market}: audit attributes {attributes.get('flow.release')}/{attributes.get('local.market')}",
    )
    others = [
        (ns, s)
        for ns, s in [
            ("postgres-vn", "staging"),
            ("postgres-vn", "market"),
            ("postgres-indo", "market"),
        ]
        if (ns, s) != (pg_ns, schema)
    ]
    leaked = [f"{ns}/{s}" for ns, s in others if db_rows(ns, s, event_id)]
    require(not leaked, f"{event_id} leaked into {leaked}")
    step(
        f"{market}: {event_id} via {kafka_ns}/{topic} -> {pg_ns}/{schema} (1 row, not elsewhere); "
        f"audit flow.release={release} local.market={MARKETS[market][6]} "
        f"(provenance {attributes['_event']} #{attributes['_event_id']})",
        event=event_id,
    )


def set_marker(nifi, shared, value):
    stamp = nifi.processor_by_name(shared, "StampFlowVersion")
    entity = nifi.api.get("/processors/" + stamp["id"])
    nifi.api.put(
        "/processors/" + stamp["id"],
        {
            "revision": entity["revision"],
            "component": {
                "id": stamp["id"],
                "config": {"properties": {"flow.release": value}},
            },
        },
    )


def marker(nifi, shared):
    return nifi.processor_by_name(shared, "StampFlowVersion")["component"]["config"][
        "properties"
    ]["flow.release"]


# --------------------------------------------------------------- GitLab flow
def promote(writer, maintainer, title, description):
    """MR staging -> main; checks that the writer PrAT cannot merge; merges as Maintainer."""
    mr = gitlab(
        writer,
        f"/projects/{PROJECT}/merge_requests",
        "POST",
        {
            "source_branch": "staging",
            "target_branch": "main",
            "title": title,
            "description": description,
            "remove_source_branch": False,
            "squash": False,
        },
    )
    iid = mr["iid"]
    author = f"{mr['author']['name']} ({mr['author']['username']})"
    wait_for(
        lambda: gitlab(maintainer, f"/projects/{PROJECT}/merge_requests/{iid}")[
            "detailed_merge_status"
        ]
        == "mergeable",
        "MR mergeable",
        120,
        3,
    )
    try:
        gitlab(writer, f"/projects/{PROJECT}/merge_requests/{iid}/merge", "PUT", {})
        raise AssertionError("writer PrAT (Developer) merged into protected main")
    except HttpError as error:
        require(
            error.code in (401, 403, 405),
            f"unexpected writer merge response {error.code}",
        )
    merged = gitlab(
        maintainer,
        f"/projects/{PROJECT}/merge_requests/{iid}/merge",
        "PUT",
        {"should_remove_source_branch": False, "squash": False},
    )
    require(merged["state"] == "merged", "MR not merged")
    commits = gitlab(
        maintainer,
        f"/projects/{PROJECT}/repository/commits?"
        + urllib.parse.urlencode({"ref_name": "main", "path": SNAPSHOT, "per_page": 5}),
    )
    step(
        f"GitLab MR !{iid} by {author}: writer merge denied, merged by {merged['merged_by']['name']}; "
        f"main snapshot SHA {commits[0]['id']}",
        mr=iid,
        sha=commits[0]["id"],
    )
    return commits[0]["id"]


def check_snapshot(writer, ref):
    raw = http(
        f"{GITLAB}/projects/{PROJECT}/repository/files/{urllib.parse.quote(SNAPSHOT, safe='')}/raw?ref={ref}",
        headers={"PRIVATE-TOKEN": writer},
    )
    path = PRIVATE / f"e2e-snapshot-{ref}.json"
    path.write_text(raw if isinstance(raw, str) else json.dumps(raw))
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "repo-template/scripts/check_snapshot.py"),
            str(path),
        ],
        capture_output=True,
        text=True,
    )
    require(result.returncode == 0, "snapshot guard failed: " + result.stderr)
    text = path.read_text()

    def property_values(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "properties" and isinstance(value, dict):
                    yield from (v for v in value.values() if isinstance(v, str))
                yield from property_values(value)
        elif isinstance(node, list):
            for value in node:
                yield from property_values(value)

    values = set(property_values(json.loads(text)))
    require(
        "LOCAL_AUDIT" not in text
        and "svc.cluster.local" not in text
        and ENV["DB_PASSWORD"] not in values,
        "snapshot contains local components or environment values",
    )
    step(f"snapshot {SNAPSHOT}@{ref}: {result.stdout.strip()}")


# -------------------------------------------------------- native Vault provider
def native_provider(nifi):
    """Guide extension: NiFi itself reads Vault with its pod ServiceAccount (KUBERNETES auth)."""
    client, _ = nifi.create_service(
        None,
        "org.apache.nifi.vault.hashicorp.StandardHashiCorpVaultClientService",
        "VaultK8sClient",
        None,
        controller_level=True,
    )
    entity = nifi.set_service_properties(
        client,
        {
            "Configuration Strategy": "Direct Properties",
            "Vault URI": "http://vault.vault-vn.svc.cluster.local:8200",
            "Vault Authentication": "KUBERNETES",
        },
        {
            "vault.kubernetes.role": "nifi-vn",
            "vault.kubernetes.service-account-token-file": "/var/run/secrets/vault/token",
        },
    )
    require(
        not entity["component"].get("validationErrors"),
        f"vault client invalid {entity['component']}",
    )
    nifi.api.put(
        f"/controller-services/{client['id']}/run-status",
        {"revision": entity["revision"], "state": "ENABLED"},
    )
    wait_for(
        lambda: nifi.api.get("/controller-services/" + client["id"])["component"][
            "state"
        ]
        == "ENABLED",
        "VaultK8sClient enabled",
        60,
    )
    provider = nifi.api.post(
        "/controller/parameter-providers",
        {
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.vault.hashicorp.HashiCorpVaultParameterProvider",
                "bundle": nifi.bundle(
                    "provider",
                    "org.apache.nifi.vault.hashicorp.HashiCorpVaultParameterProvider",
                ),
                "name": "VaultK8sProvider",
            },
        },
    )
    provider = nifi.configure(
        "/parameter-providers/" + provider["id"],
        provider,
        {
            "HashiCorp Vault Client Service": client["id"],
            "Key/Value Path": "kv",
            "Key/Value Version": "KV_1",
            "Secret Name Pattern": "^preview$",
        },
    )
    fetched = nifi.api.post(
        f"/parameter-providers/{provider['id']}/parameters/fetch-requests",
        {"id": provider["id"], "revision": provider["revision"]},
    )
    groups = fetched["component"]["parameterGroupConfigurations"]
    require([g["groupName"] for g in groups] == ["preview"], f"fetched groups {groups}")
    config = {
        "groupName": "preview",
        "parameterContextName": "vault-preview",
        "synchronized": True,
        "parameterSensitivities": {
            k: ("SENSITIVE" if k == "db.password" else "NON_SENSITIVE")
            for k in groups[0]["parameterSensitivities"]
        },
    }
    request = nifi.api.post(
        f"/parameter-providers/{provider['id']}/apply-parameters-requests",
        {
            "id": provider["id"],
            "revision": fetched["revision"],
            "parameterGroupConfigurations": [config],
        },
    )["request"]
    nifi.api.wait_request(
        f"/parameter-providers/{provider['id']}/apply-parameters-requests/{request['requestId']}"
    )
    context = nifi.api.get("/parameter-contexts/" + nifi.context_id("vault-preview"))[
        "component"
    ]
    values = {p["parameter"]["name"]: p["parameter"] for p in context["parameters"]}
    require(
        len(values) == 8
        and values["kafka.group"]["value"] == "nifi-vn-preview"
        and values["db.password"]["sensitive"],
        f"vault-preview parameters {values}",
    )
    require(
        context.get("parameterProviderConfiguration"),
        "vault-preview lacks provider association",
    )
    step(
        "vn: native HashiCorpVaultParameterProvider (Vault Authentication=KUBERNETES, role nifi-vn, "
        "projected ServiceAccount token) fetched kv/preview -> context vault-preview (8 params)"
    )


# ------------------------------------------------------------------- scenario
def main():
    args = set(sys.argv[1:])
    nifi = {m: Instance(m) for m in MARKETS}
    if args & {"--reset", "--reset-only"}:
        for instance in nifi.values():
            instance.reset()
        if "--reset-only" in args:
            return
    writer = vault_read_root("vn", "kv/gitlab-registry/staging")["access_token"]
    reader = vault_read_root("vn", "kv/gitlab-registry/vn")["access_token"]
    release_file = PRIVATE / "gitlab-release-token.json"
    require(
        release_file.exists(),
        "run k8s/scripts/gitlab-bootstrap.py --release-token first",
    )
    maintainer = json.loads(release_file.read_text())["token"]
    for m, instance in nifi.items():
        require(
            not instance.child_pg(instance.root, "MARKET_RUNTIME"),
            f"{m}: MARKET_RUNTIME exists; use --reset",
        )

    # 1. Staging authoring
    stag = nifi["stag"]
    runtime = {"stag": stag.create_pg(stag.root, "MARKET_RUNTIME")}
    shared = {"stag": build_shared(stag, runtime["stag"])}
    _, audit_stag = build_local(stag, runtime["stag"], shared["stag"], "STAG")
    start_runtime(stag, runtime["stag"], shared["stag"])
    step(
        "stag: built MARKET_RUNTIME/{kafka-to-postgres, LOCAL_AUDIT}, services enabled, flow running"
    )
    prove_event(stag, "stag", f"stag-k8s-v1-{RUN}", audit_stag, "v1")

    # 2. Save Version v1 to GitLab staging (writer PrAT through NiFi)
    consumer = {"stag": stag.processor_by_name(shared["stag"], "ConsumeKafka")["id"]}
    stag.drain_and_stop(runtime["stag"], shared["stag"], consumer["stag"])
    registry_stag = stag.registry_id("gitlab-staging")
    s1 = stag.save_version(
        shared["stag"], registry_stag, "staging", "v1: kafka to postgres", new=True
    )
    commits = gitlab(
        writer,
        f"/projects/{PROJECT}/repository/commits?"
        + urllib.parse.urlencode(
            {"ref_name": "staging", "path": SNAPSHOT, "per_page": 1}
        ),
    )
    author = commits[0]["author_name"]
    step(
        f"stag: Save Version v1 -> GitLab staging S1={s1['version']} (commit author {author})",
        sha=s1["version"],
    )
    check_snapshot(writer, "staging")
    try:
        gitlab(
            reader,
            f"/projects/{PROJECT}/repository/files/{urllib.parse.quote('flows/shared/x.txt', safe='')}",
            "POST",
            {"branch": "staging", "content": "x", "commit_message": "should fail"},
        )
        raise AssertionError("reader PrAT (read_api) could write to the repository")
    except HttpError as error:
        require(
            error.code in (401, 403), f"unexpected reader write response {error.code}"
        )
    step("GitLab: reader PrAT (Reporter, read_api) write attempt denied")
    stag.pg_state(shared["stag"], "RUNNING")

    # 3. Promote v1
    m1 = promote(
        writer,
        maintainer,
        "Promote kafka-to-postgres v1",
        f"Test stag-k8s-v1-{RUN}: staging row + audit v1/STAG. S1 {s1['version']}. Snapshot guard PASS.",
    )

    # 4. Import M1 into VN, then INDO
    for m in ("vn", "indo"):
        n = nifi[m]
        registry = n.registry_id("gitlab-main")
        listed = [v["version"] for v in n.versions(registry, "main")]
        require(m1 in listed, f"{m}: M1 not listed by gitlab-main: {listed}")
        runtime[m] = n.create_pg(n.root, "MARKET_RUNTIME")
        shared[m] = n.import_flow(runtime[m], registry, m1)
        bound = n.api.get("/process-groups/" + shared[m])["component"][
            "parameterContext"
        ]["component"]["name"]
        require(bound == "shared-runtime", f"{m}: imported PG bound to {bound}")
        _, audit = build_local(n, runtime[m], shared[m], MARKETS[m][6])
        start_runtime(n, runtime[m], shared[m])
        consumer[m] = n.processor_by_name(shared[m], "ConsumeKafka")["id"]
        step(
            f"{m}: imported kafka-to-postgres @ M1 from main via gitlab-main (reader PrAT), "
            f"bound local shared-runtime, LOCAL_AUDIT local.market={MARKETS[m][6]}"
        )
        prove_event(n, m, f"{m}-k8s-v1-{RUN}", audit, "v1")

    # 5. v2 on staging -> M2; VN Change Version, INDO stays; VN rollback
    stag.drain_and_stop(runtime["stag"], shared["stag"], consumer["stag"])
    set_marker(stag, shared["stag"], "v2")
    s2 = stag.save_version(
        shared["stag"], registry_stag, "staging", "v2: release marker"
    )
    step(f"stag: Commit Local Changes v2 -> S2={s2['version']}", sha=s2["version"])
    stag.pg_state(shared["stag"], "RUNNING")
    prove_event(
        stag,
        "stag",
        f"stag-k8s-v2-{RUN}",
        stag.processor_by_name(
            stag.child_pg(runtime["stag"], "LOCAL_AUDIT")["id"], "AuditSuccess"
        )["id"],
        "v2",
    )
    check_snapshot(writer, "staging")
    m2 = promote(
        writer,
        maintainer,
        "Promote kafka-to-postgres v2",
        f"stag-k8s-v2-{RUN} audit v2/STAG. S2 {s2['version']}.",
    )

    def audit_id(m):
        return nifi[m].processor_by_name(
            nifi[m].child_pg(runtime[m], "LOCAL_AUDIT")["id"], "AuditSuccess"
        )["id"]

    vn = nifi["vn"]
    vn.drain_and_stop(runtime["vn"], shared["vn"], consumer["vn"])
    vn.change_version(shared["vn"], m2)
    vn.services_state(shared["vn"], "ENABLED")
    vn.pg_state(runtime["vn"], "RUNNING")
    require(
        vn.vci(shared["vn"])["version"] == m2 and marker(vn, shared["vn"]) == "v2",
        "vn not on M2/v2",
    )
    step(
        "vn: Change Version M1 -> M2 (context shared-runtime and LOCAL_AUDIT unchanged)"
    )
    prove_event(vn, "vn", f"vn-k8s-v2-{RUN}", audit_id("vn"), "v2")
    indo = nifi["indo"]
    require(
        indo.vci(shared["indo"])["version"] == m1
        and marker(indo, shared["indo"]) == "v1",
        "indo moved",
    )
    prove_event(indo, "indo", f"indo-k8s-still-v1-{RUN}", audit_id("indo"), "v1")
    step("indo: still M1/v1 after merge of M2 (no automatic rollout)")

    vn.drain_and_stop(runtime["vn"], shared["vn"], consumer["vn"])
    vn.change_version(shared["vn"], m1)
    vn.services_state(shared["vn"], "ENABLED")
    vn.pg_state(runtime["vn"], "RUNNING")
    require(
        vn.vci(shared["vn"])["version"] == m1 and marker(vn, shared["vn"]) == "v1",
        "vn rollback failed",
    )
    prove_event(vn, "vn", f"vn-k8s-rollback-v1-{RUN}", audit_id("vn"), "v1")
    step("vn: rollback M2 -> M1 by Change Version; INDO untouched")

    # 6. Vault -> shared-runtime update while the flow runs (Job with ServiceAccount, update path)
    resync = subprocess.run(
        ["bash", str(ROOT / "k8s/scripts/nifi-sync.sh"), "params", "vn"],
        capture_output=True,
        text=True,
    )
    require(
        resync.returncode == 0 and "Updated shared-runtime" in resync.stdout,
        "params re-sync failed: " + resync.stdout + resync.stderr,
    )
    step(
        "vn: Job vault-param-sync (ServiceAccount nifi-vn/nifi) updated shared-runtime while running"
    )
    prove_event(vn, "vn", f"vn-k8s-after-resync-{RUN}", audit_id("vn"), "v1")

    # 7. Native provider with Kubernetes auth (NiFi VN)
    native_provider(vn)

    EVIDENCE.update({"S1": s1["version"], "M1": m1, "S2": s2["version"], "M2": m2})
    out = PRIVATE / f"e2e-evidence-{RUN}.json"
    out.write_text(json.dumps(EVIDENCE, indent=2))
    print(f"E2E PASSED. Evidence: {out.relative_to(ROOT)}  M1={m1} M2={m2}")


if __name__ == "__main__":
    main()
