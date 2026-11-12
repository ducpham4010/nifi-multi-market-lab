"""Static checks of the Kubernetes manifests and offline tests of the sync logic.

Run: python3 tests/test_k8s_manifests.py   (manifest tests need kubectl + PyYAML)
"""

import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock as mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "k8s/nifi/base"))
import nifi_k8s_sync as sync  # noqa: E402

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def render():
    output = subprocess.run(
        ["kubectl", "kustomize", str(ROOT / "k8s")],
        capture_output=True,
        text=True,
        check=True,
    )
    return [d for d in yaml.safe_load_all(output.stdout) if d]


def pod_spec(doc):
    if doc["kind"] in ("StatefulSet", "Job", "Deployment"):
        return doc["spec"]["template"]["spec"]
    if doc["kind"] == "CronJob":
        return doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    return None


@unittest.skipUnless(shutil.which("kubectl") and yaml, "requires kubectl and PyYAML")
class ManifestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docs = render()

    def by_kind(self, kind):
        return [d for d in self.docs if d["kind"] == kind]

    def test_one_namespace_per_service(self):
        workloads = {
            (d["metadata"]["namespace"], d["metadata"]["name"])
            for d in self.by_kind("StatefulSet")
        }
        namespaces = [ns for ns, _ in workloads]
        self.assertEqual(len(workloads), 10)
        self.assertEqual(
            len(set(namespaces)), 10, "each StatefulSet must live in its own namespace"
        )
        declared = {d["metadata"]["name"] for d in self.by_kind("Namespace")}
        self.assertTrue(set(namespaces) <= declared)

    def test_no_secrets_or_static_vault_tokens_in_git(self):
        self.assertEqual(
            self.by_kind("Secret"), [], "Secrets are created from .env by deploy.sh"
        )
        text = "\n".join(
            p.read_text()
            for p in (ROOT / "k8s").rglob("*")
            if p.is_file() and "__pycache__" not in p.parts
        )
        for forbidden in (
            "nifi-vn-lab-token",
            "nifi-indo-lab-token",
            "glpat-",
            "hvs.",
            "X-Vault-Token: nifi",
        ):
            self.assertNotIn(forbidden, text)

    def test_nifi_uses_serviceaccount_token_with_vault_audience(self):
        for doc in self.docs:
            spec = pod_spec(doc)
            if not spec or not doc["metadata"]["namespace"].startswith("nifi-"):
                continue
            self.assertEqual(
                spec["serviceAccountName"], "nifi", doc["metadata"]["name"]
            )
            self.assertFalse(spec["automountServiceAccountToken"])
            tokens = [
                s["serviceAccountToken"]
                for v in spec["volumes"]
                if "projected" in v
                for s in v["projected"]["sources"]
                if "serviceAccountToken" in s
            ]
            self.assertEqual(
                [t["audience"] for t in tokens], ["vault"], doc["metadata"]["name"]
            )
        accounts = [
            d for d in self.by_kind("ServiceAccount") if d["metadata"]["name"] == "nifi"
        ]
        self.assertEqual(len(accounts), 3)

    def test_sync_jobs_are_manual_and_target_own_role(self):
        cronjobs = self.by_kind("CronJob")
        self.assertEqual(len(cronjobs), 6)
        self.assertTrue(all(c["spec"]["suspend"] for c in cronjobs))
        envs = {
            d["metadata"]["namespace"]: d["data"]
            for d in self.by_kind("ConfigMap")
            if d["metadata"]["name"].startswith("nifi-sync-env")
        }
        self.assertEqual(envs["nifi-stag"]["VAULT_ROLE"], "nifi-stag")
        self.assertEqual(envs["nifi-stag"]["REGISTRY_BRANCH"], "staging")
        self.assertEqual(envs["nifi-vn"]["VAULT_PARAMS_PATH"], "kv/runtime")
        self.assertIn("vault-indo", envs["nifi-indo"]["VAULT_ADDR"])
        for ns, data in envs.items():
            self.assertEqual(
                data["REGISTRY_CLIENT_NAME"],
                "gitlab-staging" if ns == "nifi-stag" else "gitlab-main",
            )

    def test_lan_exposure(self):
        tls = {
            d["metadata"]["namespace"]: d["spec"]["hostnames"]
            for d in self.by_kind("TLSRoute")
        }
        self.assertEqual(sorted(tls), ["nifi-indo", "nifi-stag", "nifi-vn"])
        envs = {
            d["metadata"]["namespace"]: d["data"]
            for d in self.by_kind("ConfigMap")
            if d["metadata"]["name"].startswith("nifi-env")
        }
        for ns, hosts in tls.items():
            self.assertEqual(hosts, [envs[ns]["NIFI_BROWSER_HOST"]])
            self.assertIn(
                hosts[0] + ":31443", envs[ns]["NIFI_WEB_PROXY_HOST"].split(",")
            )
            self.assertIn(
                f"dns:nifi.{ns}.svc.cluster.local", envs[ns]["NIFI_TLS_EXTRA_SANS"]
            )
        http = sorted(
            h for d in self.by_kind("HTTPRoute") for h in d["spec"]["hostnames"]
        )
        self.assertEqual(
            http,
            sorted(
                f"{n}.192.168.102.4.sslip.io"
                for n in ("gitlab", "vault-vn", "vault-indo")
            ),
        )
        node_ports = sorted(
            p["nodePort"]
            for d in self.by_kind("Service")
            if d["spec"].get("type") == "NodePort"
            for p in d["spec"]["ports"]
        )
        self.assertEqual(node_ports, [31022, 31092, 31432, 32092, 32432])
        policy = self.by_kind("ClientTrafficPolicy")[0]["spec"]
        self.assertEqual(
            policy["path"]["escapedSlashesAction"],
            "KeepUnchanged",
            "GitLab needs %2F intact",
        )
        proxy = self.by_kind("EnvoyProxy")[0]["spec"]["provider"]["kubernetes"][
            "envoyService"
        ]
        self.assertEqual(
            sorted(p["nodePort"] for p in proxy["patch"]["value"]["spec"]["ports"]),
            [31080, 31443],
        )

    def test_postgres_init_matches_compose(self):
        self.assertEqual(
            (ROOT / "k8s/postgres/base/init.sql").read_text(),
            (ROOT / "sql/init.sql").read_text(),
        )

    def test_image_tag_matches_deploy_script(self):
        base = yaml.safe_load((ROOT / "k8s/nifi/base/kustomization.yaml").read_text())[
            "images"
        ][0]
        self.assertIn(
            f'{base["newName"]}:{base["newTag"]}',
            (ROOT / "k8s/scripts/common.sh").read_text(),
        )


class FakeResponse:
    def __init__(self, data):
        self.data = data if isinstance(data, bytes) else json.dumps(data).encode()
        self.headers = {
            "Content-Type": (
                "application/json" if not isinstance(data, bytes) else "text/plain"
            )
        }

    def read(self):
        return self.data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class SyncLogicTest(unittest.TestCase):
    """Offline: Vault Kubernetes login payload, contract check, context creation, registry properties."""

    def setUp(self):
        self.calls = []
        self.values = {k: "x" for k in sync.REQUIRED_PARAMS}

    def fake(self, request, **kwargs):
        url, method = request.full_url, request.get_method()
        body = (
            json.loads(request.data)
            if request.data
            and request.headers.get("Content-type") == "application/json"
            else None
        )
        self.calls.append((method, url, body, dict(request.headers)))
        if url.endswith("/v1/auth/kubernetes/login"):
            return FakeResponse(
                {
                    "auth": {
                        "client_token": "s.child",
                        "token_policies": ["default", "nifi-vn"],
                        "lease_duration": 3600,
                        "metadata": {
                            "role": "nifi-vn",
                            "service_account_name": "nifi",
                            "service_account_namespace": "nifi-vn",
                        },
                    }
                }
            )
        if url.endswith("/v1/kv/runtime"):
            return FakeResponse({"data": self.values})
        if url.endswith("/v1/auth/token/revoke-self"):
            return FakeResponse(b"")
        if url.endswith("/access/token"):
            return FakeResponse(b"jwt")
        if url.endswith("/flow/parameter-contexts"):
            return FakeResponse({"parameterContexts": []})
        if url.endswith("/parameter-contexts") and method == "POST":
            return FakeResponse({"component": {"id": "ctx-1"}})
        raise AssertionError(f"unexpected {method} {url}")

    def test_params_sync_uses_serviceaccount_jwt_and_creates_context(self):
        with tempfile.NamedTemporaryFile("w") as jwt, mock.patch(
            "urllib.request.urlopen", self.fake
        ):
            jwt.write("sa.jwt.value\n")
            jwt.flush()
            vault = sync.Vault("http://vault:8200", "nifi-vn", jwt_file=jwt.name)
            vault.login()
            nifi = sync.NiFi("https://nifi:8443/nifi-api", "admin", "pw").login()
            sync.sync_params(vault, nifi, "kv/runtime")
            vault.revoke()
        login = self.calls[0]
        self.assertEqual(login[2], {"role": "nifi-vn", "jwt": "sa.jwt.value"})
        self.assertNotIn("X-vault-token", login[3])
        read = next(c for c in self.calls if c[1].endswith("/v1/kv/runtime"))
        self.assertEqual(read[3]["X-vault-token"], "s.child")
        created = next(
            c
            for c in self.calls
            if c[1].endswith("/parameter-contexts") and c[0] == "POST"
        )
        params = {
            p["parameter"]["name"]: p["parameter"]["sensitive"]
            for p in created[2]["component"]["parameters"]
        }
        self.assertEqual(set(params), sync.REQUIRED_PARAMS)
        self.assertEqual([k for k, v in params.items() if v], ["db.password"])
        self.assertTrue(self.calls[-1][1].endswith("/revoke-self"))

    def test_update_request_carries_entity_id(self):
        existing = {
            "id": "ctx-1",
            "revision": {"version": 3},
            "component": {
                "id": "ctx-1",
                "name": "shared-runtime",
                "parameters": [
                    {"parameter": {"name": k, "sensitive": k == "db.password"}}
                    for k in sync.REQUIRED_PARAMS
                ],
            },
        }
        fake = self.fake

        def routed(request, **kwargs):
            url, method = request.full_url, request.get_method()
            if url.endswith("/flow/parameter-contexts"):
                return FakeResponse(
                    {
                        "parameterContexts": [
                            {"component": {"id": "ctx-1", "name": "shared-runtime"}}
                        ]
                    }
                )
            if url.endswith("/parameter-contexts/ctx-1"):
                return FakeResponse(existing)
            if url.endswith("/update-requests") and method == "POST":
                self.calls.append((method, url, json.loads(request.data), {}))
                return FakeResponse({"request": {"requestId": "r1"}})
            if "/update-requests/r1" in url:
                self.calls.append((method, url, None, {}))
                return FakeResponse({"request": {"complete": True}})
            return fake(request, **kwargs)

        with tempfile.NamedTemporaryFile("w") as jwt, mock.patch(
            "urllib.request.urlopen", routed
        ):
            jwt.write("t")
            jwt.flush()
            vault = sync.Vault("http://vault:8200", "nifi-vn", jwt_file=jwt.name)
            vault.login()
            nifi = sync.NiFi("https://nifi:8443/nifi-api", "admin", "pw").login()
            sync.sync_params(vault, nifi, "kv/runtime")
        body = next(c[2] for c in self.calls if c[1].endswith("/update-requests"))
        # NiFi 2.6 ParameterUpdateManager: "The componentId must be specified" without entity-level id.
        self.assertEqual(body["id"], "ctx-1")
        self.assertEqual(body["revision"], {"version": 3})
        self.assertTrue(
            any(c[0] == "DELETE" and "/update-requests/r1" in c[1] for c in self.calls)
        )

    def test_contract_violation_stops_before_nifi_changes(self):
        self.values["extra.key"] = "y"
        with tempfile.NamedTemporaryFile("w") as jwt, mock.patch(
            "urllib.request.urlopen", self.fake
        ):
            jwt.write("t")
            jwt.flush()
            vault = sync.Vault("http://vault:8200", "nifi-vn", jwt_file=jwt.name)
            vault.login()
            nifi = sync.NiFi("https://nifi:8443/nifi-api", "admin", "pw").login()
            with self.assertRaises(SystemExit):
                sync.sync_params(vault, nifi, "kv/runtime")
        self.assertFalse(any("parameter-contexts" in c[1] for c in self.calls))

    def test_registry_properties_resolve_display_names_and_values(self):
        descriptors = {
            "url": {"name": "url", "displayName": "GitLab API URL"},
            "branch": {"name": "branch", "displayName": "Default Branch"},
            "auth": {
                "name": "auth",
                "displayName": "Authentication Type",
                "allowableValues": [
                    {
                        "allowableValue": {
                            "value": "ACCESS_TOKEN",
                            "displayName": "Access Token",
                        }
                    }
                ],
            },
            "params": {
                "name": "params",
                "displayName": "Parameter Context Values",
                "allowableValues": [
                    {"allowableValue": {"value": "RETAIN", "displayName": "Retain"}},
                    {"allowableValue": {"value": "REMOVE", "displayName": "Remove"}},
                ],
            },
        }
        resolved = sync.NiFi.resolve_properties(
            descriptors,
            {
                "GitLab API URL": "http://gitlab.gitlab.svc.cluster.local/",
                ("Default Branch", "Repository Branch"): "main",
                "Authentication Type": "Access Token",
                "Parameter Context Values": "Remove",
            },
        )
        self.assertEqual(
            resolved,
            {
                "url": "http://gitlab.gitlab.svc.cluster.local/",
                "branch": "main",
                "auth": "ACCESS_TOKEN",
                "params": "REMOVE",
            },
        )
        with self.assertRaises(RuntimeError):
            sync.NiFi.resolve_properties(
                descriptors, {"Parameter Context Values": "Keep"}
            )


class SnapshotGuardTest(unittest.TestCase):
    def test_kubernetes_service_dns_is_rejected(self):
        snapshot = {
            "flowContents": {
                "processors": [
                    {
                        "name": "ConsumeKafka",
                        "properties": {
                            "bootstrap.servers": "kafka.kafka-vn.svc.cluster.local:9092"
                        },
                    }
                ]
            }
        }
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "flow.json"
            path.write_text(json.dumps(snapshot))
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "repo-template/scripts/check_snapshot.py"),
                    str(path),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("environment literal", result.stderr)
            snapshot["flowContents"]["processors"][0]["properties"][
                "bootstrap.servers"
            ] = "#{kafka.bootstrap}"
            path.write_text(json.dumps(snapshot))
            ok = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "repo-template/scripts/check_snapshot.py"),
                    str(path),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(ok.returncode, 0, ok.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)


FAKE_KUBECTL = """#!/usr/bin/env bash
echo "$*" >> "$KUBECTL_LOG"
case "$*" in
  "get ns -l app.kubernetes.io/part-of=nifi-market-lab -o jsonpath="*) printf 'nifi-vn vault-vn' ;;
  "-n "*" get pvc --no-headers") echo "data-0 Bound pvc-1 1Gi RWO local-path 1d" ;;
  *) : ;;
esac
"""


class LifecycleScriptTest(unittest.TestCase):
    """down.sh / up.sh: confirmation, archive of stale Vault keys, step order."""

    def test_shell_scripts_parse(self):
        for script in sorted((ROOT / "k8s/scripts").glob("*.sh")):
            with self.subTest(script=script.name):
                subprocess.run(["bash", "-n", str(script)], check=True)

    def test_up_runs_bootstrap_steps_in_order(self):
        text = (ROOT / "k8s/scripts/up.sh").read_text()
        body = text[text.index("step \"1/6") :]
        steps = [
            "deploy.sh",
            "vault-init.py",
            "gitlab-bootstrap.py",
            "nifi-sync.sh\" params all",
            "nifi-sync.sh\" registry all",
            "check-k8s.py",
        ]
        positions = [body.index(s) for s in steps]
        self.assertEqual(positions, sorted(positions))

    def run_down(self, *args):
        directory = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory)
        scripts = directory / "k8s/scripts"
        scripts.mkdir(parents=True)
        for name in ("common.sh", "down.sh"):
            shutil.copy(ROOT / "k8s/scripts" / name, scripts / name)
        (directory / ".env").write_text("HOST_IP=192.0.2.10\n")
        keys = directory / ".private/k8s"
        keys.mkdir(parents=True)
        (keys / "vault-vn-init.json").write_text("{}")
        bin_dir = directory / "bin"
        bin_dir.mkdir()
        (bin_dir / "kubectl").write_text(FAKE_KUBECTL)
        (bin_dir / "kubectl").chmod(0o755)
        log = directory / "kubectl.log"
        result = subprocess.run(
            ["bash", str(scripts / "down.sh"), *args],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "KUBECTL_LOG": str(log),
            },
        )
        calls = log.read_text().splitlines() if log.exists() else []
        return result, calls, directory

    def test_down_without_confirmation_deletes_nothing(self):
        result, calls, directory = self.run_down()
        self.assertEqual(result.returncode, 1)
        self.assertIn("nifi-vn vault-vn", result.stdout)
        self.assertIn("pvc nifi-vn/data-0 (1Gi)", result.stdout)
        self.assertFalse(any(c.startswith("delete") for c in calls), calls)
        self.assertTrue((directory / ".private/k8s/vault-vn-init.json").exists())

    def test_down_yes_deletes_lab_and_archives_vault_keys(self):
        result, calls, directory = self.run_down("--yes")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"delete -k {directory}/k8s --ignore-not-found --wait=false", calls)
        self.assertFalse((directory / ".private/k8s").exists())
        archives = list((directory / ".private").glob("k8s-archive-*/vault-vn-init.json"))
        self.assertEqual(len(archives), 1)
        self.assertIn("bash k8s/scripts/up.sh", result.stdout)
