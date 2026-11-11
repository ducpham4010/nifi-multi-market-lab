import tempfile, pathlib, runpy, sys, json, urllib.request, urllib.error, unittest.mock as mock, io, contextlib, copy

root = pathlib.Path(__file__).resolve().parents[1]


class Response:
    def __init__(self, data, typ="application/json"):
        self.data = json.dumps(data).encode() if typ.endswith("json") else data.encode()
        self.headers = {"Content-Type": typ}

    def read(self):
        return self.data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


with tempfile.TemporaryDirectory() as temp:
    p = pathlib.Path(temp)
    (p / "scripts").mkdir()
    (p / ".env").write_text(
        "HOST_IP=192.168.1.50\nNIFI_PASSWORD=adminadmin123\nDB_PASSWORD=admin\n"
    )
    for name in ["init-vault.py", "sync-params.py"]:
        (p / "scripts" / name).write_text((root / "scripts" / name).read_text())
    vaults = {}
    updates = []
    contexts = []

    def fake(req, **kw):
        url = req.full_url
        port = urllib.parse.urlparse(url).port
        path = urllib.parse.urlparse(url).path
        method = req.method
        data = (
            json.loads(req.data)
            if req.data and req.headers.get("Content-type") == "application/json"
            else None
        )
        if port in (18200, 28200):
            state = vaults.setdefault(
                port,
                {
                    "initialized": False,
                    "sealed": True,
                    "mounts": {},
                    "secrets": {},
                    "tokens": {},
                },
            )
            route = path.removeprefix("/v1/")
            if route == "sys/init":
                if method == "GET":
                    return Response({"initialized": state["initialized"]})
                state["initialized"] = True
                return Response({"keys": ["unseal"], "root_token": "root-token"})
            if route == "sys/seal-status":
                return Response({"sealed": state["sealed"]})
            if route == "sys/unseal":
                state["sealed"] = False
                return Response({})
            if route == "sys/mounts":
                return Response(state["mounts"])
            if route == "sys/mounts/kv":
                state["mounts"]["kv/"] = data
                return Response({})
            if route.startswith("kv/"):
                if method == "POST":
                    state["secrets"][route] = data
                    return Response({})
                if route not in state["secrets"]:
                    raise urllib.error.HTTPError(url, 404, "missing", {}, None)
                return Response({"data": state["secrets"][route]})
            if route.startswith("sys/policies/acl/"):
                assert "auth/token/lookup-self" in data["policy"]
                return Response({})
            if route == "auth/token/lookup-self":
                if req.headers["X-vault-token"] not in state["tokens"]:
                    raise urllib.error.HTTPError(url, 403, "missing token", {}, None)
                return Response({})
            if route == "auth/token/create":
                assert data["id"] not in state["tokens"]
                state["tokens"][data["id"]] = data
                return Response({})
        else:
            route = path.removeprefix("/nifi-api")
            if route == "/access/token":
                return Response("jwt", "text/plain")
            if route == "/flow/parameter-contexts":
                return Response({"parameterContexts": contexts})
            if route == "/parameter-contexts" and method == "POST":
                data["component"]["id"] = "ctx"
                contexts.append(data)
                return Response(data)
            if route == "/parameter-contexts/ctx":
                return Response(contexts[0])
            if route.endswith("/update-requests"):
                updates.append(data)
                return Response({"request": {"requestId": "request"}})
            if route.endswith("/update-requests/request"):
                return Response(
                    {"request": {"complete": True}} if method == "GET" else {}
                )
        raise AssertionError((url, method, data))

    with mock.patch("urllib.request.urlopen", fake), contextlib.redirect_stdout(
        io.StringIO()
    ):
        runpy.run_path(str(p / "scripts/init-vault.py"), run_name="__main__")
        before = copy.deepcopy(vaults)
        runpy.run_path(str(p / "scripts/init-vault.py"), run_name="__main__")
        assert before == vaults
        for attempt in range(2):
            with mock.patch.object(sys, "argv", ["sync-params.py", "vn"]):
                try:
                    runpy.run_path(
                        str(p / "scripts/sync-params.py"), run_name="__main__"
                    )
                except SystemExit as e:
                    assert e.code == 0
        assert len(contexts) == 1 and len(updates) == 1
        params = contexts[0]["component"]["parameters"]
        assert len(params) == 8
        assert [
            x["parameter"]["name"] for x in params if x["parameter"]["sensitive"]
        ] == ["db.password"]
        assert updates[0]["revision"] == {"version": 0}
        # NiFi 2.6 rejects update requests without the entity-level id.
        assert updates[0]["id"] == updates[0]["component"]["id"]
    print(
        "PASS mocked HTTP: Vault initial seed + repeat idempotence; NiFi context create + async update; password sensitivity"
    )
