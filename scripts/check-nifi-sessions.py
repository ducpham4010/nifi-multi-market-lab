#!/usr/bin/env python3
"""Check three NiFi logins in one cookie jar, like three tabs in one browser.

No flow/context mutations. Authentication cookies and JWTs stay in memory.
Run from a client or the lab host; self-signed TLS is accepted for this lab.
Kubernetes lab: NIFI_GATEWAY_PORT=31443 python3 scripts/check-nifi-sessions.py
(same hostnames, one Gateway port for all three NiFi).
"""

import base64
import os
import http.cookiejar
import json
import pathlib
import ssl
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
ENV = dict(
    line.split("=", 1)
    for line in (ROOT / ".env").read_text().splitlines()
    if line and not line.startswith("#")
)
IP = ENV["HOST_IP"]


def main():
    targets = [
        (
            market,
            ENV.get(key) or f"nifi-{market}.{IP}.sslip.io",
            int(os.environ.get("NIFI_GATEWAY_PORT", port)),
        )
        for market, key, port in [
            ("stag", "NIFI_STAG_HOST", 18443),
            ("vn", "NIFI_VN_HOST", 28443),
            ("indo", "NIFI_INDO_HOST", 38443),
        ]
    ]
    if len({host.lower() for _, host, _ in targets}) != 3:
        raise RuntimeError("Each NiFi needs a distinct browser hostname")
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPCookieProcessor(jar),
        urllib.request.HTTPSHandler(context=ssl._create_unverified_context()),
    )

    def login(market, host, port):
        request = urllib.request.Request(
            f"https://{host}:{port}/nifi-api/access/token",
            data=urllib.parse.urlencode(
                {"username": "admin", "password": ENV["NIFI_PASSWORD"]}
            ).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with opener.open(request, timeout=30) as response:
            encoded = response.read().decode().split(".")[1]
            claims = json.loads(
                base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
            )
        lifetime = claims["exp"] - claims["iat"]
        if lifetime != 8 * 60 * 60:
            raise RuntimeError(
                f"{market}: unexpected Single User JWT lifetime {lifetime}s"
            )
        print(f"PASS {market}: login, JWT lifetime {lifetime // 3600} hours")

    def check_all():
        for market, host, port in targets:
            # Deliberately no Authorization header: the shared cookie jar must work.
            with opener.open(
                f"https://{host}:{port}/nifi-api/flow/current-user", timeout=30
            ) as response:
                current = json.load(response)
            if current.get("identity") != "admin":
                raise RuntimeError(f"{market}: expected authenticated admin")
        print("PASS all three cookie sessions remain authenticated")

    for target in targets:
        login(*target)
    check_all()
    login(*targets[0])
    check_all()
    domains = {
        cookie.domain
        for cookie in jar
        if cookie.name == "__Secure-Authorization-Bearer"
    }
    if len(domains) != 3:
        raise RuntimeError("Expected three independently scoped authorization cookies")
    print("PASS independent cookie domains, including after staging re-login")


if __name__ == "__main__":
    main()
