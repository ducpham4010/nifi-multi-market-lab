#!/usr/bin/env python3
"""Include the isolated browser hostname in the lab's self-signed certificate.

Keep existing store passwords, other trust anchors and all flow/authentication
settings. Reuse certificates that already match; keytool passwords use env vars.

Optional NIFI_TLS_EXTRA_SANS adds comma-separated ``dns:<name>``/``ip:<addr>``
entries, e.g. Kubernetes Service DNS names used by in-cluster API clients.
Jetty checks SNI against the certificate, so every hostname used to reach
NiFi over TLS must be listed.
"""

import ipaddress
import os
import pathlib
import re
import secrets
import shutil
import subprocess
import tempfile


def main():
    host = os.environ["NIFI_BROWSER_HOST"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", host):
        raise RuntimeError("NIFI_BROWSER_HOST must be a DNS hostname")
    ip = str(
        ipaddress.ip_address(
            os.environ["NIFI_WEB_PROXY_HOST"].split(",")[0].rsplit(":", 1)[0]
        )
    )
    extra_dns, extra_ip = [], []
    for entry in filter(
        None, (e.strip() for e in os.environ.get("NIFI_TLS_EXTRA_SANS", "").split(","))
    ):
        kind, _, value = entry.partition(":")
        if kind.lower() == "dns" and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", value):
            extra_dns.append(value)
        elif kind.lower() == "ip":
            extra_ip.append(str(ipaddress.ip_address(value)))
        else:
            raise RuntimeError(f"Invalid NIFI_TLS_EXTRA_SANS entry: {entry}")
    dns_names = list(dict.fromkeys([host, "localhost", *extra_dns]))
    ip_names = list(dict.fromkeys([ip, *extra_ip]))
    home = pathlib.Path(os.environ.get("NIFI_HOME", "/opt/nifi/nifi-current"))
    properties = home / "conf/nifi.properties"
    lines = properties.read_text().splitlines()
    values = dict(
        line.split("=", 1)
        for line in lines
        if line and not line.startswith("#") and "=" in line
    )

    def path(key, default):
        p = pathlib.Path(values.get(key) or default)
        return p if p.is_absolute() else home / p

    key_store = path("nifi.security.keystore", "./conf/keystore.p12")
    trust_store = path("nifi.security.truststore", "./conf/truststore.p12")
    for key in ("nifi.security.keystoreType", "nifi.security.truststoreType"):
        if values.get(key) not in (None, "", "PKCS12"):
            raise RuntimeError("Lab browser TLS setup requires PKCS12 stores")
    store_password = values.get(
        "nifi.security.keystorePasswd"
    ) or secrets.token_urlsafe(24)
    trust_password = values.get(
        "nifi.security.truststorePasswd"
    ) or secrets.token_urlsafe(24)
    key_password = values.get("nifi.security.keyPasswd") or store_password
    if any(
        p.startswith("enc{") for p in (store_password, trust_password, key_password)
    ):
        raise RuntimeError(
            "Configure certificates manually when store passwords are encrypted"
        )
    if key_password != store_password:
        raise RuntimeError(
            "PKCS12 key and store passwords must match for lab TLS setup"
        )
    env = {
        **os.environ,
        "LAB_TLS_STORE_PASS": store_password,
        "LAB_TLS_TRUST_PASS": trust_password,
    }

    def run(*args, check=True):
        return subprocess.run(
            args, env=env, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

    with tempfile.TemporaryDirectory(
        prefix=".lab-tls-", dir=home / "conf"
    ) as directory:
        temp = pathlib.Path(directory)
        cert = temp / "browser.pem"
        if key_store.exists():
            cert.write_bytes(
                run(
                    "openssl",
                    "pkcs12",
                    "-in",
                    str(key_store),
                    "-clcerts",
                    "-nokeys",
                    "-passin",
                    "env:LAB_TLS_STORE_PASS",
                ).stdout
            )
            san = run(
                "openssl", "x509", "-in", str(cert), "-noout", "-ext", "subjectAltName"
            ).stdout.decode()
            names = {
                name.strip()
                for line in san.splitlines()[1:]
                for name in line.split(",")
            }
            wanted = {f"DNS:{n}" for n in dns_names} | {
                f"IP Address:{n}" for n in ip_names
            }
            if wanted <= names:
                print(f"Lab TLS certificate already covers {host} and {ip}")
                return

        backup = home / "conf/.lab-browser-tls-original"
        if not backup.exists():
            backup.mkdir(mode=0o700)
            for source in (key_store, trust_store, properties):
                if source.exists():
                    dest = backup / source.name
                    shutil.copy2(source, dest)
                    dest.chmod(0o600)

        new_key = temp / "keystore.p12"
        new_trust = temp / "truststore.p12"
        alias = "nifi-lab-browser"
        run(
            "keytool",
            "-genkeypair",
            "-alias",
            alias,
            "-keyalg",
            "RSA",
            "-keysize",
            "3072",
            "-validity",
            "365",
            "-dname",
            f"CN={host}",
            "-ext",
            "SAN="
            + ",".join([f"dns:{n}" for n in dns_names] + [f"ip:{n}" for n in ip_names]),
            "-storetype",
            "PKCS12",
            "-keystore",
            str(new_key),
            "-storepass:env",
            "LAB_TLS_STORE_PASS",
        )
        cert.write_bytes(
            run(
                "keytool",
                "-exportcert",
                "-rfc",
                "-alias",
                alias,
                "-keystore",
                str(new_key),
                "-storepass:env",
                "LAB_TLS_STORE_PASS",
            ).stdout
        )
        if trust_store.exists():
            shutil.copy2(trust_store, new_trust)
            # Replace only our own prior browser certificate; preserve other anchors.
            run(
                "keytool",
                "-delete",
                "-alias",
                alias,
                "-keystore",
                str(new_trust),
                "-storepass:env",
                "LAB_TLS_TRUST_PASS",
                check=False,
            )
        run(
            "keytool",
            "-importcert",
            "-noprompt",
            "-alias",
            alias,
            "-file",
            str(cert),
            "-storetype",
            "PKCS12",
            "-keystore",
            str(new_trust),
            "-storepass:env",
            "LAB_TLS_TRUST_PASS",
        )
        new_key.chmod(0o600)
        new_trust.chmod(0o600)
        new_key.replace(key_store)
        new_trust.replace(trust_store)

    updates = {
        "nifi.security.keystore": str(key_store),
        "nifi.security.keystoreType": "PKCS12",
        "nifi.security.keystorePasswd": store_password,
        "nifi.security.keyPasswd": store_password,
        "nifi.security.truststore": str(trust_store),
        "nifi.security.truststoreType": "PKCS12",
        "nifi.security.truststorePasswd": trust_password,
    }
    output = []
    for line in lines:
        key = line.split("=", 1)[0]
        output.append(key + "=" + updates.pop(key) if key in updates else line)
    output.extend(key + "=" + value for key, value in updates.items())
    properties.write_text("\n".join(output) + "\n")
    print(f"Configured lab TLS certificate for {host} and {ip}")


if __name__ == "__main__":
    main()
