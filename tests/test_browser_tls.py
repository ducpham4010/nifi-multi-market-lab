"""Exercise real PKCS12 generation, repeat startup and hostname migration.

Requires Java keytool and OpenSSL; also runnable inside the NiFi image with
NIFI_TLS_CONFIG_SCRIPT=/opt/nifi/scripts/configure-browser-tls.py.
"""

import hashlib
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(
    os.environ.get(
        "NIFI_TLS_CONFIG_SCRIPT",
        str(
            pathlib.Path(__file__).resolve().parents[1]
            / "nifi/configure-browser-tls.py"
        ),
    )
)


@unittest.skipUnless(
    shutil.which("keytool") and shutil.which("openssl"), "requires keytool and openssl"
)
class BrowserTlsIntegrationTest(unittest.TestCase):
    def test_first_boot_repeat_and_hostname_change(self):
        with tempfile.TemporaryDirectory() as directory:
            home = pathlib.Path(directory)
            conf = home / "conf"
            conf.mkdir()
            properties = conf / "nifi.properties"
            properties.write_text(
                "unrelated.setting=keep-me\nnifi.web.https.host=0.0.0.0\n"
            )
            env = {
                **os.environ,
                "NIFI_HOME": str(home),
                "NIFI_BROWSER_HOST": "first.nifi.invalid",
                "NIFI_WEB_PROXY_HOST": "192.168.102.4:18443,first.nifi.invalid:18443",
            }

            def configure():
                subprocess.run(
                    [sys.executable, str(SCRIPT)],
                    env=env,
                    check=True,
                    capture_output=True,
                )

            def values():
                return dict(
                    line.split("=", 1)
                    for line in properties.read_text().splitlines()
                    if "=" in line
                )

            def fingerprint():
                return tuple(
                    hashlib.sha256((conf / name).read_bytes()).digest()
                    for name in ("keystore.p12", "truststore.p12", "nifi.properties")
                )

            configure()
            before = fingerprint()
            self.assertEqual(values()["unrelated.setting"], "keep-me")
            self.assertEqual(values()["nifi.web.https.host"], "0.0.0.0")
            configure()
            self.assertEqual(
                fingerprint(), before, "repeat startup must reuse stores and properties"
            )

            # Add an unrelated trusted certificate; hostname migration must retain it.
            p = values()
            passwords = {
                **env,
                "TEST_KEY_PASS": p["nifi.security.keystorePasswd"],
                "TEST_TRUST_PASS": p["nifi.security.truststorePasswd"],
            }
            cert = home / "external.pem"
            exported = subprocess.run(
                [
                    "keytool",
                    "-exportcert",
                    "-rfc",
                    "-alias",
                    "nifi-lab-browser",
                    "-keystore",
                    p["nifi.security.keystore"],
                    "-storepass:env",
                    "TEST_KEY_PASS",
                ],
                env=passwords,
                capture_output=True,
                check=True,
            )
            cert.write_bytes(exported.stdout)
            subprocess.run(
                [
                    "keytool",
                    "-importcert",
                    "-noprompt",
                    "-alias",
                    "external-anchor",
                    "-file",
                    str(cert),
                    "-keystore",
                    p["nifi.security.truststore"],
                    "-storepass:env",
                    "TEST_TRUST_PASS",
                ],
                env=passwords,
                capture_output=True,
                check=True,
            )

            env["NIFI_BROWSER_HOST"] = "second.nifi.invalid"
            configure()
            p2 = values()
            self.assertEqual(
                p2["nifi.security.keystorePasswd"], p["nifi.security.keystorePasswd"]
            )
            self.assertEqual(
                p2["nifi.security.truststorePasswd"],
                p["nifi.security.truststorePasswd"],
            )
            result = subprocess.run(
                [
                    "keytool",
                    "-list",
                    "-alias",
                    "external-anchor",
                    "-keystore",
                    p2["nifi.security.truststore"],
                    "-storepass:env",
                    "TEST_TRUST_PASS",
                ],
                env=passwords,
                capture_output=True,
            )
            self.assertEqual(
                result.returncode,
                0,
                "unrelated trust anchor must survive hostname changes",
            )
            after = fingerprint()
            configure()
            self.assertEqual(fingerprint(), after)

    def test_kubernetes_service_names_in_san(self):
        """NIFI_TLS_EXTRA_SANS adds in-cluster Service DNS names (Jetty checks SNI)."""
        with tempfile.TemporaryDirectory() as directory:
            home = pathlib.Path(directory)
            (home / "conf").mkdir()
            (home / "conf/nifi.properties").write_text("nifi.web.https.host=0.0.0.0\n")
            env = {
                **os.environ,
                "NIFI_HOME": str(home),
                "NIFI_BROWSER_HOST": "nifi-vn.192.168.102.4.sslip.io",
                "NIFI_WEB_PROXY_HOST": "192.168.102.4:31443,nifi-vn.192.168.102.4.sslip.io:31443",
                "NIFI_TLS_EXTRA_SANS": "dns:nifi.nifi-vn.svc,dns:nifi.nifi-vn.svc.cluster.local",
            }

            def configure():
                return subprocess.run(
                    [sys.executable, str(SCRIPT)],
                    env=env,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout

            configure()
            values = dict(
                line.split("=", 1)
                for line in (home / "conf/nifi.properties").read_text().splitlines()
                if "=" in line
            )
            pem = subprocess.run(
                [
                    "openssl",
                    "pkcs12",
                    "-in",
                    str(home / "conf/keystore.p12"),
                    "-clcerts",
                    "-nokeys",
                    "-passin",
                    "env:TEST_PASS",
                ],
                env={**env, "TEST_PASS": values["nifi.security.keystorePasswd"]},
                capture_output=True,
                check=True,
            ).stdout
            san = subprocess.run(
                ["openssl", "x509", "-noout", "-ext", "subjectAltName"],
                input=pem,
                capture_output=True,
                check=True,
            ).stdout.decode()
            for name in (
                "DNS:nifi-vn.192.168.102.4.sslip.io",
                "DNS:localhost",
                "DNS:nifi.nifi-vn.svc",
                "DNS:nifi.nifi-vn.svc.cluster.local",
                "IP Address:192.168.102.4",
            ):
                self.assertIn(name, san)
            self.assertIn(
                "already covers", configure(), "repeat start must reuse the certificate"
            )
            env["NIFI_TLS_EXTRA_SANS"] = "bad-entry"
            with self.assertRaises(subprocess.CalledProcessError):
                configure()


if __name__ == "__main__":
    unittest.main()
