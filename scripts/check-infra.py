#!/usr/bin/env python3
"""Read-only readiness checks after init-topics, init-vault and sync-params.

Run on the Linux Docker host. This checks infrastructure, not a NiFi data flow.
"""
import json
import os
import pathlib
import socket
import ssl
import subprocess
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
ENV = dict(
    line.split('=', 1) for line in (ROOT / '.env').read_text().splitlines()
    if line and not line.startswith('#')
)
IP = ENV['HOST_IP']
TLS = ssl._create_unverified_context()  # Self-signed certificates in this lab.


def http(url, data=None, headers=None):
    request = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(request, context=TLS, timeout=30) as response:
        return response.read()


def docker(*args):
    return subprocess.check_output(
        ['docker', 'compose', *args], cwd=ROOT, text=True,
        env={**os.environ, 'PGPASSWORD': ENV['DB_PASSWORD']},
    ).strip()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    services = docker('config', '--services').splitlines()
    require(len(services) == 10, 'Expected 10 services')
    for service in services:
        ident = docker('ps', '-a', '-q', service)
        require(bool(ident), f'{service}: missing container')
        state = json.loads(subprocess.check_output(
            ['docker', 'inspect', '--format', '{{json .State}}', ident], text=True,
        ))
        require(state['Running'] and not state['Restarting'] and not state['OOMKilled'],
                f'{service}: not running cleanly')
    print('PASS all 10 containers running, no OOM state')

    for market, port, expected in [
        ('vn', 19092, {'trips', 'trips-staging'}), ('indo', 29092, {'trips'}),
    ]:
        for topic in sorted(expected):
            description = docker(
                'exec', '-T', f'kafka-{market}', '/opt/kafka/bin/kafka-topics.sh',
                '--bootstrap-server', f'{IP}:{port}', '--describe', '--topic', topic,
            )
            require('PartitionCount: 3' in description and 'ReplicationFactor: 1' in description,
                    f'{market}/{topic}: unexpected partition configuration')
            require('Leader: -1' not in description, f'{market}/{topic}: missing leader')
        print(f'PASS Kafka {market}: topics and partition leaders')

    for market, port in [('vn', 15432), ('indo', 25432)]:
        for schema in ('market', 'staging'):
            count = docker(
                'exec', '-T', '-e', 'PGPASSWORD', f'postgres-{market}',
                'psql', '-h', IP, '-p', str(port),
                '-U', 'admin', '-d', 'market', '-Atc',
                f'SELECT count(*) FROM {schema}.trip_events',
            )
            require(count.isdigit(), f'{market}/{schema}: invalid table query')
            print(f'PASS PostgreSQL {market}/{schema}: {count} rows')

    for market, port in [('vn', 18200), ('indo', 28200)]:
        health = json.loads(http(f'http://{IP}:{port}/v1/sys/health'))
        require(health['initialized'] and not health['sealed'], f'Vault {market}: sealed')
        keyfile = ROOT / '.private' / f'vault-{market}-init.json'
        require(keyfile.exists() and keyfile.stat().st_mode & 0o077 == 0,
                f'Vault {market}: missing or permissive key file')
        print(f'PASS Vault {market}: initialized, unsealed, keys restricted')

    settings = [('stag', 18443, 'vn', 18200, 'staging'),
                ('vn', 28443, 'vn', 18200, 'runtime'),
                ('indo', 38443, 'indo', 28200, 'runtime')]
    for market, port, vm, vp, secret in settings:
        base = f'https://{IP}:{port}'
        http(base + '/nifi/')
        token = http(base + '/nifi-api/access/token', urllib.parse.urlencode({
            'username': 'admin', 'password': ENV['NIFI_PASSWORD'],
        }).encode(), {'Content-Type': 'application/x-www-form-urlencoded'}).decode()
        headers = {'Authorization': 'Bearer ' + token}
        contexts = json.loads(http(base + '/nifi-api/flow/parameter-contexts', headers=headers))
        matches = [c for c in contexts['parameterContexts']
                   if c['component']['name'] == 'shared-runtime']
        require(len(matches) == 1, f'NiFi {market}: missing/ambiguous context')
        ident = matches[0]['id']
        context = json.loads(http(base + '/nifi-api/parameter-contexts/' + ident, headers=headers))
        params = {p['parameter']['name']: p['parameter']
                  for p in context['component']['parameters']}
        values = json.loads(http(f'http://{IP}:{vp}/v1/kv/{secret}', headers={
            'X-Vault-Token': f'nifi-{vm}-lab-token',
        }))['data']
        require(len(params) == 8 and set(params) == set(values), f'NiFi {market}: parameter keys')
        for name, value in values.items():
            require(bool(params[name]['sensitive']) == (name == 'db.password'),
                    f'NiFi {market}: sensitivity mismatch for {name}')
            if name != 'db.password':
                require(params[name]['value'] == value, f'NiFi {market}: mismatch for {name}')
        docker('exec', '-T', f'nifi-{market}', 'test', '-s', '/opt/nifi/drivers/postgresql.jar')
        print(f'PASS NiFi {market}: UI, admin login, 8 Vault parameters, JDBC JAR')

    # GitLab limits monitoring endpoints to loopback by default.
    readiness = json.loads(http('http://127.0.0.1:18080/-/readiness?all=1'))
    require(readiness.get('status') == 'ok', 'GitLab readiness failed')
    http(f'http://{IP}:18080/users/sign_in')
    print('PASS GitLab: readiness including dependencies, login page')
    with socket.create_connection((IP, 18022), timeout=10) as connection:
        require(connection.recv(256).startswith(b'SSH-2.0-'), 'GitLab SSH banner missing')
    print('PASS GitLab SSH: protocol banner on port 18022')
    print('Infrastructure ready. Kafka -> NiFi -> PostgreSQL flow remains a lab exercise.')


if __name__ == '__main__':
    main()
