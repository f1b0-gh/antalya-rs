#!/usr/bin/env python3
"""Run a command against a disposable, digest-pinned Antalya bearer fixture.

Example: python3 scripts/fixture.py cargo test --test live_qualification -- --ignored
Pass --framed first to qualify the separate pinned framing-capable build.
Only this invocation's container is removed. Tokens stay in the child environment.
"""
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.request

VERSION = '26.6.8.20004.altinityantalya'
IMAGE = 'altinity/clickhouse-server@sha256:aec9534624af9142abe87aab1b51ff5bcca9e74ca3b7e58fead2c1ec5e157cdf'


def token(secret, *, subject='fixture', audience='antalya-rs', expires=None):
    def enc(value):
        return base64.urlsafe_b64encode(value).rstrip(b'=')
    body = b'.'.join(enc(json.dumps(x, separators=(',', ':')).encode()) for x in (
        {'alg': 'HS256', 'typ': 'JWT'},
        {'sub': subject, 'iss': 'antalya-rs-fixture', 'aud': audience,
         'iat': int(time.time()) - 5, 'exp': expires if expires is not None else int(time.time()) + 3600}))
    return (body + b'.' + enc(hmac.new(secret.encode(), body, hashlib.sha256).digest())).decode()


def command(*args, **kwargs):
    result = subprocess.run(args, capture_output=True, text=True, **kwargs)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def main():
    args = sys.argv[1:]
    version, image = VERSION, IMAGE
    if args[:1] == ['--framed']:
        args = args[1:]
        version = '26.8.34.20001.altinityantalya'
        image = 'altinityinfra/clickhouse-server@sha256:0989fbcd21552463cff9ef8d7a44bd2213a2c8e1879816f856a744a14b28e77a'
    parent = Path('/tmp/antalya-rs')
    parent.mkdir(exist_ok=True)
    if parent.is_symlink() or parent.stat().st_uid != os.getuid():
        raise RuntimeError('Unsafe fixture directory')
    work = Path(tempfile.mkdtemp(prefix='qualification-', dir=parent))
    name = 'antalya-rs-' + secrets.token_hex(6)
    secret = secrets.token_hex(32)
    config = work / 'fixture.xml'
    config.write_text(f'''<clickhouse>
 <enable_token_auth>1</enable_token_auth>
 <access_control_improvements><settings_constraints_replace_previous>true</settings_constraints_replace_previous></access_control_improvements>
 <token_processors><fixture><type>jwt_static_key</type><algo>HS256</algo>
 <static_key>{secret}</static_key><username_claim>sub</username_claim>
 <expected_issuer>antalya-rs-fixture</expected_issuer><expected_audience>antalya-rs</expected_audience>
 <allow_no_expiration>false</allow_no_expiration><verifier_leeway>0</verifier_leeway><token_cache_lifetime>0</token_cache_lifetime>
 </fixture></token_processors>
 <max_server_memory_usage>800000000</max_server_memory_usage>
 <background_schedule_pool_size>8</background_schedule_pool_size>
 <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
 <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
 <mark_cache_size>16777216</mark_cache_size><uncompressed_cache_size>16777216</uncompressed_cache_size>
 <metric_log><schema_type>transposed</schema_type><table>metric_log_compact</table></metric_log>
 <query_log><flush_interval_milliseconds>100</flush_interval_milliseconds></query_log>
</clickhouse>''')
    try:
        command('docker', 'run', '-d', '--name', name, '--memory', '1g', '--cpus', '2',
                '-p', '127.0.0.1::8123', '-v', f'{config}:/etc/clickhouse-server/config.d/fixture.xml:ro', image)
        endpoint = 'http://' + command('docker', 'port', name, '8123/tcp') + '/'
        for attempt in range(90):
            result = subprocess.run(['docker', 'exec', name, 'clickhouse-client', '-q', 'SELECT version()'], capture_output=True, text=True)
            if result.returncode == 0:
                if result.stdout.strip() != version:
                    raise RuntimeError('Unexpected server version: ' + result.stdout.strip())
                break
            time.sleep(1)
        else:
            raise RuntimeError('Antalya did not start')
        command('docker', 'exec', '-i', name, 'clickhouse-client', '--max_threads', '1', '--multiquery', input="CREATE USER fixture IDENTIFIED WITH jwt SETTINGS max_threads=1, max_memory_usage=200000000, log_queries=1, log_query_settings=1; GRANT SELECT, INSERT, CREATE DATABASE, CREATE TABLE, DROP DATABASE, DROP TABLE, ALTER TABLE, KILL QUERY, SYSTEM FLUSH LOGS ON *.* TO fixture;")
        good = token(secret)
        def query(bearer):
            req = urllib.request.Request(endpoint, data=b'SELECT currentUser()', headers={'Authorization': 'Bearer ' + bearer})
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(req, timeout=10) as response:
                return response.read().decode().strip()
        if query(good) != 'fixture':
            raise RuntimeError('Bearer mapped to wrong principal')
        for invalid in (token(secret, audience='wrong'), token(secret, expires=int(time.time()) - 3600), token('wrong-key')):
            try:
                query(invalid)
            except urllib.error.HTTPError as exc:
                if exc.code not in (401, 403):
                    raise
            else:
                raise RuntimeError('Invalid bearer accepted')
        print(f'Qualified bearer validation on {version} ({image}); artifacts: {work}', flush=True)
        # Synthetic fixture-only key: tests may mint short-lived tokens after
        # compilation. Never use this helper against a non-fixture endpoint.
        env = dict(os.environ, ANTALYA_TEST_URL=endpoint, ANTALYA_TEST_TOKEN=good, ANTALYA_TEST_CONTAINER=name, ANTALYA_TEST_DMZ_TOKEN=token(secret, subject='team_dmz'), ANTALYA_TEST_SIGNING_KEY=secret, ANTALYA_TEST_VERSION=version)
        args = args or ['cargo', 'test', '--test', 'live_qualification', '--', '--ignored']
        return subprocess.run(args, env=env).returncode
    finally:
        logs = subprocess.run(['docker', 'logs', name], capture_output=True, text=True)
        (work / 'server.log').write_text(logs.stdout + logs.stderr)
        # These anonymous volumes belong only to this disposable container.
        subprocess.run(['docker', 'rm', '-fv', name], capture_output=True)
        config.unlink(missing_ok=True)


if __name__ == '__main__':
    sys.exit(main())
