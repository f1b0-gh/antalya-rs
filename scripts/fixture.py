#!/usr/bin/env python3
"""Run a command against a disposable, digest-pinned Antalya bearer fixture.

Example: python3 scripts/fixture.py cargo test --test live_qualification -- --ignored
Pass --framed first to qualify the separate pinned framing-capable build.
Only this invocation's container is removed. Tokens stay in the child environment.
Uses a Linux Docker internal bridge, the server's default query-log interval,
and a loopback catalog stub inside the fixture (Perl is in both pinned images).
The child command has a 20-minute limit; timeout stops its process group before
checked cleanup of the owned container and network.
"""
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

VERSION = '26.6.8.20004.altinityantalya'
IMAGE = 'altinity/clickhouse-server@sha256:aec9534624af9142abe87aab1b51ff5bcca9e74ca3b7e58fead2c1ec5e157cdf'
CHILD_TIMEOUT_SECONDS = 20 * 60

# The pinned images already include Perl. Bind only the container's loopback:
# neither a public endpoint nor the host firewall is part of catalog qualification.
CATALOG_STUB = r'''
use strict;
use warnings;
use IO::Socket::INET;
my $server = IO::Socket::INET->new(LocalAddr => '127.0.0.1', LocalPort => 18123,
    Listen => 8, ReuseAddr => 1) or die "catalog stub listener: $!";
while (my $client = $server->accept()) {
    eval {
        local $SIG{ALRM} = sub { die "request timeout" };
        alarm 3;
        my $request = <$client> // '';
        my $size = length($request);
        my $authenticated = 0;
        while (my $header = <$client>) {
            $size += length($header);
            die "headers too large" if $size > 16384;
            if ($header =~ /^Authorization:\s*Bearer\s+([^\r\n]+)\r?\n$/i) {
                my $credential = $1;
                $credential =~ s/\s+$//;
                $authenticated = 1 if $credential eq $ENV{ANTALYA_CATALOG_CANARY};
            }
            last if $header eq "\r\n";
        }
        if ($request =~ m{ /(success|failure)/}) {
            open my $observations, '>>', '/catalog-stub-observations.tsv' or die $!;
            print $observations "$1\t$authenticated\n";
            close $observations;
        }
        my $body = $request =~ m{ /failure/} ? '{invalid catalog configuration'
            : $request =~ m{/config} ? '{"defaults":{},"overrides":{}}'
            : '{"namespaces":[],"identifiers":[]}';
        print $client "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
            . length($body) . "\r\nConnection: close\r\n\r\n" . $body;
        alarm 0;
    };
    if (my $error = $@) {
        open my $errors, '>>', '/tmp/catalog-stub-errors.log';
        print $errors $error;
        close $errors;
    }
    alarm 0;
    close $client;
}
'''


def token(secret, *, subject='fixture', audience='antalya-rs', expires=None):
    def enc(value):
        return base64.urlsafe_b64encode(value).rstrip(b'=')
    body = b'.'.join(enc(json.dumps(x, separators=(',', ':')).encode()) for x in (
        {'alg': 'HS256', 'typ': 'JWT'},
        {'sub': subject, 'iss': 'antalya-rs-fixture', 'aud': audience,
         'iat': int(time.time()) - 5, 'exp': expires if expires is not None else int(time.time()) + 3600}))
    return (body + b'.' + enc(hmac.new(secret.encode(), body, hashlib.sha256).digest())).decode()


def command(*args, **kwargs):
    kwargs.setdefault('timeout', 45)
    result = subprocess.run(args, capture_output=True, text=True, **kwargs)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def stop_child(child):
    # Cargo and its test processes share this new process group. Escalate even
    # if the group leader exits first, so its surviving children are stopped.
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    finally:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    child.wait(timeout=5)


def run_child(args, env):
    child = subprocess.Popen(args, env=env, start_new_session=True)
    try:
        return child.wait(timeout=CHILD_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print(f'Fixture child exceeded {CHILD_TIMEOUT_SECONDS} seconds; terminating its process group',
              file=sys.stderr, flush=True)
        stop_child(child)
        return 124
    except BaseException as original:
        try:
            stop_child(child)
        except Exception as failure:
            original.add_note(f'Could not stop fixture child process group: {failure}')
        raise


def cleanup_fixture(name, network, config, work, container_attempted, network_attempted):
    failures = []
    if container_attempted:
        try:
            logs = subprocess.run(['docker', 'logs', name], capture_output=True, text=True, timeout=30)
            (work / 'server.log').write_text(logs.stdout + logs.stderr)
        except Exception as error:
            failures.append(f'collecting container logs: {error}')
    removals = []
    if container_attempted:
        # Creation may have reached Docker even if the CLI timed out or failed.
        removals.append((['docker', 'rm', '-fv', name], (f'No such container: {name}',)))
    if network_attempted:
        removals.append((['docker', 'network', 'rm', network],
                         (f'network {network} not found', f'No such network: {network}')))
    for args, absent_messages in removals:
        try:
            result = subprocess.run(args, capture_output=True, text=True, timeout=30)
            if result.returncode and not any(message in result.stderr for message in absent_messages):
                failures.append(f'{" ".join(args)} exited {result.returncode}: {result.stderr.strip()}')
        except Exception as error:
            failures.append(f'{" ".join(args)}: {error}')
    try:
        config.unlink(missing_ok=True)
    except Exception as error:
        failures.append(f'removing fixture signing configuration: {error}')
    return failures


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
    network = name + '-isolated'
    secret = secrets.token_hex(32)
    catalog_canary = 'synthetic-catalog-canary-' + secrets.token_hex(16)
    catalog_observations = work / 'catalog-stub-observations.tsv'
    catalog_observations.write_text('')
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
</clickhouse>''')
    network_attempted = container_attempted = False
    child_status = None
    try:
        # The internal network has no route to external catalog/object-store hosts.
        network_attempted = True
        command('docker', 'network', 'create', '--internal', network)
        container_attempted = True
        command('docker', 'run', '-d', '--name', name, '--memory', '1g', '--cpus', '2',
                '--network', network,
                '-v', f'{config}:/etc/clickhouse-server/config.d/fixture.xml:ro',
                '-v', f'{catalog_observations}:/catalog-stub-observations.tsv:rw', image)
        container_ip = json.loads(command('docker', 'inspect', name))[0]['NetworkSettings']['Networks'][network]['IPAddress']
        endpoint = f'http://{container_ip}:8123/'
        for attempt in range(90):
            result = subprocess.run(['docker', 'exec', name, 'clickhouse-client', '-q', 'SELECT version()'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                if result.stdout.strip() != version:
                    raise RuntimeError('Unexpected server version: ' + result.stdout.strip())
                break
            time.sleep(1)
        else:
            raise RuntimeError('Antalya did not start')
        command('docker', 'exec', '-i', name, 'clickhouse-client', '--max_threads', '1', '--multiquery', input="CREATE USER fixture IDENTIFIED WITH jwt SETTINGS max_threads=1, max_memory_usage=200000000, log_queries=1, log_query_settings=1; GRANT SELECT, INSERT, CREATE DATABASE, CREATE TABLE, DROP DATABASE, DROP TABLE, ALTER TABLE, KILL QUERY, SYSTEM FLUSH LOGS ON *.* TO fixture;")
        for level in (1, 2):
            command('docker', 'exec', '-i', name, 'clickhouse-client', '--multiquery', input=f"CREATE USER qualification_readonly{level} IDENTIFIED WITH jwt SETTINGS readonly={level}, max_threads=1, max_memory_usage=200000000, log_queries=1; GRANT SELECT ON *.* TO qualification_readonly{level};")
        flush_interval = command('docker', 'exec', name, 'clickhouse', 'extract-from-config', '--config-file', '/var/lib/clickhouse/preprocessed_configs/config.xml', '--key', 'query_log.flush_interval_milliseconds')
        if flush_interval != '7500':
            raise RuntimeError('Unexpected default query-log flush interval: ' + flush_interval)
        # Avoid sticky /tmp: protected_regular can reject O_CREAT even for root
        # when the mounted file belongs to the host user. Fail before any DDL.
        command('docker', 'exec', name, 'perl', '-e',
                'open my $f, ">>", "/catalog-stub-observations.tsv" or die $!;')
        command('docker', 'exec', '-d', '-e', 'ANTALYA_CATALOG_CANARY=' + catalog_canary,
                name, 'perl', '-e', CATALOG_STUB)
        # Wait for the local listener with a bounded probe; never silently run the
        # masking test against an unavailable endpoint and its server retry loop.
        command('docker', 'exec', name, 'perl', '-MIO::Socket::INET', '-e',
                'for (1..50) { my $s = IO::Socket::INET->new(PeerAddr=>"127.0.0.1",PeerPort=>18123,Timeout=>1); exit 0 if $s; select undef,undef,undef,0.1; } exit 1;', timeout=10)
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
        env.update(ANTALYA_TEST_READONLY1_TOKEN=token(secret, subject='qualification_readonly1'),
                   ANTALYA_TEST_READONLY2_TOKEN=token(secret, subject='qualification_readonly2'),
                   ANTALYA_TEST_LOG_FLUSH_INTERVAL=flush_interval,
                   ANTALYA_TEST_CATALOG_URL='http://127.0.0.1:18123/',
                   ANTALYA_TEST_CATALOG_TOKEN=catalog_canary,
                   ANTALYA_TEST_CATALOG_OBSERVATIONS=str(catalog_observations))
        bypass = ','.join(filter(None, (os.environ.get('NO_PROXY'), os.environ.get('no_proxy'),
                                       container_ip, '127.0.0.1', 'localhost')))
        env.update(NO_PROXY=bypass, no_proxy=bypass)
        args = args or ['cargo', 'test', '--test', 'live_qualification', '--', '--ignored']
        child_status = run_child(args, env)
    finally:
        original = sys.exc_info()[1]
        failures = cleanup_fixture(name, network, config, work, container_attempted, network_attempted)
        if failures:
            detail = 'Fixture cleanup failed:\n' + '\n'.join(failures)
            if original is not None:
                # Preserve the setup/command exception as the primary failure.
                original.add_note(detail)
            elif child_status:
                # Preserve the child's exit status while making cleanup failure visible.
                print(f'Fixture child exited {child_status}. {detail}', file=sys.stderr, flush=True)
            else:
                raise RuntimeError(detail)
    return child_status


if __name__ == '__main__':
    sys.exit(main())
