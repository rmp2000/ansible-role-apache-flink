"""Flink health, local processing and persisted-state recovery verification."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
import uuid

ROOT = Path('/srv/apache-flink')
STAGED = ROOT/'.verification.json'
JOB = 'bd003e1f1db333f8652cb8a391d5ab3a'


def wait_for(test, label, timeout=240, guard=None):
    until = time.monotonic()+timeout
    while time.monotonic() < until:
        if guard:
            guard()
        try:
            value = test()
            if value:
                return value
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError('Timed out: '+label)


def api(path):
    with urllib.request.urlopen('http://127.0.0.1:8081'+path, timeout=10) as response:
        return json.load(response)


def healthy():
    job = api('/jobs/'+JOB)
    return (job['state'] == 'RUNNING' and job['vertices']
            and all(vertex['status'] == 'RUNNING' for vertex in job['vertices']))


def check_services():
    for name in ('apache-flink-jobmanager', 'apache-flink-taskmanager'):
        value = subprocess.check_output(['systemctl', 'show', name, '-p', 'NRestarts', '--value'], text=True)
        if int(value.strip() or '0') > 3:
            raise RuntimeError('Service repeatedly failed to start: '+name)


def checkpoint():
    return api('/jobs/'+JOB+'/checkpoints').get('latest', {}).get('completed', {})


def completed_after(identifier):
    value = checkpoint()
    return value if value.get('id', 0) > identifier else None


def write_input(sensor, event_id, value):
    records = [{'sensor': sensor, 'event_id': event_id, 'value': value}]
    write_lines(records)


def write_lines(records):
    # The monitor ignores dot files until the complete fixture is renamed.
    name = uuid.uuid4().hex
    temporary = ROOT/'input'/('.'+name)
    temporary.write_text(''.join(json.dumps(item)+'\n' for item in records))
    temporary.chmod(0o644)
    temporary.rename(ROOT/'input'/(name+'.jsonl'))


def results(sensor):
    found = []
    for path in (ROOT/'output').rglob('*'):
        if path.is_file() and not path.name.startswith('.'):
            for line in path.read_text().splitlines():
                record = json.loads(line)
                if record.get('sensor') == sensor:
                    found.append(record)
    return found


def expect(sensor, count, total):
    return any(row.get('count') == count and row.get('sum') == total
               and row.get('mean') == total/count for row in results(sensor))


def cleanup():
    # Committed FileSink output may share files with other data. Never delete it.
    # Remove only the fixture marker; reference data remains auditable on test disks.
    STAGED.unlink(missing_ok=True)


def main():
    mode = sys.argv[1]
    settings = json.loads(Path('/etc/apache-flink/job.json').read_text())
    wait_for(healthy, 'running Flink source, aggregation and sink', guard=check_services)
    wait_for(checkpoint, 'completed persistent checkpoint')
    if settings['mode'] != 'local':
        if mode != 'smoke':
            raise RuntimeError('Local recovery tests require local mode')
        print('APACHE_FLINK_HEALTH_OK')
        return
    if mode in ('smoke', 'stage'):
        tag = uuid.uuid4().hex
        sensor = 'verify_'+tag
        initial = checkpoint()['id']
        write_lines([{'sensor': sensor, 'event_id': tag+'_1', 'value': 10},
                     {'sensor': sensor, 'event_id': tag+'_2', 'value': 20},
                     {'sensor': sensor, 'event_id': tag+'_2', 'value': 20}])
        wait_for(lambda: expect(sensor, 2, 30), 'count, sum and mean in committed output')
        wait_for(lambda: completed_after(initial), 'state persisted after processing')
        if len(results(sensor)) != 2:
            raise RuntimeError('Duplicate event affected output')
        if mode == 'stage':
            STAGED.write_text(json.dumps({'sensor': sensor, 'tag': tag}))
            STAGED.chmod(0o600)
            print('APACHE_FLINK_RECOVERY_STAGED')
        else:
            print('APACHE_FLINK_LOCAL_PROCESSING_OK')
    elif mode == 'check':
        staged = json.loads(STAGED.read_text())
        sensor, tag = staged['sensor'], staged['tag']
        initial = checkpoint()['id']
        write_lines([{'sensor': sensor, 'event_id': tag+'_2', 'value': 20},
                     {'sensor': sensor, 'event_id': tag+'_3', 'value': 30}])
        wait_for(lambda: expect(sensor, 3, 60), 'restored count, sum and mean')
        wait_for(lambda: completed_after(initial), 'completed checkpoint after recovery')
        if len(results(sensor)) != 3:
            raise RuntimeError('Replayed input created duplicate committed output')
        if not api('/jobs/'+JOB+'/checkpoints').get('latest', {}).get('restored'):
            raise RuntimeError('Job did not restore a persisted checkpoint')
        cleanup()
        print('APACHE_FLINK_CHECKPOINT_RECOVERY_OK')
    elif mode == 'restart':
        subprocess.run(['/usr/bin/python3', __file__, 'stage'], check=True)
        try:
            subprocess.run(['systemctl', 'restart', 'apache-flink-jobmanager', 'apache-flink-taskmanager'], check=True)
            subprocess.run(['/usr/bin/python3', __file__, 'check'], check=True)
        finally:
            subprocess.run(['systemctl', 'start', 'apache-flink-jobmanager', 'apache-flink-taskmanager'], check=True)
    else:
        raise RuntimeError('Unknown verification mode')


if __name__ == '__main__':
    main()
