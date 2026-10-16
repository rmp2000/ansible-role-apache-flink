"""Restore the newest completed externalized checkpoint when the JM starts."""
import json
import os
from pathlib import Path

config = json.loads(Path('/etc/apache-flink/job.json').read_text())
root = Path(config['data_directory'])
checkpoints = list((root/'checkpoints').glob('*/chk-*/_metadata'))
args = ['/opt/apache-flink/bin/standalone-job.sh', 'start-foreground',
        '--job-classname', 'org.data_platform.MeasurementJob',
        '--job-id', 'd1a7a00000000000000000000000000001']
if checkpoints:
    latest = max(checkpoints, key=lambda path: path.stat().st_mtime_ns)
    args += ['--fromSavepoint', latest.parent.as_uri()]
    print('APACHE_FLINK_RESTORE_CHECKPOINT '+str(latest.parent), flush=True)
args += ['/etc/apache-flink/job.json']
os.execv(args[0], args)
