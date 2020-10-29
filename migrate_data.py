#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MLFlow 1.4.0 Migration From FileBased store to mysql store
Credits:
* Based on work of bnekolny https://gist.github.com/bnekolny/8ec96faa0d57913eea6bb7848c06b912
* Latest version https://gist.github.com/weldpua2008/7f0c4644d247bd0fc7ba9a83c2d337d5

requirements:
    pyyaml version 5.1 was required
    pip3.6 install -U pyyaml


Usage:

  * Import all experements 
    migrate_data.py.py \
    --wipe-db \
    --mlruns-dir /opt/mlflow/mlruns > /tmp/migration_inserts_full.sql
    mysql -D  mlflow_storage < /tmp/migration_inserts_full.sql 2>> /var/log/mlflow-migration-full.log
   
  * Run periodically import(every 10 minutes by cron) of the latest runs
    migrate_data.py.py \
    --mlruns-dir /opt/mlflow/mlruns --partial-update --partial-last-seconds 1000 \
    --mlruns-dir /opt/mlflow/mlruns > /tmp/migration_inserts_partial.sql
    mysql -D  mlflow_storage < /tmp/migration_inserts_partial.sql 2>> /var/log/mlflow-migration-full.log

Important:
* To fix  Cannot import data ERROR 1406 (22001): Data too long (https://github.com/mlflow/mlflow/issues/2814)
  You can change schema (uncomment ALTER TABLE `params` MODIFY value VARCHAR(6512) NOT NULL;)


"""
import os
from pathlib import Path
import sys
import yaml
import codecs
import argparse
from functools import partial
from datetime import datetime

error = partial(print, file=sys.stderr)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mlruns-dir",
        type=Path,
        default=Path("/opt/mlflow/mlruns/"),
        help="Path to the MLflow runs data directory",
    )
    parser.add_argument(
        '--wipe-db',
        action="store_true",
        default=False,
        help="Add SQL statements for flush all data in database")
    parser.add_argument(
        '--partial-update',
        action="store_true",
        default=False,
        help="Prepare partial SQL statements")
    parser.add_argument(
        "--partial-last-seconds",
        type=int,
        default=900,
        help="Prepare dump for MLFlow Runs recorded since last seconds",
    )
    parser.add_argument(
        "--partial-since-seconds",
        type=int,
        default=120,
        help="Skip MLFlow Runs created less then since last seconds",
    )

    return parser.parse_args()


def progress(_cur, _max):
    p = round(100*_cur/_max)
    b = f"Progress: {p}% - ["+"."*int(p/5)+" "*(20-int(p/5))+"]"
    error(b, end="\r")

def load_metadata_file(fpath):
    if os.path.exists(fpath):
        with open(fpath) as fp:
            try:
                return yaml.full_load(fp)
            except AttributeError:
                return yaml.load(fp)



def process_experiment(rootDir, experiment_id, experiment_names, partial_update,
                       partial_last_seconds, partial_since_seconds):
    status = {1: 'RUNNING', 2: 'SCHEDULED', 3: 'FINISHED', 4: 'FAILED', 5: 'KILLED'}
    sourceType = {1: 'NOTEBOOK', 2: 'JOB', 3: 'PROJECT', 4: 'LOCAL', 5: 'UNKNOWN'}
    EPATH = "{root}/{experiment}".format(root=rootDir, experiment=experiment_id)
    NOW = datetime.now()

    experiment = load_metadata_file("{experiment}/meta.yaml".format(experiment=EPATH))
    if experiment is None:
        return
    experiment['experiment_id'] = experiment.get('experiment_id', experiment_id)
    if experiment_id == 0 or experiment_id == '0':
        print("SET sql_mode='NO_AUTO_VALUE_ON_ZERO';")

    if len(experiment['name']) < 1:
        error("experiment name is empty at {experiment}/meta.yaml".format(experiment=EPATH))
        return
    if experiment['name'] in experiment_names:
        error("experiment {name} exists, appending _".format(name=experiment['name']))
        experiment['name'] = "{}_".format(experiment['name'])
    experiment_names.add(experiment['name'])
    experiment_insert = "INSERT IGNORE INTO `experiments` (`experiment_id`, `name`, `artifact_location`, `lifecycle_stage`) VALUES ({0}, '{1}', '{2}', '{3}');".format(
        experiment['experiment_id'],
        experiment['name'],
        experiment['artifact_location'],
        experiment.get('lifecycle_stage','active'))
    print("-- {root}/{experiment}".format(root=rootDir, experiment=experiment['experiment_id']))
    print(experiment_insert)
    for run_uuid in os.listdir("{experiment}".format(experiment=EPATH)):
        if run_uuid in ['meta.yaml']:
            continue
        RPATH = "{experiment}/{run}".format(experiment=EPATH, run=run_uuid)
        if partial_update is True:
            diff = int(NOW.timestamp()) - int(min(os.path.getctime(RPATH), os.path.getmtime(RPATH)))
            if (diff > int(partial_last_seconds)):
                continue
            if (diff < int(partial_since_seconds)):
                continue

        run = load_metadata_file("{run}/meta.yaml".format(run=RPATH))
        if run is None:
            continue
        run['run_uuid'] = run.get('run_uuid', run_uuid)
        run_insert = "INSERT IGNORE INTO `runs` (" \
            "`run_uuid`, `name`, `source_type`, `source_name`, `entry_point_name`, `user_id`, `status`, `start_time`, `end_time`, `source_version`, `lifecycle_stage`, `artifact_uri`, `experiment_id`" \
            ") VALUES ( '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', {7}, {8}, '{9}', '{10}', '{11}', {12});".format(
                    run['run_uuid'],
                    run['name'],
                    sourceType[int(run['source_type'])],
                    run['source_name'],
                    run['entry_point_name'],
                    run['user_id'],
                    status[int(run['status'])],
                    run['start_time'],
                    "NULL" if run['end_time'] is None else run['end_time'],
                    run['source_version'],
                    run.get('lifecycle_stage', 'active'),
                    run['artifact_uri'],
                    experiment_id)
        print(run_insert)

        # Tags
        tag_path = "{experiment}/{run}/tags".format(experiment=EPATH, run=run_uuid)
        for tag_fname in Path(tag_path).rglob("*"):
            if os.path.isdir(tag_fname):
                continue
            tag = str(tag_fname.relative_to(tag_path))
            with codecs.open(tag_fname, mode='r', encoding="utf-8") as f:
                line = f.read()
                tag_insert = "INSERT IGNORE INTO `tags` (" \
                    "`key`, `value`, `run_uuid`" \
                    ") VALUES ( '{0}', '{1}', '{2}' );".format(
                        tag,
                        line.strip().replace("\\", "\\\\").replace("'", "\\'") if "'" in line else line,
                        run['run_uuid'])
                print(tag_insert)

        # Metrics
        # Latest_Metrics
        metrics_path = "{experiment}/{run}/metrics".format(experiment=EPATH, run=run_uuid)
        for metrics_fname in Path(metrics_path).rglob("*"):
            if os.path.isdir(metrics_fname):
                continue
            with open(metrics_fname,'r') as f:
                lines = set(f.readlines())
                metric = str(metrics_fname.relative_to(metrics_path))
                for line in lines:
                    #split
                    timestamp, val, step = line.split()
                    metric_insert = "INSERT IGNORE INTO `metrics` (" \
                        "`key`, `value`, `timestamp`, `run_uuid`" \
                        ") VALUES ( '{0}', '{1}', {2}, '{3}');".format(
                            metric,
                            val,
                            timestamp,
                            run_uuid)
                    print(metric_insert)
                    latest_metric_insert = "INSERT IGNORE INTO `latest_metrics` (" \
                        "`key`, `value`, `timestamp`, `run_uuid`" \
                        ") VALUES ( '{0}', '{1}', {2}, '{3}');".format(
                            metric,
                            val,
                            timestamp,
                            run_uuid)
                    print(latest_metric_insert)
                    line = f.readline()
        # Params
        param_path = "{experiment}/{run}/params".format(experiment=EPATH, run=run_uuid)
        for param_fname in Path(param_path).rglob("*"):
            if os.path.isdir(param_fname):
                continue
            param = str(param_fname.relative_to(param_path))
            with codecs.open(param_fname, mode='r', encoding="utf-8") as f:
                line = f.read()
                param_insert = "INSERT IGNORE INTO `params` (" \
                    "`key`, `value`, `run_uuid`" \
                    ") VALUES ( '{0}', '{1}', '{2}' );".format(
                        param.replace("'", "\\'") if "'" in param else param,
                        line.strip().replace("\\", "\\\\").replace("'", "\\'") if "'" in line else line,
                        run_uuid)
                print(param_insert)


def main():
    """
    Execution for me was:
    `python migrate_data.py > ./migration_inserts.sql`
    `mysq < ./migration_inserts.sql`

    NOTE: A few things to know about the script here:
    - Artifacts were stored remotely, so no artifact migration
    - experiment source_version is never set
    - experiment lifecycle_stage is always active for us, I avoided the mapping from int -> str
    - metric timestamp is made up, since it was tracked as an array in filesystem and as an epoch in the DB

    """

    args = parse_args()
    num_experiments = len(os.listdir(args.mlruns_dir))+1
    error(f"Migration of {num_experiments-1} experements")
    print("-- MLFlow SQL Dump %s" % datetime.now())
    if args.wipe_db is True:
        print("""
        SET FOREIGN_KEY_CHECKS=0;
        TRUNCATE `runs`;
        TRUNCATE `experiments`;
        TRUNCATE `metrics`;
        TRUNCATE `params`;
        TRUNCATE `tags`;
        TRUNCATE `latest_metrics`;
        SET FOREIGN_KEY_CHECKS=1;
        -- ALTER TABLE `params` MODIFY value VARCHAR(6512) NOT NULL;
        """)
    experiment_names = set()

    for _step, experiment_id in enumerate(os.listdir(args.mlruns_dir)):
        if experiment_id in ['.trash']:
            continue
        process_experiment(rootDir=args.mlruns_dir, experiment_id=experiment_id,
                           experiment_names=experiment_names,
                           partial_update=args.partial_update,
                           partial_last_seconds=args.partial_last_seconds,
                           partial_since_seconds=args.partial_since_seconds)
        progress(_step, num_experiments)
        if experiment_id in ['.trash']:
            continue
    progress(num_experiments, num_experiments)
    error("..."*5, end="\r")
    error("DONE")


if __name__ == '__main__':
    main()
