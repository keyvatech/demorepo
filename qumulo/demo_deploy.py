#!/usr/bin/env python3
# Prior to running be sure you ran "pip install qumulo_api"
from threading import Thread
import queue
from io import StringIO
from io import BytesIO
import time
import sys
import qumulo
from qumulo.rest_client import RestClient


cluster1 = "mycluster1.local"
cluster2 = "mycluster2.local"
api_user = 'admin'
api_password = 'mypassword'
replication_sync = False


def main():
    global replication_sync
    # Initial setup
    rc1 = RestClient(cluster1, 8000)
    rc1.login(api_user, api_password)
    rc2 = RestClient(cluster2, 8000)
    rc2.login(api_user, api_password)
    create_dir(rc1, name="keyva_demo", dir_path='/')
    create_snapshots(rc1)
    setup_replication(rc1, rc2)
    # Directory and share creation
    dir_count = 2000

    dir_list = ["dir{0:05d}".format(i) for i in range(dir_count)]
    work_queue1 = queue.Queue()
    work_queue2 = queue.Queue()
    for i in dir_list:
        work_queue1.put(i)
        work_queue2.put(i)
    thread_count = min(50, len(dir_list))
    print('Spinning up worker threads...')
    threads = []
    for i in range(thread_count):
        worker = Thread(target=process_dirs, args=(work_queue1, work_queue2, i), name="thread{0}".format(i))
        threads.append(worker)
        # worker.setDaemon(True)
        worker.start()
    print("\n\nCreating {0} '/keyva_demo/dir00000/' directories".format(dir_count))
    while not work_queue1.empty():
        sys.stdout.write("\rDirectories left: {0:05d}".format(work_queue1.qsize()))
        sys.stdout.flush()
    print("\n\nQueue 1 empty")
    print("\n\nWaiting on initial replication to finish...")
    time.sleep(60)
    replication_sync = False
    while True:
        my_status = None
        remaining = ""
        rep_status = rc1.replication.list_source_relationship_statuses()
        for i in rep_status:
            if i['target_root_path'] == '/keyva_demo/':
                my_status = i
                break
        if my_status and my_status['state'] == "ESTABLISHED" and my_status['job_state'] == 'REPLICATION_NOT_RUNNING':
            replication_sync = "success"
            print("\nInitial replication complete")
            break
        try:
            if my_status['replication_job_status']:
                remaining = "- {0:.0%} done".format(
                    my_status['replication_job_status']['percent_complete'])
        except KeyError:
            pass
        sys.stdout.write("\rWaiting for replication sync {0}".format(remaining))
        sys.stdout.flush()
        # time.sleep(5)
    print('\nCreating {0} SMB shares and {0} test files'.format(dir_count))
    while not work_queue2.empty():
        sys.stdout.write("\rItems left: {0:05d}".format(work_queue2.qsize()))
        sys.stdout.flush()
    print('\n\nQueue 2 empty')
    for t in threads:
        t.join()

    print('Done')


def process_dirs(work_queue1, work_queue2, i):
    global replication_sync
    print("Thread{0}: starting".format(i))
    rc1 = RestClient(cluster1, 8000)
    rc1.login(api_user, api_password)
    rc2 = RestClient(cluster2, 8000)
    rc2.login(api_user, api_password)
    while not work_queue1.empty():
        try:
            directory = work_queue1.get(timeout=30)
        except queue.Empty:
            break
        # if int(directory[-5:]) % 100 == 0:
        #     print("Thread{0:02d}: making dir {1}".format(i, directory))
        create_dir(rc1, name=directory, dir_path='/keyva_demo')
        work_queue1.task_done()
    counter = 0
    while replication_sync != "success":
        if counter == 600:
            raise RuntimeError("Timed out waiting on replication sync.")
        time.sleep(1)
        counter += 1
    while not work_queue2.empty():
        try:
            directory = work_queue2.get(timeout=30)
        except queue.Empty:
            break
        # if int(directory[-5:]) % 100 == 0:
        #     print("Thread{0:02d}: making smb share test_{1}".format(i, directory))
        create_smb_share(rc1, share_name="test_{0}".format(directory), fs_path="/keyva_demo/{0}".format(directory))
        create_smb_share(rc2, share_name="test_{0}".format(directory), fs_path="/keyva_demo/{0}".format(directory))
        create_test_file(rc1, filename="testfile", dir_path="/keyva_demo/{0}".format(directory))
        work_queue2.task_done()
    print("Thread{0}: complete".format(i))


def create_dir(rc, name, dir_path='/'):
    # rc.fs.delete(path="{0}/{1}".format(dir_path, name))
    try:
        rc.fs.create_directory(name=name, dir_path=dir_path)
    except qumulo.lib.request.RequestError as err:
        if "fs_entry_exists_error" not in str(err):
            raise


def create_smb_share(rc, share_name, fs_path):
    # rc.smb.smb_delete_share()
    permissions = [{u'rights': [u'READ'], u'type': u'ALLOWED', u'trustee': {u'name': u'qumulo-grp2'}},
                   {u'rights': [u'READ', u'WRITE'], u'type': u'ALLOWED', u'trustee': {u'name': u'qumulo-grp1'}},
                   {u'rights': [u'ALL'], u'type': u'ALLOWED', u'trustee': {u'name': u'quser1'}},
                   {u'rights': [u'ALL'], u'type': u'ALLOWED', u'trustee': {u'name': u'admin'}}]
    try:
        rc.smb.smb_add_share(share_name=share_name, fs_path=fs_path, description="", permissions=permissions)
    except qumulo.lib.request.RequestError as err:
        if "smb_share_already_exists_error" not in str(err):
            raise


def create_test_file(rc, filename, dir_path):
    file_path = '{0}/{1}'.format(dir_path, filename)
    # if int(dir_path[-5:]) % 100 == 0:
    #     print('creating test file: {0}'.format(file_path))
    try:
        rc.fs.create_file(name=filename, dir_path=dir_path)
    except qumulo.lib.request.RequestError as err:
        if "fs_entry_exists_error" not in str(err):
            raise
    fh = BytesIO('File at: {0}\n'.format(file_path).encode('utf-8'))
    rc.fs.write_file(data_file=fh, path=file_path)
    fh.close()


def setup_replication(rc1, rc2):
    print("Setting up cluster replication")
    rels = rc1.replication.list_source_relationship_statuses()
    for i in rels:
        if i['source_root_path'] == '/keyva_demo/':
            print("Source relationship already exists: /keyva_demo/")
            return
    try:
        snapshot_policies = [{"target_expiration": "same_as_policy",
                              "id": get_snapshot_policy_id(rc1, "keyva_demo_hourly")}]
        rel = rc1.replication.create_source_relationship(
            target_path='/keyva_demo', address=cluster2, source_path='/keyva_demo',
            replication_mode="REPLICATION_SNAPSHOT_POLICY_WITH_CONTINUOUS",
            replication_enabled=True, snapshot_policies=snapshot_policies)
        rc2.replication.authorize(relationship_id=rel['id'], allow_non_empty_directory=True, allow_fs_path_create=True)
    except qumulo.lib.request.RequestError as err:
        if "relationship_manager_target_contains_another_relationship_error" not in str(err):
            raise
        else:
            print("Relationship already exists: /keyva_demo")


def get_snapshot_policy_id(rc, name):
    policies = rc.snapshot.list_policies()['entries']
    for i in policies:
        if i['name'] == name:
            return i['id']
    raise RuntimeError("policy not found: {0}".format(name))


def create_snapshots(rc):
    schedules = {
        "keyva_demo_hourly": {
            "creation_schedule": {
                "fire_every_interval": "FIRE_IN_MINUTES",
                "window_start_minute": 0,
                "window_end_hour": 23,
                "frequency": "SCHEDULE_HOURLY_OR_LESS",
                "on_days": ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"],
                "window_start_hour": 0,
                "window_end_minute": 59,
                "timezone": "America/Chicago",
                "fire_every": 60
            },
            "expiration_time_to_live": "36hours"
        },
        "keyva_demo_daily": {
            "creation_schedule": {
                "timezone": "America/Chicago",
                "frequency": "SCHEDULE_DAILY_OR_WEEKLY",
                "on_days": ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"],
                "minute": 0,
                "hour": 0
            },
            "expiration_time_to_live": "7days"
        },
        "keyva_demo_weekly": {
            "creation_schedule": {
                "timezone": "America/Chicago",
                "frequency": "SCHEDULE_DAILY_OR_WEEKLY",
                "on_days": ["SUN"],
                "minute": 0,
                "hour": 1
            },
            "expiration_time_to_live": "4weeks"
        },
        "keyva_demo_monthly": {
            "creation_schedule": {
                "timezone": "America/Chicago",
                "frequency": "SCHEDULE_MONTHLY",
                "minute": 0,
                "hour": 2,
                "day_of_month": 1
            },
            "expiration_time_to_live": "3months"
        }
    }
    dir_id = get_file_id(rc, "/keyva_demo")
    for schedule in schedules.keys():
        try:
            rc.snapshot.create_policy(name=schedule,
                                      schedule_info=schedules[schedule],
                                      directory_id=dir_id)
            print("Created snapshot policy: {0}".format(schedule))
        except qumulo.lib.request.RequestError as err:
            if "snapshot_policy_name_in_use_error" not in str(err):
                raise
            else:
                print("Snapshot policy exists: {0}".format(schedule))


def get_file_id(rc, path):
    data = rc.fs.get_file_attr(path=path)
    return data["file_number"]


if __name__ == "__main__":
    main()