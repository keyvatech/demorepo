#!/usr/bin/env python3
from threading import Thread
from queue import Queue
import qumulo
from qumulo.rest_client import RestClient
import time


cluster1 = "mycluster1.local"
cluster2 = "mycluster2.local"
api_user = 'admin'
api_password = 'mypassword'


def main():
    # Initial setup
    rc1 = RestClient(cluster1, 8000)
    rc1.login(api_user, api_password)
    rc2 = RestClient(cluster2, 8000)
    rc2.login(api_user, api_password)
    try:
        print("Removing replicaiton relationship")
        relationship_id = get_relationship_id(rc1, "/keyva_demo/")
        rc1.replication.delete_source_relationship(relationship_id=relationship_id)
    except RuntimeError:
        pass

    print("Removing snapshot policies")
    for i in rc1.snapshot.list_policies()['entries']:
        rc1.snapshot.delete_policy(policy_id=i['id'])
    for i in rc2.snapshot.list_policies()['entries']:
        rc2.snapshot.delete_policy(policy_id=i['id'])

    print("Removing any created/replicated snapshots")
    for i in rc1.snapshot.list_snapshots()['entries']:
        if "keyva_demo" in i['name']:
            rc1.snapshot.delete_snapshot(snapshot_id=i['id'])
    for i in rc2.snapshot.list_snapshots()['entries']:
        if "keyva_demo" in i['name']:
            rc2.snapshot.delete_snapshot(snapshot_id=i['id'])

    time.sleep(5)
    print("Clearing directory tree")
    try:
        rc1.fs.delete_tree(path="/keyva_demo")
        rc2.fs.delete_tree(path="/keyva_demo")
    except qumulo.lib.request.RequestError as err:
        if "fs_no_such_entry_error" not in str(err) and "tree_delete_already_started_error" not in str(err):
            raise

    print("Waiting on dir tree deletion")
    rm1_done = False
    rm2_done = False
    for i in range(600):
        if not rm1_done:
            try:
                rm1 = rc1.fs.tree_delete_status(path='/keyva_demo/')
                print("Cluster1: {0} files left".format(rm1['remaining_files']))
            except qumulo.lib.request.RequestError as err:
                if "fs_no_such_entry_error" in str(err):
                    rm1_done = True
        if not rm2_done:
            try:
                rm1 = rc2.fs.tree_delete_status(path='/keyva_demo/')
                print("Cluster2: {0} files left".format(rm1['remaining_files']))
            except qumulo.lib.request.RequestError as err:
                if "fs_no_such_entry_error" in str(err):
                    rm2_done = True
        if rm1_done and rm2_done:
            break
        time.sleep(5)

    # Directory and share creation
    dir_count = 2000
    dir_list = ["dir{0:05d}".format(i) for i in range(dir_count)]
    work_queue = Queue()
    for i in dir_list:
        work_queue.put(i)
    thread_count = min(50, len(dir_list))
    print('Spinning up worker threads...')
    threads = []
    for i in range(thread_count):
        worker = Thread(target=process_dirs, args=(work_queue, i), name="thread{0}".format(i))
        threads.append(worker)
        worker.start()
    print('\nWaiting for workers to finish\n\n')
    for t in threads:
        t.join()
    print('Done')


def process_dirs(work_queue, i):
    print("Thread{0}: starting".format(i))
    rc1 = RestClient(cluster1, 8000)
    rc1.login(api_user, api_password)
    rc2 = RestClient(cluster2, 8000)
    rc2.login(api_user, api_password)
    while not work_queue.empty():
        directory = work_queue.get()
        if int(directory[-5:]) % 100 == 0:
            print("Thread{0:02d}: removing SMB share {1}".format(i, directory))
        delete_smb_share(rc1, share_name="test_{0}".format(directory), fs_path="/keyva_demo/{0}".format(directory))
        delete_smb_share(rc2, share_name="test_{0}".format(directory), fs_path="/keyva_demo/{0}".format(directory))
        work_queue.task_done()


def delete_smb_share(rc, share_name, fs_path):
    try:
        rc.smb.smb_delete_share(name=share_name)
    except qumulo.lib.request.RequestError as err:
        if "smb_share_doesnt_exist_error" not in str(err):
            raise


def get_relationship_id(rc, path):
    rels = rc.replication.list_source_relationship_statuses()
    for i in rels:
        if i['target_root_path'] == path:
            return i['id']
    raise RuntimeError("target root path relationship not found: {0}".format(path))


if __name__ == "__main__":
    main()