#!/usr/bin/env python3
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

    relationship_id = get_relationship_id(rc1, "/keyva_demo/")
    rc1.replication.make_target_writable(relationship_id=relationship_id)
    rc2.replication.modify_source_relationship(relationship_id=relationship_id, source_root_read_only=True)
    time.sleep(15)
    rc1.replication.reverse_target_relationship(relationship_id=relationship_id, source_address=cluster2)
    rc2.replication.reconnect_target_relationship(relationship_id=relationship_id)


def get_relationship_id(rc, path):
    rels = rc.replication.list_target_relationship_statuses()
    for i in rels:
        if i['target_root_path'] == path:
            return i['id']
    raise RuntimeError("target root path relationship not found: {0}".format(path))


if __name__ == "__main__":
    main()