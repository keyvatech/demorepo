#!/usr/bin/env python
import os
import qumulo
from qumulo.rest_client import RestClient
from ansible.module_utils.basic import AnsibleModule

DOCUMENTATION = '''
---
module: qumulo_upload

short_description: Example module to upload a file with ansible and Qumulo API

description:
    - "This module uses the Qumulo REST API to upload files"

options:
    user:
        description:
            - User name to log in with
        required: true
    password:
        description:
            - Password for user
        required: true
    cluster_address:
        description:
            - Address for Qumulo cluster
        required: true
    cluster_port:
        description:
            - Port for Qumulo api
        required: false
        default: 8000        
    src:
        description:
            - File source path
        required: true
    dest:
        description:
            - File destination path on Qumulo Filesystem
        required: true
    
author:
    - Brad Johnson, Keyva 
    - https://github.com/keyvatech
    - https://keyvatech.com/ 
'''

EXAMPLES = '''
- name: Upload file to qumulo
  qumulo_upload:
    user: "{{ qumulo_user }}"
    password: "{{ qumulo_password }}"
    cluster_address: "{{ qumulo_address }}"
    src: "myfile.txt"
    dest: "/keyva_demo/newdir/myfile.txt"
'''

RETURN = '''
'''


def main():
    """
    Main ansible module function
    """
    module = AnsibleModule(
        argument_spec=dict(
            user=dict(required=True, type='str'),
            password=dict(required=True, type='str', no_log=True),
            cluster_address=dict(required=True, type='str'),
            cluster_port=dict(required=False, type='int', default='8000'),
            src=dict(required=True, type='str'),
            dest=dict(required=True, type='str')
        )
    )
    user = module.params['user']
    password = module.params['password']
    cluster_address = module.params['cluster_address']
    cluster_port = module.params['cluster_port']
    src = module.params['src']
    dest = module.params['dest']

    try:
        qumulo_upload(user, password, cluster_address, cluster_port, src, dest)
    except qumulo.lib.request.RequestError as err:
        module.fail_json(msg="{0}".format(err))

    module.exit_json(changed=True)


def qumulo_upload(user, password, cluster_address, cluster_port, src, dest):
    """
    Function to read file and upload it to the Qumulo file system
    """
    # Connect to Qumulo REST API
    rc = RestClient(cluster_address, cluster_port)
    rc.login(user, password)
    # Split filename and directory
    dir_path, filename = os.path.split(dest)
    # Make sure the directory we are uploading to exists
    qumulo_mkdir_p(rc, dir_path)
    # Open local file in binary read-only mode
    with open(src, 'rb') as fh:
        # Initialize file
        rc.fs.create_file(name=filename, dir_path=dir_path)
        # Write data to file
        rc.fs.write_file(data_file=fh, path=dest)


def qumulo_mkdir_p(rc, path):
    """
    Function to create target directory and any missing parents
    """
    basepath = "/"
    for name in path.split("/")[1:]:
        try:
            rc.fs.create_directory(name=name, dir_path=basepath)
        except qumulo.lib.request.RequestError as err:
            # Catch expected error for existing dirs
            if "fs_entry_exists_error" not in str(err):
                # If unexpected error re-raise
                raise
        finally:
            basepath = os.path.join(basepath, name)


if __name__ == '__main__':
    main()
