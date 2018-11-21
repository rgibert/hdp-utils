# ansible_inventory.py

## Installing Requirements

pip install -r requirements.txt

## Usage

### Direct

```shell
AMBARI_CLUSTER_NAME=cluster AMBARI_USER_NAME=name AMBARI_USER_PASS=pass AMBARI_URI=uri ./ansible-inventory.py --list
```

### From Ansible

```shell
AMBARI_CLUSTER_NAME=cluster AMBARI_USER_NAME=name AMBARI_USER_PASS=pass AMBARI_URI=uri ansible-playbook -i ./ansible-inventory.py playbook.yml
```