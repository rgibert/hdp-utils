#!/usr/bin/env python
"""Dynamic Prometheus service discovery script"""

import logging
import os
import sys
import argparse
import json
import re
import urllib3
import requests

logging.basicConfig(level=os.environ.get("AMBARI_LOG_LEVEL", "INFO"))

# Disable SSL warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def process_args():
    """Process command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--file',
        action='store',
        help='where to store the resulting service discovery file'
    )
    return parser.parse_args()

def get_env_var(var_name, exit_on_missing=True):
    """Retrieve the value of environment variables"""
    if os.environ.get(var_name) is None:
        if exit_on_missing:
            print("Required {} environment variable not set.".format(var_name))
            sys.exit(1)
        else:
            return None
    else:
        return os.environ[var_name]

class AmbariPrometheusServiceDiscovery():
    """Class for generating Ambari Prometheus host lists"""

    def __init__(self):
        self._cluster_name = get_env_var('AMBARI_CLUSTER_NAME')
        self._uri = get_env_var('AMBARI_URI')
        self._ambari_user = get_env_var('AMBARI_USER_NAME')
        self._ambari_pass = get_env_var('AMBARI_USER_PASS')

        args = process_args()

        host_component_list = self.get_host_component_list()
        targets = self.generate_targets(host_component_list)

        logging.debug('writing to %s', args.file)
        with open(args.file, 'w') as json_file:
            json.dump(targets, json_file)

    def ambari_get(self, path):
        """Wrapper function for making REST calls to Ambari"""
        full_uri = self._uri + '/api/v1/clusters/' + self._cluster_name + path

        logging.debug(full_uri)

        return requests.get(
            full_uri,
            auth=(self._ambari_user, self._ambari_pass),
            verify=False)

    def get_host_component_list(self):
        """Generates a list of hosts and their installed components"""
        hosts = {}

        logging.debug('Getting host list')
        result = self.ambari_get('/hosts')

        # pylint: disable=R0101,no-member
        if result.status_code == requests.codes.ok:
            for item in json.loads(result.text)['items']:
                hosts[item['Hosts']['host_name']] = []

            for host_k, _ in hosts.items():
                logging.debug('Getting component list for host = %s', host_k)
                result = self.ambari_get('/hosts/' + host_k)

                # pylint: disable=R0101,no-member
                if result.status_code == requests.codes.ok:
                    for item in json.loads(result.text)['host_components']:
                        hosts[host_k].append(item['HostRoles']['component_name'])
                else:
                    result.raise_for_status()
        else:
            result.raise_for_status()

        return hosts

    def generate_targets(self, hosts):
        """Converts a service list into Prometheus service discovery format"""

        ambari_host = re.sub(r'https?:\/\/', '', self._uri)
        ambari_host = re.sub(r':\d+', '', ambari_host)

        master_target_index = 0
        worker_target_index = 1

        targets = [
            {
                'targets': ['ambari_host'],
                'labels': {
                    'hadoop_cluster': self._cluster_name,
                    'node_type': 'master'
                }
            },
            {
                'targets': [],
                'labels': {
                    'hadoop_cluster': self._cluster_name,
                    'node_type': 'worker'
                }
            }
        ]

        # Loop over hosts
        for host, components in hosts.items():
            is_master = False

            for component in components:
                if component in (
                        'JOURNALNODE',
                        'ZOOKEEPER_SERVER',
                        'HIVE_SERVER',
                        'NAMENODE',
                        'RESOURCEMANAGER'):
                    is_master = True

            if host not in targets[master_target_index]['targets'] and host not in targets[worker_target_index]['targets']:
                if is_master:
                    logging.debug('%s identified as master node', host)
                    targets[master_target_index]['targets'].append(host)
                else:
                    logging.debug('%s identified as worker node', host)
                    targets[worker_target_index]['targets'].append(host)
            else:
                logging.debug('%s already in output', host)

        return targets

if __name__ == "__main__":
    AmbariPrometheusServiceDiscovery()
