#!/usr/bin/env python
"""Dynamic inventory script to pull component details from Ambari"""

import os
import sys
import argparse
import json
import re
import requests


def process_args():
    """Process command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--host', action='store')
    return parser.parse_args()

class AmbariInventory(object):
    """Class for generating Ambari inventories"""

    def __init__(self):
        if os.environ.get('AMBARI_CLUSTER_NAME') is None:
            print("Required AMBARI_CLUSTER_NAME environment variable not set.")
            sys.exit(1)
        elif os.environ.get('AMBARI_USER_NAME') is None:
            print("Required AMBARI_USER_NAME environment variable not set.")
            sys.exit(1)
        elif os.environ.get('AMBARI_USER_PASS') is None:
            print("Required AMBARI_USER_PASS environment variable not set.")
            sys.exit(1)
        elif os.environ.get('AMBARI_URI') is None:
            print("Required AMBARI_URI environment variable not set.")
            sys.exit(1)

        # Get the cluster name from the OS environment variable AMBARI_CLUSTER_NAME
        self.cluster_name = os.environ['AMBARI_CLUSTER_NAME']
        self.uri = os.environ['AMBARI_URI']
        self.ambari_user = os.environ['AMBARI_USER_NAME']
        self.ambari_pass = os.environ['AMBARI_USER_PASS']

        args = process_args()
        service_list = self.get_service_list()
        #service_list = json.load(open('ansible-inventory-sample.json'))

        ambari_inv = self.generate_ambari_inventory(service_list)

        # Called with `--list`.
        if args.list:
            print(json.dumps(ambari_inv))
        elif args.host:
            print('')

    # Handle API calls to Ambari
    def ambari_get(self, path):
        """Wrapper function for making REST calls to Ambari"""
        return requests.get(
            self.uri +
            '/api/v1/clusters/' +
            self.cluster_name +
            path,
            auth=(self.ambari_user, self.ambari_pass),
            verify=False)

    # hosts/services/components from Ambari
    def get_service_list(self):
        """Generates a list of services in Ambari"""
        services = {}

        result = self.ambari_get('/services')

        # pylint: disable=R0101,no-member
        if result.status_code == requests.codes.ok:
            for item in json.loads(result.text)['items']:
                services[item['ServiceInfo']['service_name']] = {}

            for service in services:
                result = self.ambari_get('/services/' + service + '/components')

                if result.status_code == requests.codes.ok:
                    for item in json.loads(result.text)['items']:
                        component_name = item['ServiceComponentInfo']['component_name']
                        services[service][component_name] = []

                        result = self.ambari_get(
                            '/services/' +
                            service +
                            '/components/' +
                            component_name +
                            '?fields=host_components/HostRoles/host_name')

                        if result.status_code == requests.codes.ok:
                            for host in json.loads(result.text)['host_components']:
                                services[service][component_name].append(
                                    host['HostRoles']['host_name'])

        return services

    def generate_ambari_inventory(self, services):
        """Converts a service list into Ansible inventory format"""

        ambari_host = re.sub(r'https?:\/\/', '', self.uri)
        ambari_host = re.sub(r':\d+', '', ambari_host)

        # Default inventory
        inventory = {
            'all': {
                'hosts': []
            },
            self.cluster_name: {
                'children': []
            },
            'hadoop': {
                'children': [self.cluster_name]
            },
            'hdp': {
                'children': 'hadoop'
            },
            self.cluster_name + '_ambari_agent': {
                'children': ['hadoop']
            },
            self.cluster_name + '_ambari_server': {
                'hosts': [ambari_host]
            },
            self.cluster_name + '_ambari': {
                'children': [self.cluster_name + '_ambari_agent',
                             self.cluster_name + '_ambari_server']
            },
            'ambari_agent': {
                'children': [self.cluster_name + '_ambari_agent']
            },
            'ambari_server': {
                'children': [self.cluster_name + '_ambari_server']
            },
            'ambari': {
                'children': [
                    'ambari_agent',
                    'ambari_server'
                ]
            },
            '_meta': {
                'hostvars': {}
            }
        }

        # Loop over services
        for service_k, service_v in services.iteritems():
            # convert service_k to lower case
            service_k = service_k.lower()

            key_clus_srv = self.cluster_name + '_' + service_k

            # Add the service group as a child of the cluster group
            if key_clus_srv not in inventory[self.cluster_name]['children']:
                inventory[self.cluster_name]['children'].append(key_clus_srv)

            # Create a group for the service which will have all the service components as children
            inventory[key_clus_srv] = {
                'children': [],
                'vars': {}
            }

            inventory[service_k] = {
                'children': [key_clus_srv]
            }

            # Loop over components
            for component_k, component_v in service_v.iteritems():
                # convert component_k to lower case
                component_k = component_k.lower()

                key_clus_srv_comp = key_clus_srv + '_' + component_k

                # strip doubling up names of services/components
                component_k = re.sub(service_k + '_', '', component_k)

                # rename component / service name duplication for clients
                if (service_k == component_k):
                    component_k = 'client'

                # rename infra_solr to infra_solr_server for consistency
                elif (service_k == 'ambari_infra' and component_k == 'infra_solr'):
                    component_k = 'infra_solr_server'

                if (key_clus_srv_comp not in inventory[key_clus_srv]['children']):
                    inventory[key_clus_srv]['children'].append(key_clus_srv_comp)

                inventory[key_clus_srv_comp] = {
                    'hosts': [],
                    'vars': {}
                }

                inventory[service_k + '_' + component_k] = {
                    'children': [key_clus_srv_comp]
                }

                for host in component_v:
                    inventory[key_clus_srv_comp]['hosts'].append(host)

                    if host not in inventory['all']['hosts']:
                        inventory['all']['hosts'].append(host)

        return inventory

if __name__ == "__main__":
    AmbariInventory()
