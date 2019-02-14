#!/usr/bin/env python
"""Dynamic inventory script to pull component details from Ambari"""

import logging
import os
import sys
import argparse
import json
import re
import requests
 
logging.basicConfig(level=os.environ.get("AMBARI_LOG_LEVEL", "INFO"))

def process_args():
    """Process command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--test', action='store_true', help='used for testing, bypasses REST calls and uses local JSON', default=False)
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

        # Called with `--list`.
        if args.list:
            if args.test:
                logging.debug('Loading test JSON data')
                service_list = {
                    "SQOOP": {
                        "SQOOP": ["sandbox-hdp.hortonworks.com"]
                    },
                    "RANGER": {
                        "RANGER_TAGSYNC": ["sandbox-hdp.hortonworks.com"],
                        "RANGER_ADMIN": ["sandbox-hdp.hortonworks.com"],
                        "RANGER_USERSYNC": ["sandbox-hdp.hortonworks.com"]
                    },
                    "ZEPPELIN": {
                        "ZEPPELIN_MASTER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "ATLAS": {
                        "ATLAS_CLIENT": ["sandbox-hdp.hortonworks.com"],
                        "ATLAS_SERVER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "KNOX": {
                        "KNOX_GATEWAY": ["sandbox-hdp.hortonworks.com"]
                    },
                    "FLUME": {
                        "FLUME_HANDLER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "YARN": {
                        "APP_TIMELINE_SERVER": ["sandbox-hdp.hortonworks.com"],
                        "NODEMANAGER": ["sandbox-hdp.hortonworks.com"],
                        "RESOURCEMANAGER": ["sandbox-hdp.hortonworks.com"],
                        "YARN_CLIENT": ["sandbox-hdp.hortonworks.com"]
                    },
                    "PIG": {
                        "PIG": ["sandbox-hdp.hortonworks.com"]
                    },
                    "TEZ": {
                        "TEZ_CLIENT": ["sandbox-hdp.hortonworks.com"]
                    },
                    "MAPREDUCE2": {
                        "MAPREDUCE2_CLIENT": ["sandbox-hdp.hortonworks.com"],
                        "HISTORYSERVER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "OOZIE": {
                        "OOZIE_CLIENT": ["sandbox-hdp.hortonworks.com"],
                        "OOZIE_SERVER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "SPARK2": {
                        "SPARK2_THRIFTSERVER": ["sandbox-hdp.hortonworks.com"],
                        "SPARK2_CLIENT": ["sandbox-hdp.hortonworks.com"],
                        "LIVY2_SERVER": ["sandbox-hdp.hortonworks.com"],
                        "SPARK2_JOBHISTORYSERVER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "SLIDER": {
                        "SLIDER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "STORM": {
                        "NIMBUS": ["sandbox-hdp.hortonworks.com"],
                        "DRPC_SERVER": ["sandbox-hdp.hortonworks.com"],
                        "SUPERVISOR": ["sandbox-hdp.hortonworks.com"],
                        "STORM_UI_SERVER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "FALCON": {
                        "FALCON_CLIENT": ["sandbox-hdp.hortonworks.com"],
                        "FALCON_SERVER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "HDFS": {
                        "NAMENODE": ["sandbox-hdp.hortonworks.com"],
                        "SECONDARY_NAMENODE": ["sandbox-hdp.hortonworks.com"],
                        "NFS_GATEWAY": ["sandbox-hdp.hortonworks.com"],
                        "DATANODE": ["sandbox-hdp.hortonworks.com"],
                        "HDFS_CLIENT": ["sandbox-hdp.hortonworks.com"]
                    },
                    "ZOOKEEPER": {
                        "ZOOKEEPER_SERVER": ["sandbox-hdp.hortonworks.com"],
                        "ZOOKEEPER_CLIENT": ["sandbox-hdp.hortonworks.com"]
                    },
                    "HIVE": {
                        "HIVE_SERVER": ["sandbox-hdp.hortonworks.com"],
                        "HIVE_METASTORE": ["sandbox-hdp.hortonworks.com"],
                        "MYSQL_SERVER": ["sandbox-hdp.hortonworks.com"],
                        "WEBHCAT_SERVER": ["sandbox-hdp.hortonworks.com"],
                        "HIVE_CLIENT": ["sandbox-hdp.hortonworks.com"]
                    },
                    "KAFKA": {
                        "KAFKA_BROKER": ["sandbox-hdp.hortonworks.com"]
                    },
                    "AMBARI_INFRA": {
                        "INFRA_SOLR_CLIENT": ["sandbox-hdp.hortonworks.com"],
                        "INFRA_SOLR": ["sandbox-hdp.hortonworks.com"]
                    },
                    "HBASE": {
                        "HBASE_MASTER": ["sandbox-hdp.hortonworks.com"],
                        "HBASE_CLIENT": ["sandbox-hdp.hortonworks.com"],
                        "HBASE_REGIONSERVER": ["sandbox-hdp.hortonworks.com"]
                    }
                }
            else:
                logging.debug('Loading JSON data from ' + self.uri)
                service_list = self.get_service_list()

            ambari_inv = self.generate_ambari_inventory(service_list)

            print(json.dumps(ambari_inv))
        # '--host' not supported
        else:
            print('{}')

    # Handle API calls to Ambari
    def ambari_get(self, path):
        """Wrapper function for making REST calls to Ambari"""
        full_uri = self.uri + '/api/v1/clusters/' + self.cluster_name + path
        
        logging.debug(full_uri)

        return requests.get(
            full_uri,
            auth=(self.ambari_user, self.ambari_pass),
            verify=False)

    # hosts/services/components from Ambari
    def get_service_list(self):
        """Generates a list of services in Ambari"""
        services = {}

        logging.debug('Getting service list')
        result = self.ambari_get('/services')

        # pylint: disable=R0101,no-member
        if result.status_code == requests.codes.ok:
            for item in json.loads(result.text)['items']:
                services[item['ServiceInfo']['service_name']] = {}

            for service in services:
                logging.debug('Getting component list for service = ' + service)
                result = self.ambari_get('/services/' + service + '/components')

                if result.status_code == requests.codes.ok:
                    for item in json.loads(result.text)['items']:
                        component_name = item['ServiceComponentInfo']['component_name']
                        services[service][component_name] = []

                        logging.debug('Getting hosts for service = ' + service + ', component = ' + component_name)
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
                logging.debug('Adding service = ' + key_clus_srv)
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

                # strip doubling up names of services/components
                component_k = re.sub(service_k + '_', '', component_k)

                # rename component / service name duplication for clients
                if (service_k == component_k):
                    logging.debug('Changing component_k = ' + component_k + ' to client')
                    component_k = 'client'

                # rename infra_solr to infra_solr_server for consistency
                elif (service_k == 'ambari_infra' and component_k == 'infra_solr'):
                    logging.debug('Changing component_k = ' + component_k + ' to infra_solr_server')
                    component_k = 'infra_solr_server'

                key_clus_srv_comp = key_clus_srv + '_' + component_k

                if (key_clus_srv_comp not in inventory[key_clus_srv]['children']):
                    logging.debug('Adding component = ' + key_clus_srv_comp)
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
