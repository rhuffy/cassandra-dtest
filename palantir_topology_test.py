import re
import time
import pytest
import logging
import os
import random
import string

from threading import Thread

from cassandra import ConsistencyLevel
from ccmlib.node import TimeoutError, ToolError

from dtest import Tester, create_ks, create_cf, mk_bman_path
from tools.assertions import assert_almost_equal, assert_all, assert_none
from tools.data import insert_c1c2, query_c1c2, insert_columns
from tools.jmxutils import JolokiaAgent, make_mbean
from tools.misc import new_node

from multiprocessing import Process
from random import randint
import json

from dtest import Tester, create_ks, create_cf, mk_bman_path

logger = logging.getLogger(__name__)

class TestPalantirTopology(Tester):

    def test_node_waiting_to_finish_bootstrap_receives_writes(self):
        """
        Test that when a node has finished streaming and is
        waiting to finish bootstrap, the node receives writes
        for data it now owns.
        @github_link https://github.com/palantir/cassandra/pull/402
        """

        cluster = self.cluster
        cluster.populate(3).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        node4 = new_node(cluster)
        node4.start()

        node4.watch_log_for('WAITING_TO_BOOTSTRAP')

        with JolokiaAgent(node4) as jmx:
            ss = make_mbean('db', type='StorageService')
            jmx.execute_method(ss, 'startBootstrap')

        node4.watch_log_for('Bootstrap almost complete')

        keys = [get_random_string() for _ in range(100)]

        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ALL)
        session.shutdown()

        for node in cluster.nodelist()[:3]:
            node.stop()

        with JolokiaAgent(node4) as jmx:
            ss = make_mbean('db', type='StorageService')
            jmx.execute_method(ss, 'finishBootstrap')
        
        node4.watch_log_for("Starting listening for CQL clients")

        session = self.patient_cql_connection(node4)
        session.execute('USE ks')

        successful_reads = 0
        for key in keys:
            if key_is_owned_by_node(key, 'ks', 'cf', node4):
                query_c1c2(session, key, consistency=ConsistencyLevel.ONE)
                successful_reads += 1
        assert successful_reads > 0

    def test_bootstrap_with_ip_change(self):
        """
        A node bounces and changes ip during bootstrap
        """

        cluster = self.cluster
        cluster.populate(3).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        node4 = new_node(cluster)
        node4.start()

        node4.watch_log_for('WAITING_TO_BOOTSTRAP')

        with JolokiaAgent(node4) as jmx:
            ss = make_mbean('db', type='StorageService')
            jmx.execute_method(ss, 'startBootstrap')

        stop_and_change_ip(node1, '124.0.0.9')
        node1.start()

        node4.watch_log_for('Bootstrap almost complete')

        with JolokiaAgent(node4) as jmx:
            ss = make_mbean('db', type='StorageService')
            jmx.execute_method(ss, 'finishBootstrap')
        
        node4.watch_log_for("Starting listening for CQL clients")

        session = self.patient_cql_connection(node4)
        session.execute('USE ks')

        successful_reads = 0
        for key in keys:
            if key_is_owned_by_node(key, 'ks', 'cf', node4):
                query_c1c2(session, key, consistency=ConsistencyLevel.ONE)
                successful_reads += 1
        assert successful_reads > 0

    def test_schema_change_ip_change(self):
        """
        CASSANDRA-15158 and followups.
        Test that if schema changes while a node restarts and changes ip,
        the schema is propagated.

        Specifically, consider a 2 node cluster.

        Node1 -> IPA
        Node2 -> IPB

        Sequence:
        - Node1 goes down
        - Node2 goes down
        - Node1 comes up with IPC
        - Node1 changes its schema
        - Node2 comes up with IPD

        Assert that Node2 pulls the schema from Node1, and encounters no errors on startup.
        Assert that a new node can bootstrap, and doesn't try to pull schema from the old IPs
        """
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        stop_and_change_ip(node1, '127.0.0.8')
        stop_and_change_ip(node2, '127.0.0.9')

        node1.start()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks2', 2)
        create_cf(session, 'cf2', columns={'c1': 'text', 'c2': 'text'})

        node2.start()

        time.sleep(10)

        node3 = new_node(cluster)
        node3.start()

        node3.watch_log_for('WAITING_TO_BOOTSTRAP')

        with JolokiaAgent(node3) as jmx:
            ss = make_mbean('db', type='StorageService')
            jmx.execute_method(ss, 'startBootstrap')
        
        node3.watch_log_for('Bootstrap almost complete')

        with JolokiaAgent(node4) as jmx:
            ss = make_mbean('db', type='StorageService')
            jmx.execute_method(ss, 'finishBootstrap')
        
        node3.watch_log_for("Starting listening for CQL clients")
        assert True


def stop_and_change_ip(node, new_ip):
    node.stop(gently=False)
    set_new_ip(node, new_ip)

def set_new_ip(node, new_address):
    node.set_configuration_options(values={
        'listen_address': new_address,
        'rpc_address': new_address,
    })
    for key, value in node.network_interfaces.items():
        if value is not None:
            address, port = value
            node.network_interfaces[key] = (new_address, port)

def get_random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def key_is_owned_by_node(key, ks, cf, node):
    out = node.nodetool(f'getendpoints {ks} {cf} k{key}')[0]
    owners = out.split()
    return node.address() in owners
