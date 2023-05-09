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


def get_random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def key_is_owned_by_node(key, ks, cf, node):
    out = node.nodetool(f'getendpoints {ks} {cf} k{key}')[0]
    owners = out.split()
    return node.address() in owners
