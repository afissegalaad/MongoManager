#!/usr/bin/env python

'''
    File name: test.py
    Author: Galaad Couillec
    Date created: 10/02/2016
    Date last modified: 10/03/2016
'''

__author__ = "Galaad Couillec"
__version__ = "1.0.0"
__status__ = "alpha"

from MongoManager import Mongod, MongoReplicaSet, MongoCluster
import argparse
import sys

parser = argparse.ArgumentParser(description='MongoDB cluster manager')
parser.add_argument('--init', dest='init', action='store_true', default=False, help='init')
parser.add_argument('--start_replicas', dest='start_replicas', action='store_true', default=False, help='start replicas')
parser.add_argument('--init_replicas', dest='init_replicas', action='store_true', default=False, help='init replicas')
parser.add_argument('--init_cluster', dest='init_cluster', action='store_true', default=False, help='init cluster')
parser.add_argument('--stop', dest='stop', action='store_true', default=False, help='stop')
parser.add_argument('--clean', dest='clean', action='store_true', default=False, help='clean')
args = vars(parser.parse_args())
init = args["init"]
start_replicas = args["start_replicas"]
init_replicas = args["init_replicas"]
init_cluster = args["init_cluster"]
stop = args["stop"]
clean = args["clean"]

cluster = MongoCluster()

if init:
    cluster.init()
elif start_replicas:
    cluster.start_replicas()
elif init_replicas:
    cluster.init_replicas()
elif init_cluster:
    cluster.init_cluster()
elif stop:
    cluster.stop()
elif clean:
    cluster.clean()
