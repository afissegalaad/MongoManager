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
sys.path.append("..")

parser = argparse.ArgumentParser(description='MongoDB cluster manager')
parser.add_argument('--init_cluster_env', dest='init_cluster_env', action='store_true', default=False, help='Initiliaze the environment of the cluster')
parser.add_argument('--start_cluster', dest='start_cluster', action='store_true', default=False, help='Start the cluster')
parser.add_argument('--init_cluster', dest='init_cluster', action='store_true', default=False, help='Initialize the cluster')
parser.add_argument('--stop_cluster', dest='stop_cluster', action='store_true', default=False, help='Stop the cluster')
parser.add_argument('--clean_cluster_env', dest='clean_cluster_env', action='store_true', default=False, help='Clean the environment cluster')
args = vars(parser.parse_args())
init_cluster_env = args["init_cluster_env"]
start_cluster = args["start_cluster"]
init_cluster = args["init_cluster"]
stop_cluster = args["stop_cluster"]
clean_cluster_env = args["clean_cluster_env"]

cluster = MongoCluster()

if init_cluster_env:
    cluster.init()
elif start_cluster:
    cluster.start_replicas()
elif init_cluster:
    cluster.init_replicas()
    cluster.init_cluster()
elif stop_cluster:
    cluster.stop()
elif clean_cluster_env:
    cluster.clean()
