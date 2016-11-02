#!/usr/bin/env python

'''
    File name: example1.py
    Author: Galaad Couillec
    Date created: 10/02/2016
    Date last modified: 10/06/2016
'''

from MongoManager import Mongod, MongoReplicaSet, MongoCluster
import argparse
import sys

parser = argparse.ArgumentParser(description='MongoDB cluster manager')

parser.add_argument('--initialize', dest='initialize', action='store_true', default=False, help='Initialize the environment of the cluster')
parser.add_argument('--start', dest='start', action='store_true', default=False, help='Start the cluster')
parser.add_argument('--restart', dest='restart', action='store_true', default=False, help='Restart the cluster')
parser.add_argument('--stop', dest='stop', action='store_true', default=False, help='Stop the cluster')
parser.add_argument('--clean', dest='clean', action='store_true', default=False, help='Clean the environment of the cluster')

args = vars(parser.parse_args())
initialize = args["initialize"]
start = args["start"]
restart = args["restart"]
stop = args["stop"]
clean = args["clean"]

cluster = MongoCluster(hostname="azimut",username="casket")

if start:
    cluster.initialize()
    cluster.start()
elif restart:
    cluster.restart()
elif stop:
    cluster.stop()
elif clean:
    cluster.clean()
