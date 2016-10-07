#!/usr/bin/env python

import sys
from MongoManager import MongoCluster

cluster = MongoCluster().initialize()
sys.exit(0)

