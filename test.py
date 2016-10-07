#!/usr/bin/env python

import sys
from MongoManager import MongoCluster

try:
    cluster = MongoCluster().initialize()
    sys.exit(0)
except:
    sys.exit(1)
