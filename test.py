#!/usr/bin/env python

import sys
from MongoManager import MongoCluster

try:
    cluster = MongoCluster().initialize().start().stop().clean()
    sys.exit(0)
except:
    sys.exit(1)
