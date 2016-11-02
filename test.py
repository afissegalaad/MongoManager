#!/usr/bin/env python

import sys
from MongoManager import MongoCluster

cluster = MongoCluster().clean()
sys.exit(0)

