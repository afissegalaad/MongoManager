#!/usr/bin/env python

'''
    File name: example2.py
    Author: Galaad Couillec
    Date created: 10/06/2016
    Date last modified: 10/06/2016
'''

from MongoManager import MongoCluster

cluster = MongoCluster().initialize().start()
