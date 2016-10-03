#!/usr/bin/env python

'''
    File name: MongoManager.py
    Author: Galaad Couillec
    Date created: 10/02/2016
    Date last modified: 10/03/2016
'''

__author__ = "Galaad Couillec"
__version__ = "1.0.0"
__status__ = "alpha"

from os import makedirs
from os.path import isdir
from subprocess import call,list2cmdline, Popen, PIPE
from uuid import uuid1
from time import sleep
from itertools import count
import socket
import sys
import json

def is_open_port(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1',port))
    return result == 0

def warning(msg):
    print "[WARNING] " + msg

def error(msg, code=1):
    print "[ERROR] " + msg
    sys.exit(code)

def success(msg):
    print "[SUCCESS] " + msg

def call(cmd):
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    rc = p.returncode
    return output,err,rc

class Mongod:
    count = 0
    def __init__(self,
                 ihostname="yoann",
                 iport=27000, 
                 itype="data",
                 ireplname="dataReplSet", 
                 idbpath="data/", 
                 ibaselogpath="log/",
                 ipidpath="pid/"):
        self.hostname = ihostname
        self.port = iport + self.__class__.count
        self.type = ""
        self.dbpath = ""
        self.baselogpath = ""
        self.basepidpath = ""
        if itype == "config":
            self.type = "--configsvr"
            self.dbpath = idbpath + "config/" + str(self.__class__.count)
            self.baselogpath = ibaselogpath + "config/" + str(self.__class__.count)
            self.basepidpath = ipidpath + "config/" + str(self.__class__.count)
        elif itype == "data":
            self.type = "--shardsvr"
            self.dbpath = idbpath + "data/" + str(self.__class__.count)
            self.baselogpath = ibaselogpath + "data/" + str(self.__class__.count)
            self.basepidpath = ipidpath + "data/" + str(self.__class__.count)
        self.replname = ireplname
        self.logpath = self.baselogpath + "/config" + str(self.__class__.count) + ".log"
        self.pidpath = self.basepidpath + "/pid"
        self.__class__.count += 1

    def init(self):
        if is_open_port(self.port):
            error ("Port "+str(self.port)+" is already used")
        if not isdir(self.dbpath):
            call(["mkdir", "-p", self.dbpath])
        else:
            msg = self.dbpath + " already exists"
            warning(msg)
        if not isdir(self.baselogpath):
            call(["mkdir", "-p", self.baselogpath])
        else:
            msg = self.baselogpath + " already exists"
            warning(msg)
        if not isdir(self.basepidpath):
            call(["mkdir", "-p", self.basepidpath])
        else:
            msg = self.basepidpath + " already exists"
            warning(msg)
        success("mongod is ready to be launched")

    def start(self):
        cmd = ["mongod", 
               self.type,
               "--port", str(self.port), 
               "--replSet", self.replname, 
               "--dbpath", self.dbpath, 
               "--logpath", self.logpath, 
               "--fork"]
        output, err, rc = call(cmd)
        pid = int(output.split("\n")[1].split()[2])
        f = open (self.pidpath,"w")
        f.write(str(pid))
        f.close()
        if rc == 0:
            success("mongod started on port " + str(self.port) + " with pid " + str(pid))
        else:
            error("mongod not started on port " + str(self.port))
        return rc

    def stop(self):
        pid = open(self.pidpath).read()
        output, err, rc = call(["kill", pid])
        if rc == 0:
            success("mongod process " + pid + " killed")
        else:
            error("mongod process " + pid + " not killed")
        #call(["mongo", "localhost:" + str(self.port) + "/admin", "--eval", "'db.shutdownServer()'"])

    def clean(self):
        output, err, rc = call(["rm", "-r", self.dbpath, self.baselogpath, self.basepidpath])
        if rc == 0:
            success("cleaned")
        else:
            error("not cleaned")

class Mongos:
    count = 0
    def __init__(self,
                 port=28000, 
                 configstring="", 
                 ibaselogpath="log/router/",
                 ipidpath="pid/router/"):
        self.port = port + self.__class__.count
        self.configstring = configstring
        self.baselogpath = ibaselogpath + str(self.__class__.count)
        self.logpath = self.baselogpath + "/router" + str(self.__class__.count) + ".log"
        self.basepidpath = ipidpath + str(self.__class__.count)
        self.pidpath = self.basepidpath + "/pid"
        self.__class__.count += 1

    def init(self):
        if is_open_port(self.port):
            error ("Port "+str(self.port)+" is already used")
        if not isdir(self.baselogpath):
            call(["mkdir", "-p", self.baselogpath])
        else:
            msg = self.baselogpath + " already exists"
            warning(msg)
        if not isdir(self.basepidpath):
            call(["mkdir", "-p", self.basepidpath])
        else:
            msg = self.basepidpath + " already exists"
            warning(msg)
        success("mongos is ready to be started")

    def start(self):
        cmd = ["mongos", 
               "--configdb", self.configstring,
               "--port", str(self.port), 
               "--logpath", self.logpath, 
               "--fork"]
        output, err, rc = call(cmd)
        pid = int(output.split("\n")[1].split()[2])
        f = open (self.pidpath,"w")
        f.write(str(pid))
        f.close()
        if rc == 0:
            success("mongos started on port " + str(self.port) + " with pid " + str(pid))
        else:
            error("mongos not started on port " + str(self.port))
        return rc

    def stop(self):
        pid = open(self.pidpath).read()
        output, err, rc = call(["kill", pid])
        if rc == 0:
            success("mongos process " + pid + " killed")
        else:
            error("mongos process " + pid + " not killed")

    def clean(self):
        output, err, rc = call(["rm", "-r", self.baselogpath, self.basepidpath])
        if rc == 0:
            success("cleaned")
        else:
            error("not cleaned")

class MongoReplicaSet:
    count = 0
    def __init__(self,
                 type="data",
                 replname="data_set",
                 replica_factor=3):
        self.replname = replname + str(self.__class__.count)
        self.replica_factor = replica_factor
        self.mongods = []
        for n in range(0, replica_factor):
            md = Mongod(ireplname=self.replname,itype=type)
            self.mongods.append(md)
        self.__class__.count += 1

    def init(self):
        for md in self.mongods:
            md.init()
        success("replica set "+self.replname+" ready to be started")

    def start(self):
        for md in self.mongods:
            md.start()
        success("replica set "+self.replname+" started")

    def init_replica(self):
        primary = self.mongods[0]
        cmd = ["mongo", "--port", str(primary.port), "--eval", "rs.initiate()"]
        output, err, rc = call(cmd)
        json_ret = json.loads(" ".join(output.split("\n")[2:]))
        if json_ret["ok"] == 1:
            success("replica set initialized")
        else:
            error(json_ret["errmsg"])
        for secondary in self.mongods[1:]:
            cmd = ["mongo", "--port", str(primary.port), 
                   "--eval", "rs.add('" + secondary.hostname + ":" + str(secondary.port) + "')"]
            output, err, rc = call(cmd)
            json_ret = json.loads(" ".join(output.split("\n")[2:]))
            if json_ret["ok"] == 1:
                success("node added to replica set")
            else:
                error(json_ret["errmsg"])
        success("replica set "+self.replname+" initialized")

    def stop(self):
        for md in self.mongods:
            md.stop()
        success("replica set "+self.replname+" stopped")
        
    def clean(self):
        for md in self.mongods:
            md.clean()
        success("replica set "+self.replname+" cleaned")

class MongoCluster:
    def __init__(self,
                 replica_factor=3,
                 scale_factor=2,
                 routers_factor=2):
        self.replica_factor = replica_factor
        self.scale_factor = scale_factor
        self.config_replica_set = MongoReplicaSet(type="config",replname="config")
        self.data_replica_sets = []
        for n in range(0, scale_factor):
            ds = MongoReplicaSet(replica_factor=replica_factor)
            self.data_replica_sets.append(ds)
        pc = self.config_replica_set.mongods[0]
        configstring = self.config_replica_set.replname + "/" + pc.hostname + ":"+ str(pc.port)
        for md in self.config_replica_set.mongods[1:]:
            configstring += "," + md.hostname + ":" + str(md.port)
        self.mongoss = []
        for n in range(0, routers_factor):
            ms = Mongos(configstring=configstring)
            self.mongoss.append(ms)

    def init(self):
        self.config_replica_set.init()
        for ds in self.data_replica_sets:
            ds.init()
        for ms in self.mongoss:
            ms.init()
        success("cluster is ready to be started")

    def start_replicas(self):
        self.config_replica_set.start()
        for ds in self.data_replica_sets:
            ds.start()
        success("replica sets started")

    def init_replicas(self):
        self.config_replica_set.init_replica()
        for ds in self.data_replica_sets:
            ds.init_replica()
        success(str(self.scale_factor) + " data replica sets initialized")
        
    def init_cluster(self):
        for ms in self.mongoss:
            ms.start()
        success("mongos started")
        success("cluster initialized")
        
    def stop(self):
        self.config_replica_set.stop()
        for ds in self.data_replica_sets:
            ds.stop()
        for ms in self.mongoss:
            ms.stop()
        success("cluster stopped")
        
    def clean(self):
        self.config_replica_set.clean()
        for ds in self.data_replica_sets:
            ds.clean()
        for ms in self.mongoss:
            ms.clean()
        success("cluster cleaned")
    
