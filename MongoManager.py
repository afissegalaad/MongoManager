#!/usr/bin/env python

'''
    File name: MongoManager.py
    Author: Galaad
    Date created: 10/02/2016
    Date last modified: 10/05/2016
'''

__author__ = "Galaad"
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
    """
    Check if a port is open (listening) or not on localhost.

    It returns true if the port is actually listening, false
    otherwise.

    :param port: The port to check.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1',port))
    return result == 0

def warning(msg):
    """
    Print a warning message.
    
    Show to the user that something happened that does not affect the
    execution but is not usual.

    :param msg: The warning message.
    """
    print "[WARNING] " + msg

def error(msg, code=1):
    """
    Print an error message.
    
    To use when something bad happened that affects the rest of the
    execution. Then the execution is stopped and a user-defined error
    code is returned.

    :param msg: The error message.
    :param code: The error code.:
    """
    print "[ERROR] " + msg
    sys.exit(code)

def success(msg):
    """
    Print a success message.
    
    Use this fonction when some execution went well and you want to
    say the user that everything is fine.

    :param msg: The success message.
    """
    print "[SUCCESS] " + msg

def call(cmd):
    """
    Wrapper for Popen. Call a given command.
    
    It returns a tuple that contains the output string, the error
    strings and the return code.

    :param cmd: The command to launch.
    """
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    rc = p.returncode
    return output,err,rc

class Mongod:
    """
    Represents a mongod process.

    This class manages one mongod process. It can start, init, stop,
    and clean it.
    """
    count = 0
    def __init__(self,
                 ihostname="yoann",
                 iport=27000, 
                 itype="data",
                 ireplname="dataReplSet", 
                 idbpath="data/", 
                 ibaselogpath="log/",
                 ipidpath="pid/"):
        """
        Initialize the Mongod class. 

        It sets all the configuration but does not change anything on
        the machine. Does not create any directories.

        :param ihostname: The hostname of the machine of which the
        process is launched (currently localhost, but the hostname of
        localhost is not necessarly localhost).
        :param iport: The port on which the process will be started.
        :param itype: The type of node to start. Either "data" or "config".
        :param ireplname: The name of the replica set it belongs to.
        :param idbpath: The path to the directory that will contain
        the data. Can be relative or absolute.
        :param ibaselogpath: The path to the log file.
        :param ipidpath: The path to a file that contains the PID of
        the mongod process launched.
        """
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
        """
        Initialize the environment to prepare the mongod process to be
        launched. 

        Check if the port is open and creates all the directories.
        """
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
        """
        Start the mongod process.

        The PID is stored into a file..
        """
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
        """
        Kill the mongod process. Does not remove the data files.
        """
        pid = open(self.pidpath).read()
        output, err, rc = call(["kill", pid])
        if rc == 0:
            success("mongod process " + pid + " killed")
        else:
            error("mongod process " + pid + " not killed")
        #call(["mongo", "localhost:" + str(self.port) + "/admin", "--eval", "'db.shutdownServer()'"])

    def clean(self):
        """
        Clean all the directories attached to the mongod process.
        """
        output, err, rc = call(["rm", "-r", self.dbpath, self.baselogpath, self.basepidpath])
        if rc == 0:
            success("cleaned")
        else:
            error("not cleaned")

class Mongos:
    """
    Class representing a mongos process and manage it.
    """
    count = 0
    def __init__(self,
                 port=28000, 
                 configstring="", 
                 ibaselogpath="log/router/",
                 ipidpath="pid/router/"):
        """
        Constructor of th Mongos class.

        :param port: The port on which the mongos will be launched.
        :param configstring: The string containing the name of the
        replica config set with all the hostnames and port of the
        config nodes.
        :param ibaselogpath: The path to the log file.
        :param ipidpath: The path to the pid file.
        """
        self.port = port + self.__class__.count
        self.configstring = configstring
        self.baselogpath = ibaselogpath + str(self.__class__.count)
        self.logpath = self.baselogpath + "/router" + str(self.__class__.count) + ".log"
        self.basepidpath = ipidpath + str(self.__class__.count)
        self.pidpath = self.basepidpath + "/pid"
        self.__class__.count += 1

    def init(self):
        """
        Initialize the environment of the mongos process that will be runned
        with start() method.
        """
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
        """
        Start the mongos process.
        """
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
        """
        Stop the mongos process.
        """
        pid = open(self.pidpath).read()
        output, err, rc = call(["kill", pid])
        if rc == 0:
            success("mongos process " + pid + " killed")
        else:
            error("mongos process " + pid + " not killed")

    def clean(self):
        """
        Clean the environment of the mongos process.
        """
        output, err, rc = call(["rm", "-r", self.baselogpath, self.basepidpath])
        if rc == 0:
            success("cleaned")
        else:
            error("not cleaned")

class MongoReplicaSet:
    """
    Class representing a replica set.
    """
    count = 0
    def __init__(self,
                 type="data",
                 replname="data_set",
                 replica_factor=3):
        """
        Constructor of the MongoReplicaSet class.
        
        :param type: The type of replica set. Either "data" or
        "config".  :param replname: The name of the replica set.
        :param replica_factor: The number of node that the replica set
        contains. Must be an odd number.
        """
        self.replname = replname + str(self.__class__.count)
        self.replica_factor = replica_factor
        self.mongods = []
        for n in range(0, replica_factor):
            md = Mongod(ireplname=self.replname,itype=type)
            self.mongods.append(md)
        self.__class__.count += 1

    def init(self):
        """
        Initialize the environment of the replica set.
        """
        for md in self.mongods:
            md.init()
        success("replica set "+self.replname+" ready to be started")

    def start(self):
        """
        Start the replica set.
        """
        for md in self.mongods:
            md.start()
        success("replica set "+self.replname+" started")

    def init_replica(self):
        """
        Initialize the replica set. It connects the nodes together. After
        the execution of this method, the replica set is ready to be
        used.
        """
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
        """
        Stop the replica set. 
        """
        for md in self.mongods:
            md.stop()
        success("replica set "+self.replname+" stopped")
        
    def clean(self):
        """
        Clean the environment of the replica set.
        """
        for md in self.mongods:
            md.clean()
        success("replica set "+self.replname+" cleaned")

class MongoCluster:
    """
    The MongoCluster class reprensent a MongoDB cluster.
    
    It permits the manage a whole cluster with a few methods like
    init, start, stop and clean.
    """
    def __init__(self,
                 replica_factor=3,
                 scale_factor=2,
                 routers_factor=2):
        """
        Constructor of the MongoCluster class.
        
        :param replica_factor: The number of nodes that each replica
        set contains.
        :param scale_factor: The number of replica set (shards).
        :param routers_factor: The number of mongos.
        """
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
        """
        Initialize the environment of the cluster.
        """
        self.config_replica_set.init()
        for ds in self.data_replica_sets:
            ds.init()
        for ms in self.mongoss:
            ms.init()
        success("cluster is ready to be started")

    def start_replicas(self):
        """
        Start all the replica sets.
        """
        self.config_replica_set.start()
        for ds in self.data_replica_sets:
            ds.start()
        success("replica sets started")

    def init_replicas(self):
        """
        Initialize the replica sets.
        """
        self.config_replica_set.init_replica()
        for ds in self.data_replica_sets:
            ds.init_replica()
        success(str(self.scale_factor) + " data replica sets initialized")
        
    def init_cluster(self):
        """
        Initialize the cluster.
        """
        for ms in self.mongoss:
            ms.start()
        success("mongos started")
        success("cluster initialized")
        
    def stop(self):
        """
        Stop the cluster.
        """
        self.config_replica_set.stop()
        for ds in self.data_replica_sets:
            ds.stop()
        for ms in self.mongoss:
            ms.stop()
        success("cluster stopped")
        
    def clean(self):
        """
        Clean the cluster.
        """
        self.config_replica_set.clean()
        for ds in self.data_replica_sets:
            ds.clean()
        for ms in self.mongoss:
            ms.clean()
        success("cluster cleaned")
    
