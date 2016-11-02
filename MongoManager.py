#!/usr/bin/env python

'''
    File name: MongoManager.py
    Author: Galaad
    Date created: 10/02/2016
    Date last modified: 10/06/2016
'''

__author__ = "Galaad"
__version__ = "0.1.0"

from os import makedirs
from os.path import isdir
from subprocess import call,list2cmdline, Popen, PIPE
from uuid import uuid1
from time import sleep
from itertools import count
import socket
import sys
import json
from datetime import datetime
import paramiko 
import subprocess

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
    print "[WARNING:" + str(datetime.now()) + "] " + msg

def error(msg, code=1):
    """
    Print an error message.
    
    To use when something bad happened that affects the rest of the
    execution. Then the execution is stopped and a user-defined error
    code is returned.

    :param msg: The error message.
    :param code: The error code.:
    """
    print "[FAILURE:" + str(datetime.now()) + "] " + msg
    raise Exception(str(code) + ":" + msg)

def success(msg):
    """
    Print a success message.
    
    Use this fonction when some execution went well and you want to
    say the user that everything is fine.

    :param msg: The success message.
    """
    print "[SUCCESS:" + str(datetime.now()) + "] " + msg

def call_localhost(cmd):
    """
    Wrapper for Popen. Call a given command.
    
    It returns a tuple that contains the output string, the error
    strings and the return code.

    :param cmd: The command to launch.
    """
    #print " ".join(cmd)
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    rc = p.returncode
    return output,err,rc

def call_ssh(hostname, username, cmd):
    #cmd2 = ["ssh", username + "@" + hostname, "'nohup " + cmd + "> /dev/null 2>&1 &'"]
    cmd2 = ["ssh", username + "@" + hostname, cmd]
    #print " ".join(cmd2)
    p = subprocess.Popen(cmd2,
                         stdin=PIPE,
                         stdout=PIPE,
                         stderr=PIPE)
    output, err = p.communicate()
    rc = p.returncode
    return output,err,rc

def call(hostname, username, cmd):
    if hostname == socket.gethostname():
        return call_localhost(cmd)
    else:
        return call_ssh(hostname, username, " ".join(cmd))

class Mongod:
    """
    Represents a mongod process.

    This class manages one mongod process. It can start, init, stop,
    and clean it.
    """
    count = 0
    def __init__(self,
                 ihostname="yoann",
                 iusername="couillec",
                 iport=27000, 
                 itype="data",
                 ireplname="dataReplSet", 
                 idbpath="data/", 
                 ibaselogpath="log/",
                 ipidpath="pid/"):
        """
        Constructor of the Mongod class. 

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
        self.username = iusername
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

    def initialize(self):
        """
        Initialize the environment to prepare the mongod process to be
        launched. 

        Check if the port is open and creates all the directories.
        """
        if is_open_port(self.port):
            error ("Port "+str(self.port)+" is already used")
        call(self.hostname,self.username,["mkdir", "-p", self.dbpath])
        call(self.hostname,self.username,["mkdir", "-p", self.baselogpath])
        call(self.hostname,self.username,["mkdir", "-p", self.basepidpath])
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
        output, err, rc = call(self.hostname, self.username, cmd)
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
        output, err, rc = call(self.hostname, self.username,["kill", pid])
        if rc == 0:
            success("mongod process " + pid + " killed")
        else:
            warning("mongod process " + pid + " not killed")

    def clean(self):
        """
        Clean all the directories attached to the mongod process.
        """
        output, err, rc = call(self.hostname, self.username,["rm", "-r", self.dbpath, self.baselogpath, self.basepidpath])
        if rc == 0:
            success("cleaned")
        else:
            warning("not cleaned")

class Mongos:
    """
    Class representing a mongos process and manage it.
    """
    count = 0
    def __init__(self,
                 hostname="yoann",
                 username="couillec",
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
        self.hostname = hostname
        self.username = username
        self.port = port + self.__class__.count
        self.configstring = configstring
        self.baselogpath = ibaselogpath + str(self.__class__.count)
        self.logpath = self.baselogpath + "/router" + str(self.__class__.count) + ".log"
        self.basepidpath = ipidpath + str(self.__class__.count)
        self.pidpath = self.basepidpath + "/pid"
        self.__class__.count += 1

    def initialize(self):
        """
        Initialize the environment of the mongos process that will be runned
        with start() method.
        """
        if is_open_port(self.port):
            error ("Port "+str(self.port)+" is already used")
        call(self.hostname, self.username, ["mkdir", "-p", self.baselogpath])
        call(self.hostname, self.username,["mkdir", "-p", self.basepidpath])
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
        output, err, rc = call(self.hostname, self.username,cmd)
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
        output, err, rc = call(self.hostname, self.username,["kill", pid])
        if rc == 0:
            success("mongos process " + pid + " killed")
        else:
            warning("mongos process " + pid + " not killed")

    def clean(self):
        """
        Clean the environment of the mongos process.
        """
        output, err, rc = call(self.hostname, self.username,["rm", "-r", self.baselogpath, self.basepidpath])
        if rc == 0:
            success("cleaned")
        else:
            warning("not cleaned")

class MongoReplicaSet:
    """
    Class representing a replica set.
    """
    count = 0
    def __init__(self,
                 hostname="yoann",
                 username="couillec",
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
        self.hostname = hostname
        self.username = username
        self.replname = replname + str(self.__class__.count)
        self.replica_factor = replica_factor
        self.mongods = []
        for n in range(0, replica_factor):
            md = Mongod(ireplname=self.replname,
                        itype=type,
                        ihostname=hostname,
                        iusername=username)
            self.mongods.append(md)
        self.__class__.count += 1

    def initialize(self):
        """
        Initialize the environment of the replica set.
        """
        for md in self.mongods:
            md.initialize()
        success("replica set "+self.replname+" ready to be started")

    def start(self):
        """
        Start the replica set.
        """
        for md in self.mongods:
            md.start()
        success("replica set "+self.replname+" started")

    def initiate(self):
        """
        Initialize the replica set. It connects the nodes together. After
        the execution of this method, the replica set is ready to be
        used.
        """
        primary = self.mongods[0]
        cmd = ["mongo", "--port", str(primary.port), "--eval", "rs.initiate()"]
        output, err, rc = call(self.hostname, self.username,cmd)
        json_ret = json.loads(" ".join(output.split("\n")[2:]))
        if json_ret["ok"] == 1:
            success("replica set initiated")
        else:
            error(json_ret["errmsg"])
        for secondary in self.mongods[1:]:
            cmd = ["mongo", "--port", str(primary.port), 
                   "--eval", "rs.add('" + secondary.hostname + ":" + str(secondary.port) + "')"]
            output, err, rc = call(self.hostname, self.username,cmd)
            json_ret = json.loads(" ".join(output.split("\n")[2:]))
            if json_ret["ok"] == 1:
                success("node added to replica set")
            else:
                error(json_ret["errmsg"])

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
                 hostname="yoann",
                 username="couillec",
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
        self.hostname = hostname
        self.username = username
        self.replica_factor = replica_factor
        self.scale_factor = scale_factor
        self.config_replica_set = MongoReplicaSet(type="config",
                                                  replname="config",
                                                  hostname=self.hostname,
                                                  username=self.username)
        self.data_replica_sets = []
        for n in range(0, scale_factor):
            ds = MongoReplicaSet(replica_factor=replica_factor,
                                 hostname=hostname,
                                 username=username)
            self.data_replica_sets.append(ds)
        pc = self.config_replica_set.mongods[0]
        configstring = self.config_replica_set.replname + "/" + pc.hostname + ":"+ str(pc.port)
        for md in self.config_replica_set.mongods[1:]:
            configstring += "," + md.hostname + ":" + str(md.port)
        self.mongoss = []
        for n in range(0, routers_factor):
            ms = Mongos(configstring=configstring,
                        hostname=self.hostname,
                        username=self.username)
            self.mongoss.append(ms)

    def initialize(self):
        """
        Initialize the environment of the cluster.
        """
        self.config_replica_set.initialize()
        for ds in self.data_replica_sets:
            ds.initialize()
        for ms in self.mongoss:
            ms.initialize()
        success("cluster initialized")
        return self

    def start(self):
        """
        Start the cluster..
        """
        self.config_replica_set.start()
        for ds in self.data_replica_sets:
            ds.start()
        self.config_replica_set.initiate()
        for ds in self.data_replica_sets:
            ds.initiate()
        success(str(self.scale_factor) + " data replica sets initiated")
        for ms in self.mongoss:
            ms.start()
        success(str(len(self.mongoss)) + " mongos started")
        success("cluster started")
        return self

    def restart(self):
        """
        Restart the cluster.
        """
        self.config_replica_set.start()
        for ds in self.data_replica_sets:
            ds.start()
        for ms in self.mongoss:
            ms.start()
        success(str(len(self.mongoss)) + " mongos restarted")
        success("cluster restarted")
        return self

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
        return self
        
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
        return self
    
