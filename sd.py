from subprocess import call,list2cmdline, Popen, PIPE
import subprocess
import sys


def call_localhost(cmd):
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

def call_ssh(hostname, username, cmd):
    p = subprocess.Popen(["ssh", username + "@" + hostname, cmd],
                           stdin=PIPE,
                           stdout=PIPE,
                           stderr=PIPE)
    #result = ssh.stdout.readlines()
    output, err = p.communicate()
    rc = p.returncode    
    return output,err,rc

def call(hostname, username, cmd):
    if hostname == "localhost":
        return call_localhost(cmd)
    else:
        return call_ssh(hostname, username, " ".join(cmd))

cmd="mongod --configsvr --port 27000 --replSet config0 --dbpath data/config/0 --logpath log/config/0/config0.log --fork"
#cmd="ls"
output, err, rc = call("azimut","casket",cmd.split())
if rc == 0:
    print output
else:
    print err
