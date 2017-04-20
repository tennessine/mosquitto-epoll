#!/usr/bin/env python

# Client connects without a certificate to a server that has use_identity_as_username=true. Should be rejected.
import subprocess
import socket
import ssl
import sys
import time

if sys.version < '2.7':
    print("WARNING: SSL not supported on Python 2.6")
    exit(0)

import inspect, os, sys
# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test

rc = 1
keepalive = 10
connect_packet = mosq_test.gen_connect("connect-no-identity-test", keepalive=keepalive)
connack_packet = mosq_test.gen_connack(rc=4)

broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=1889)

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssock = ssl.wrap_socket(sock, ca_certs="../ssl/test-root-ca.crt", cert_reqs=ssl.CERT_REQUIRED)
    ssock.settimeout(20)
    ssock.connect(("localhost", 1888))
    ssock.send(connect_packet)

    if mosq_test.expect_packet(ssock, "connack", connack_packet):
        rc = 0

    ssock.close()
finally:
    time.sleep(2)
    broker.terminate()
    broker.wait()
    if rc:
        (stdo, stde) = broker.communicate()
        print(stde)

exit(rc)

