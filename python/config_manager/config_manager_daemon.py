#!/usr/bin/env python3
#
# This script opens a UNIX socket for the DomainRadar config_manager service. It listens for up/down commands
# from the config_manager on this socket and executes "docker compose -f compose.yml (up|down) [service]".
#
# The first positional argument (optional) sets the path to the socket.
#   Default: ./domrad_control.sock
# The second positional argument (optional) sets the base Docker Compose command.
#   Default: docker compose -f compose.yml
#

import os
import socket
import subprocess
import sys


def _down(component_id: str) -> int:
    print(f"{compose_cmd} down {component_id}")
    return subprocess.Popen(f"{compose_cmd} down {component_id}", shell=True).wait()


def _up(component_id: str) -> int:
    print(f"{compose_cmd} up -d {component_id}")
    return subprocess.Popen(f"{compose_cmd} up -d {component_id}", shell=True).wait()


# Set the path for the Unix socket
socket_path = sys.argv[1] if len(sys.argv) > 1 and len(sys.argv[1]) > 0 else './domrad_control.sock'
compose_cmd = sys.argv[2] if len(sys.argv) > 2 else 'docker compose -f compose.yml'

# remove the socket file if it already exists
try:
    os.unlink(socket_path)
except OSError:
    if os.path.exists(socket_path):
        raise

# Create the Unix socket server
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

# Bind the socket to the path
sock.bind(socket_path)

# Listen for incoming connections
sock.listen(1)

# accept connections
print(f'The config_manager daemon is listening on {socket_path}')

while True:
    try:
        connection, client_address = sock.accept()
        print('Connection from', str(connection).split(", ")[0][-4:])

        # receive data from the client
        while True:
            data = connection.recv(1024)
            if not data:
                print("Closing connection")
                break

            op = data[0]
            data_str = data[1:].split(b'\x00', maxsplit=1)[0].decode("ascii")

            if op == 1:
                ret = _up(data_str)
            elif op == 2:
                ret = _down(data_str)
            else:
                print(f"Invalid operation {op}: {data.decode('ascii')}")
                ret = -1

            # Send the return code back to the client
            response = ret.to_bytes(4, byteorder='big', signed=True)
            connection.sendall(response)

        # close the connection
        connection.close()
    except KeyboardInterrupt:
        break
    except Exception as e:
        print("Error: " + str(e))

sock.close()
os.unlink(socket_path)
print('Exiting...')
