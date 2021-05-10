#!/usr/bin/env python3
"""slurm-ops-manager utils."""
import os
import re
import socket
import subprocess
import sys

from pathlib import Path


OS_RELEASE = Path("/etc/os-release").read_text().split("\n")
OS_RELEASE_CTXT = {
    k: v.strip("\"")
    for k, v in [item.split("=") for item in OS_RELEASE if item != '']
}


def operating_system():
    """Return what operating system we are running."""
    return OS_RELEASE_CTXT['ID']


def _get_real_mem():
    """Return the real memory."""
    try:
        real_mem = subprocess.check_output(
            "free -m | grep -oP '\\d+' | head -n 1",
            shell=True
        )
    except subprocess.CalledProcessError as e:
        # logger.debug(e)
        print(e)
        sys.exit(-1)

    return real_mem.decode().strip()


def _get_cpu_info():
    """Return the socket info."""
    try:
        lscpu = \
            subprocess.check_output(
                "lscpu",
                shell=True
            ).decode().replace("(s)", "")
    except subprocess.CalledProcessError as e:
        print(e)
        sys.exit(-1)

    cpu_info = {
        'CPU:': '',
        'Thread per core:': '',
        'Core per socket:': '',
        'Socket:': '',
    }

    try:
        for key in cpu_info:
            cpu_info[key] = re.search(f"{key}.*", lscpu)\
                              .group()\
                              .replace(f"{key}", "")\
                              .replace(" ", "")
    except Exception as error:
        print(f"Unable to set Node configuration: {error}")
        sys.exit(-1)

    return f"CPUs={cpu_info['CPU:']} "\
           f"ThreadsPerCore={cpu_info['Thread per core:']} "\
           f"CoresPerSocket={cpu_info['Core per socket:']} "\
           f"SocketsPerBoard={cpu_info['Socket:']}"


# Get the number of GPUs and check that they exist at /dev/nvidiaX
def _get_gpus():
    gpu = int(
        subprocess.check_output(
            "lspci | grep -i nvidia | awk '{print $1}' "
            "| cut -d : -f 1 | sort -u | wc -l",
            shell=True
        )
    )

    for i in range(gpu):
        gpu_path = "/dev/nvidia" + str(i)
        if not os.path.exists(gpu_path):
            return 0
    return gpu


def get_hostname():
    """Return the hostname."""
    return socket.gethostname().split(".")[0]


def get_inventory():
    """Assemble and return the node info."""
    hostname = get_hostname()
    mem = _get_real_mem()
    cpu_info = _get_cpu_info()
    gpus = _get_gpus()

    node_info = f"NodeName={hostname} "\
                f"NodeAddr={hostname} "\
                f"State=UNKNOWN "\
                f"{cpu_info} "\
                f"RealMemory={mem}"
    if (gpus > 0):
        node_info = node_info + f" Gres={gpus}"

    return node_info
