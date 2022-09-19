"""MPI Class."""

import logging
import shlex
import subprocess

from ops.framework import Object, StoredState
from slurm_ops_manager.utils import operating_system


logger = logging.getLogger()


class MPI(Object):
    """Responsible for MPI operations."""

    _stored = StoredState()

    def __init__(self, parent, key):
        """Initialize class."""
        super().__init__(parent, key)

        self._operating_system = operating_system()

    def install(self):
        """Install MPI (mpich)."""

        logger.debug("#### Installing MPI (mpich)")

        cmds = {"ubuntu": "apt-get install --yes mpich",
                "centos": "yum install --assumeyes mpich-3.2 mpich-3.2-devel"}

        os_ = self._operating_system
        logger.debug(f"#### Installing on {self._operating_system}")
        logger.debug(f"#### Command: {cmds[os_]}")

        subprocess.run(shlex.split(cmds[os_]))

        # In centos MPI is installed as a module
        # So, we must load the MPI module
        if self._operating_system == "centos":
            logger.debug("#### Configuring /etc/bashrc to load MPI module")

            cmd = "module load mpi/mpich-3.2-x86_64"

            with open("/etc/bashrc", "a") as bashrc:
                bashrc.write(cmd)

        logger.debug("#### MPI successfully installed")
