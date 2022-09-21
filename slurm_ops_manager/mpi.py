"""This MPI Class is responsible for installing MPI (mpich) in the slurmd charms."""

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

        self._stored.set_default(mpi_installed=False)

        self._operating_system = operating_system()

    @property
    def installed(self) -> bool:
        """Return wether mpich is installed."""
        return self._stored.mpi_installed

    def install(self):
        """Install mpich using the package managers apt-get (Ubuntu) or yum (CentOS)."""

        logger.debug("#### Installing MPI (mpich)")

        cmds = {"ubuntu": "apt-get install --yes mpich=3.3.2-2build1",
                "centos": "yum install --assumeyes mpich-3.2 mpich-3.2-devel"}

        os_ = self._operating_system

        logger.debug(f"#### Installing on {os_}")
        logger.debug(f"#### Command: {cmds[os_]}")

        try:
            subprocess.check_output(shlex.split(cmds[os_]))

            # In centos MPI is installed as a module
            # So, we must load the MPI module
            if os_ == "centos":
                logger.debug("#### Configuring /etc/bashrc to load MPI module")

                cmd = "module load mpi/mpich-3.2-x86_64\n"

                with open("/etc/bashrc", "a") as bashrc:
                    bashrc.write(cmd)

            self._stored.mpi_installed = True
            logger.debug("#### MPI successfully installed")
        except subprocess.CalledProcessError as e:
            logger.error(f"#### Error installing MPI - {e}")
