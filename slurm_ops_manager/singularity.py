"""Singularity Class."""

import logging
import shlex
import subprocess
from pathlib import Path

from ops.framework import Object, StoredState
from slurm_ops_manager.utils import operating_system


logger = logging.getLogger()


class Singularity(Object):
    """Responsible for Singularity operations."""

    _stored = StoredState()

    def __init__(self, parent, key):
        """Initialize class."""
        super().__init__(parent, key)

        self._stored.set_default(resource_name=str())

        self._operating_system = operating_system()

        # setup resource name based on operating system
        if self._operating_system == "ubuntu":
            self._stored.resource_name = "singularity-deb"
        elif self._operating_system == "centos":
            self._stored.resource_name = "singularity-rpm"
        else:
            logger.error(f"#### Unsupported OS: {self._operating_system}")
            self._stored.resource_name = ""

    @property
    def resource_name(self) -> str:
        """Return singularity resource name."""
        return self._stored.resource_name

    def install(self, resource_path: Path):
        """Install Singularity."""

        logger.debug(f"#### Installing singularity from: {resource_path}")

        cmds = {"ubuntu": f"apt-get install --yes {resource_path}",
                "centos": f"yum localinstall --assumeyes {resource_path}"}

        os_ = self._operating_system
        logger.debug(f"#### Installing on {self._operating_system}")
        logger.debug(f"#### Command: {cmds[os_]}")

        subprocess.run(shlex.split(cmds[os_]))

        logger.debug("#### Singularity successfully installed")
