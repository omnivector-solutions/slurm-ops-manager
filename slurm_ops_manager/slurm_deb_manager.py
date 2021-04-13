#!/usr/bin/env python3
"""This module provides the SlurmDebManager."""
import logging
import subprocess
import sys
from pathlib import Path

from slurm_ops_manager.slurm_ops_base import SlurmOpsManagerBase


logger = logging.getLogger()


class SlurmDebManager(SlurmOpsManagerBase):
    """Slurm debian operations manager."""

    def __init__(self, component):
        """Set initial attribute values."""
        super().__init__(component)

    @property
    def _slurm_plugin_dir(self) -> Path:
        return Path("/usr/lib/x86_64-linux-gnu/slurm-wlm/")

    @property
    def _mail_prog(self) -> Path:
        return Path("/usr/bin/mail.mailutils")

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        # from Debian HPC Team
        return "20.11.4-1"

    def _install_slurm_from_deb(self):
        """Install Slurm debs"""

        slurm_component = self._slurm_component

        with open("/etc/apt/sources.list.d/bullseye.list", "w") as afile:
            afile.write("deb http://deb.debian.org/debian bullseye main")

        subprocess.call(["apt-get", "install", "--yes", "debian-keyring"])
        subprocess.call(["apt-key", "adv", "--keyserver",
                          "keyserver.ubuntu.com", "--recv-keys",
                          "04EE7237B7D453EC", "648ACFD622F3D138"])
        subprocess.call(["apt-get", "update"])

        # update specific needed dependencies
        subprocess.call(["apt-get", "install", "--yes", "libgcrypt20"])
        subprocess.call(["apt-get", "install", "--yes", "mailutils"])
        subprocess.call(["apt-get", "install", "--yes", "logrotate"])

        # pin munge vesion
        subprocess.call(["apt-get", "install", "--yes", "munge=0.5.14-4"])

        try:
            # @todo: improve slurm version handling
            subprocess.call(["apt-get", "install", "--yes",
                             slurm_component + "=" + self.slurm_version,
                             "slurm-client=" + self.slurm_version])
        except subprocess.CalledProcessError as e:
            print(f"Error installing {slurm_component} - {e}")
            # @todo: set appropriate juju status
            return -1

        subprocess.call(["apt-get", "autoremove", "--yes"])

    def _setup_paths(self):
        """Create needed paths with correct permisions."""

        if "slurmd" == self._slurm_component:
            user = f"{self._slurmd_user}:{self._slurmd_group}"
        else:
            user = f"{self._slurm_user}:{self._slurm_group}"

        all_paths = [self._slurm_conf_dir,
                     self._slurm_state_dir,
                     self._slurm_spool_dir]

        for syspath in all_paths:
            if not syspath.exists():
                syspath.mkdir()
            subprocess.call(["chown", "-R", user, syspath])

    def upgrade(self, channel):
        """Run upgrade operations."""
        pass

    def setup_system(self) -> None:
        """Install the slurm deb."""
        self._install_slurm_from_deb()
        self._setup_paths()
