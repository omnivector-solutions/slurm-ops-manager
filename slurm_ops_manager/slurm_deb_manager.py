#!/usr/bin/env python3
"""This module provides the SlurmDebManager."""
import logging
import subprocess
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
        cmd = 'dpkg --status slurm-client | grep "^Version"'
        version = subprocess.check_output(cmd, shell=True)
        return version.decode().split(":")[-1].strip()

    @property
    def munge_version(self) -> str:
        """Return munge verion."""
        cmd = 'dpkg --status munge | grep "^Version"'
        version = subprocess.check_output(cmd, shell=True)
        return version.decode().split(":")[-1].strip()

    def _install_slurm_from_deb(self) -> bool:
        """Install Slurm debs.

        Returns True on success and False otherwise.
        """

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

        # setup munge
        subprocess.call(["apt-get", "install", "--yes", "munge"])
        subprocess.call(["systemctl", "enable", self._munged_systemd_service])

        try:
            # @todo: improve slurm version handling
            subprocess.check_output(["apt-get", "install", "--yes",
                                     slurm_component, "slurm-client"])
        except subprocess.CalledProcessError as e:
            logger.error(f"## Error installing {slurm_component} - {e}")
            return False

        subprocess.call(["apt-get", "autoremove", "--yes"])

        # we need to override the default service unit for slurmrestd only
        if "slurmrestd" == self._slurm_component:
            self.setup_slurmrestd_systemd_unit()

        return True

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

    def setup_slurm(self) -> bool:
        """Install Slurm and its dependencies."""
        successful_installation = self._install_slurm_from_deb()
        self._setup_paths()
        self.slurm_systemctl('enable')

        return successful_installation
