#!/usr/bin/env python3
"""This module provides the SlurmDebManager."""
import logging
import shlex
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
        # Debian packages slurm plugins in /usr/lib/x86_64-linux-gnu/slurm-wlm/
        # but we symlink /usr/lib64/slurm to it for compatibility with centos
        return Path("/usr/lib64/slurm/")

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

    def _setup_ppas(self, custom_ppa: str) -> bool:
        """Set up a custom repository to install Slurm.

        Args:
            custom_ppa: a string in the format "ppa:user/ppa-name" or the URL
                        for the repository to install Slurm from.
        Returns:
            bool: whether the operations was successfull.
        """
        if custom_ppa:
            ppa = custom_ppa
        else:
            ppa = "ppa:omnivector/osd"

        logger.debug(f"## Adding ppa {ppa}.")
        try:
            cmd = f'add-apt-repository --yes --update "{ppa}"'
            subprocess.check_output(shlex.split(cmd))
        except subprocess.CalledProcessError as e:
            logger.error(f"## Error setting up {cmd}: {e}")
            return False

        return True

    def _install_slurm_from_deb(self) -> bool:
        """Install Slurm debs.

        Returns:
            bool: True on success and False otherwise.
        """
        subprocess.check_output(["apt-get", "update"])

        # update specific needed dependencies
        logger.debug("## Installing dependencies")
        subprocess.check_output(["apt-get", "install", "--yes", "mailutils", "logrotate"])

        # setup munge
        logger.debug("## Installing munge")
        subprocess.check_output(["apt-get", "install", "--yes", "munge"])
        subprocess.check_output(["systemctl", "enable", self._munged_systemd_service])

        slurm_component = self._slurm_component
        logger.debug(f"## Installing {slurm_component}")
        try:
            subprocess.check_output(["apt-get", "install", "--yes",
                                     slurm_component, "slurm-client"])
        except subprocess.CalledProcessError as e:
            logger.error(f"## Error installing {slurm_component} - {e}")
            return False

        subprocess.check_output(["apt-get", "autoremove", "--yes"])

        # we need to override the default service unit for slurmrestd only
        if "slurmrestd" == self._slurm_component:
            self.setup_slurmrestd_systemd_unit()

        # symlink /usr/lib64/slurm -> /usr/lib/x86_64-linux-gnu/slurm-wlm/ to
        # have "standard" location accross OSes
        lib64_slurm = Path("/usr/lib64/slurm")
        if not lib64_slurm.exists():
            lib64_slurm.symlink_to("/usr/lib/x86_64-linux-gnu/slurm-wlm/")

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

    def upgrade(self) -> bool:
        """Run upgrade operations."""
        return self._install_slurm_from_deb()

    def setup_slurm(self, custom_ppa: str = "") -> bool:
        """Install Slurm and its dependencies.

        Args:
            custom_ppa: URL to a custom repository. Setting it to any value
                        superseeds the Omnivector stable PPA.
        Returns:
            bool: whether the installation succeds or not.
        """
        if not self._setup_ppas(custom_ppa):
            return False
        if not self._install_slurm_from_deb():
            return False

        self._setup_paths()
        self.slurm_systemctl('enable')

        return True
