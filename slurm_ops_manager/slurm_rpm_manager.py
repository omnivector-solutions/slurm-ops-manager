#!/usr/bin/env python3
"""This module provides the SlurmRpmManager."""
import logging
import subprocess
from pathlib import Path

from slurm_ops_manager.slurm_ops_base import SlurmOpsManagerBase


logger = logging.getLogger()


class SlurmRpmManager(SlurmOpsManagerBase):
    """Slurm debian operations manager."""

    def __init__(self, component):
        """Set initial attribute values."""
        super().__init__(component)

    @property
    def _slurm_plugin_dir(self) -> Path:
        return Path("/usr/lib64/slurm/")

    @property
    def _mail_prog(self) -> Path:
        return Path("/usr/bin/mailx")

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        cmd = 'yum info -C slurm | grep "^Version"'
        locale = {'LC_ALL': 'C', 'LANG': 'C.UTF-8'}
        version = subprocess.check_output(cmd, shell=True, env=locale)
        return version.decode().split(":")[-1].strip()

    @property
    def munge_version(self) -> str:
        """Return munge verion."""
        cmd = 'yum info -C munge | grep "^Version"'
        locale = {'LC_ALL': 'C', 'LANG': 'C.UTF-8'}
        version = subprocess.check_output(cmd, shell=True, env=locale)
        return version.decode().split(":")[-1].strip()

    def _install_slurm_from_rpm(self) -> bool:
        """Install Slurm rpms.

        Returns True on success and False otherwise.
        """

        slurm_component = self._slurm_component

        # the dispatch file in the charms takes care of installing epel and py3
        subprocess.call(["yum", "makecache"])

        # update/install specific needed dependencies
        subprocess.call(["yum", "install", "--assumeyes",
                         "pciutils", "logrotate", "mailx",
                         "munge"])
        subprocess.call(["systemctl", "enable", self._munged_systemd_service])

        try:
            # @todo: improve slurm version handling
            subprocess.check_output(["yum", "install", "--assumeyes",
                                     f"slurm-{slurm_component}", "slurm"])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error installing {slurm_component} - {e}")
            return False

        logger.info("#### All packages installed!")

        # munge rpm does not create a munge key, so we need to create one
        logger.info("#### Creating munge key")
        keycmd = f"dd if=/dev/urandom of={str(self._munge_key_path)} bs=1 count=1024"
        subprocess.call(keycmd.split())
        usergroup = f"{self._munge_user}:{self._munge_group}"
        subprocess.call(f"chown {usergroup} {str(self._munge_key_path)}".split())
        subprocess.call(f"chmod 0400 {str(self._munge_key_path)}".split())
        logger.info("#### Created munge key")

        # current rpms do not create a slurm user and group, so we create it
        logger.info("#### Creating slurm user and group")
        subprocess.call(["groupadd", "--gid", self._slurm_group_id,
                                     self._slurm_group])
        subprocess.call(["adduser", "--system",
                                    "--gid", self._slurm_group_id,
                                    "--uid", self._slurm_user_id,
                                    "--no-create-home",
                                    "--home", "/nonexistent",
                                    self._slurm_user])
        logger.info("#### Created slurm user and group")

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

    def upgrade(self) -> bool:
        """Run upgrade operations."""
        logger.warning("## This operation is not yet supported on CentOS.")
        return True

    def setup_slurm(self, custom_repo: str = "") -> bool:
        """Install Slurm and its dependencies.

        Args:
            custom_repo: URL to a custom repository. Setting it to any value
                         superseeds the Omnivector Repository.
        Returns:
            bool: whether the installation succeds or not.
        """
        successful_installation = self._install_slurm_from_rpm()
        self._setup_paths()
        self.slurm_systemctl('enable')

        return successful_installation
