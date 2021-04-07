#!/usr/bin/env python3
"""This module provides the SlurmRpmManager."""
import logging
import subprocess
import sys
from pathlib import Path

from slurm_ops_manager.slurm_ops_base import SlurmOpsManagerBase


logger = logging.getLogger()


class SlurmRpmManager(SlurmOpsManagerBase):
    """Slurm debian operations manager."""

    def __init__(self, component):
        """Set initial attribute values."""
        super().__init__(component)

    @property
    def _slurm_bin_dir(self) -> Path:
        """Return the directory where the slurm bins live."""
        return Path("/usr/bin") # move to base class

    @property
    def _slurm_conf_dir(self) -> Path:
        return Path("/etc/slurm")  # move to base class

    @property
    def _slurm_spool_dir(self) -> Path:
        return Path("/var/spool/slurmd") # move to base class

    @property
    def _slurm_state_dir(self) -> Path:
        return Path("/var/spool/slurmctld") # move to base class

    @property
    def _slurm_plugin_dir(self) -> Path:
        return Path("/usr/lib64/slurm/")

    @property
    def _slurm_log_dir(self) -> Path:
        return Path("/var/log/slurm") # move to base class

    @property
    def _slurm_pid_dir(self) -> Path:
        return Path("/var/run/") # move to base class

    @property
    def _mail_prog(self) -> Path:
        return Path("/usr/bin/mailx")

    @property
    def _munge_key_path(self) -> Path:
        return Path("/etc/munge/munge.key")  # move to base class

    @property
    def _slurm_plugstack_dir(self) -> Path:
        return Path("/etc/slurm/plugstack.d") # move to base class

    @property
    def _slurm_plugstack_conf(self) -> Path:
        return self._slurm_plugstack_dir / 'plugstack.conf' # TODO check this

    @property
    def _slurm_user(self) -> str:
        """Return the slurm user."""
        return "slurm" # move to base class

    @property
    def _slurm_group(self) -> str:
        """Return the slurm group."""
        return "slurm" # move to base class

    @property
    def _slurmd_user(self) -> str:
        """Return the slurmd user."""
        return "root" # move to base class

    @property
    def _slurmd_group(self) -> str:
        """Return the slurmd group."""
        return "root" # move to base class

    @property
    def _slurm_systemd_service(self) -> str:
        return f"{self._slurm_component}"

    @property
    def _munge_socket(self) -> Path:
        """Return the munge socket."""
        return Path("/var/run/munge/munge.socket.2") # move to base class

    @property
    def _munged_systemd_service(self) -> str:
        return "munge" # move to base class

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        # from EPEL7
        return "20.11.2"

    def _install_slurm_from_rpm(self):
        """Install Slurm debs"""

        slurm_component = self._slurm_component

        # the dispatch file in the charms takes care of installing epel and py3
        subprocess.call(["yum", "makecache"])

        # update/install specific needed dependencies
        subprocess.call(["yum", "install", "--assumeyes",
                         "pciutils", "logrotate", "mailx",
                         "munge-0.5.11"]) # pin munge vesion

        try:
            # @todo: improve slurm version handling
            subprocess.call(["yum", "install", "--assumeyes",
                             f"slurm-{slurm_component}-{self.slurm_version}",
                             "slurm-" + self.slurm_version])
        except subprocess.CalledProcessError as e:
            print(f"Error installing {slurm_component} - {e}")
            # @todo: set appropriate juju status
            return -1

        # munge rpm does not create a munge key, so we need to create one
        keycmd = f"dd if=/dev/urandom of={str(self._munge_key_path)} bs=1 count=1024"
        subprocess.call(keycmd.split())
        subprocess.call(f"chown munge:munge {str(self._munge_key_path)}".split())
        subprocess.call(f"chmod 0400 {str(self._munge_key_path)}".split())

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
        self._install_slurm_from_rpm()
        self._setup_paths()
