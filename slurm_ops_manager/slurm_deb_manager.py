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
    def _slurm_bin_dir(self) -> Path:
        """Return the directory where the slurm bins live."""
        return Path("/usr/bin")

    @property
    def _slurm_conf_dir(self) -> Path:
        return Path("/etc/slurm")

    @property
    def _slurm_spool_dir(self) -> Path:
        return Path("/var/spool/slurmd")

    @property
    def _slurm_state_dir(self) -> Path:
        return Path("/var/spool/slurmctld")

    @property
    def _slurm_plugin_dir(self) -> Path:
        return Path("/usr/lib/x86_64-linux-gnu/slurm-wlm/")

    @property
    def _slurm_log_dir(self) -> Path:
        return Path("/var/log/slurm")

    @property
    def _slurm_pid_dir(self) -> Path:
        return Path("/var/run/")

    @property
    def _mail_prog(self) -> Path:
        return Path("/usr/bin/mail.mailutils")

    @property
    def _munge_key_path(self) -> Path:
        return Path("/etc/munge/munge.key")

    @property
    def _slurm_plugstack_dir(self) -> Path:
        return Path("/etc/slurm/plugstack.d")

    @property
    def _slurm_plugstack_conf(self) -> Path:
        return self._slurm_plugstack_dir / 'plugstack.conf'

    @property
    def _slurm_user(self) -> str:
        """Return the slurm user."""
        if "slurmd" == self._slurm_component:
            return "root"
        else:
            return "slurm"

    @property
    def _slurm_group(self) -> str:
        """Return the slurm group."""
        if "slurmd" == self._slurm_component:
            return "root"
        else:
            return "slurm"

    @property
    def _slurmd_user(self) -> str:
        """Return the slurmd user."""
        return "root"

    @property
    def _slurmd_group(self) -> str:
        """Return the slurmd group."""
        return "root"

    @property
    def _slurm_systemd_service(self) -> str:
        return f"{self._slurm_component}"

    @property
    def _munge_socket(self) -> Path:
        """Return the munge socket."""
        return Path("/var/run/munge/munge.socket.2")

    @property
    def _munged_systemd_service(self) -> str:
        return "munged"

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

        user = f"{self._slurm_user}:{self._slurm_group}"

        if not self._slurm_conf_dir.exists():
            self._slurm_conf_dir.mkdir()
        subprocess.call(["chown", "-R", user, self._slurm_conf_dir])

        if not self._slurm_state_dir.exists():
            self._slurm_state_dir.mkdir()
        subprocess.call(["chown", "-R", user, self._slurm_state_dir])

        if not self._slurm_spool_dir.exists():
            self._slurm_spool_dir.mkdir()
        subprocess.call(["chown", "-R", user, self._slurm_spool_dir])

    def upgrade(self, channel):
        """Run upgrade operations."""
        pass

    def setup_system(self) -> None:
        """Install the slurm deb."""
        self._install_slurm_from_deb()
        self._setup_paths()
