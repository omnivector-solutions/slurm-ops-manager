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
        return Path("/var/spool/slurm/d")

    @property
    def _slurm_state_dir(self) -> Path:
        return Path("/var/spool/slurm/ctld")

    @property
    def _slurm_plugin_dir(self) -> Path:
        return Path("/usr/lib/slurm")

    @property
    def _slurm_log_dir(self) -> Path:
        return Path("/var/log/slurm")

    @property
    def _slurm_pid_dir(self) -> Path:
        return Path("/tmp")

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
        return "slurm"

    @property
    def _slurm_group(self) -> str:
        """Return the slurm group."""
        return "slurm"

    @property
    def _slurm_systemd_service(self) -> str:
        return f"{self._slurm_component}"

    @property
    def _munge_socket(self) -> Path:
        """Return the munge socket."""
        return Path("/tmp/munged.socket.2")

    @property
    def _munged_systemd_service(self) -> str:
        return "munged"

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        try:
            slurm_version = subprocess.check_output(
                [
                    'slurmd',
                    '-V'
                ]
            ).decode().strip()
        except subprocess.CalledProcessError as e:
            print(f"Cannot get slurm version - {e}")
            sys.exit(-1)

        return slurm_version

    def _install_slurm_from_deb(self):

        slurm_component = self._slurm_component

        try:
            subprocess.call([
                "apt",
                "install",
                "-y",
                slurm_component,
            ])
        except subprocess.CalledProcessError as e:
            print(f"Error installing {slurm_component} - {e}")


    def upgrade(self, channel):
        """Run upgrade operations."""
        pass

    def setup_system(self) -> None:
        """Install the slurm deb."""
        self._install_slurm_from_deb()
        self._provision_snap_systemd_service_override_file()
        self._systemctld_daemon_reload()
