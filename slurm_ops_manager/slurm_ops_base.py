#!/usr/bin/env python3
"""This module provides the SlurmInstallManager."""
import logging
import os
import subprocess
from base64 import b64decode, b64encode
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from slurm_ops_manager.utils import get_hostname


logger = logging.getLogger()


class SlurmOpsManagerBase:
    """Base class for slurm ops."""

    _TEMPLATE_DIR = Path(
        os.path.dirname(os.path.abspath(__file__))) / 'templates'

    def __init__(self, component, resource_path):
        """Set the initial values for attributes in the base class."""
        self._resource_path = resource_path

        port_map = {
            'slurmctld': 6817,
            'slurmd': 6818,
            'slurmdbd': 6819,
            'slurmrestd': 6820,
        }

        self._slurm_cmds = [
            "sacct",
            "sacctmgr",
            "salloc",
            "sattach",
            "sbatch",
            "sbcast",
            "scancel",
            "scontrol",
            "sdiag",
            "sinfo",
            "sprio",
            "squeue",
            "sreport",
            "srun",
            "sshare",
            "sstat",
            "strigger",
        ]

        if component in ['slurmd', 'slurmctld', 'slurmrestd']:
            self._slurm_conf_template_name = 'slurm.conf.tmpl'
            self._slurm_conf_path = self._slurm_conf_dir / 'slurm.conf'
        elif component == "slurmdbd":
            self._slurm_conf_template_name = 'slurmdbd.conf.tmpl'
            self._slurm_conf_path = self._slurm_conf_dir / 'slurmdbd.conf'
        else:
            raise Exception(f'slurm component {component} not supported')

        self._slurm_component = component

        self._slurmd_log_file = self._slurm_log_dir / 'slurmd.log'
        self._slurmctld_log_file = self._slurm_log_dir / 'slurmctld.log'
        self._slurmdbd_log_file = self._slurm_log_dir / 'slurmdbd.log'

        self._slurmd_pid_file = self._slurm_pid_dir / 'slurmd.pid'
        self._slurmctld_pid_file = self._slurm_pid_dir / 'slurmctld.pid'
        self._slurmdbd_pid_file = self._slurm_pid_dir / 'slurmdbd.pid'

        self._hostname = get_hostname()
        self._port = port_map[self._slurm_component]

        self._slurm_conf_template_location = \
            self._TEMPLATE_DIR / self._slurm_conf_template_name

    @property
    def hostname(self) -> str:
        """Return the hostname."""
        return self._hostname

    @property
    def port(self) -> str:
        """Return the port."""
        return self._port

    @property
    def slurm_conf_path(self) -> Path:
        """Return the slurm conf path."""
        return self._slurm_conf_path

    @property
    def slurm_is_active(self) -> bool:
        """Return True if the slurm component is running."""
        return self._slurm_systemctl("is-active") == 0

    def _slurm_systemctl(self, operation):
        """Start systemd services for slurmd."""
        supported_systemctl_cmds = [
            "enable",
            "start",
            "stop",
            "restart",
            "is-active",
        ]

        if operation not in supported_systemctl_cmds:
            msg = f"Unsupported systemctl command: {operation}"
            logger.error(msg)
            raise Exception(msg)
        try:
            subprocess.call([
                "systemctl",
                operation,
                self._slurm_systemd_service,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running {operation} - {e}")

    @property
    def _slurm_conf_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_spool_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_state_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_plugin_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_log_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_plugstack_conf(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_plugstack_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_pid_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _mail_prog(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _munge_socket(self) -> Path:
        """Return the munge socket."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_user(self) -> str:
        """Return the slurm user."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_group(self) -> str:
        """Return the slurm group."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_systemd_service(self) -> str:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _munge_key_path(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _munged_systemd_service(self) -> str:
        raise Exception("Inheriting object needs to define this property.")

    def upgrade(self):
        """Preform upgrade-charm operations."""
        raise Exception("Inheriting object needs to define this method.")

    def setup_system(self):
        """Preform the install and setup operations."""
        raise Exception("Inheriting object needs to define this method.")

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        raise Exception("Inheriting object needs to define this property.")

    def write_slurm_config(self, context) -> None:
        """Render the context to a template, adding in common configs."""
        common_config = {
            'munge_socket': str(self._munge_socket),
            'mail_prog': str(self._mail_prog),
            'slurm_state_dir': str(self._slurm_state_dir),
            'slurm_spool_dir': str(self._slurm_spool_dir),
            'slurm_plugin_dir': str(self._slurm_plugin_dir),
            'slurmdbd_log_file': str(self._slurmdbd_log_file),
            'slurmd_log_file': str(self._slurmd_log_file),
            'slurmctld_log_file': str(self._slurmctld_log_file),
            'slurmdbd_pid_file': str(self._slurmdbd_pid_file),
            'slurmd_pid_file': str(self._slurmd_pid_file),
            'slurmctld_pid_file': str(self._slurmctld_pid_file),
            'slurm_plugstack_conf': str(self._slurm_plugstack_conf),
            'slurm_user': str(self._slurm_user),
        }

        template_name = self._slurm_conf_template_name
        source = self._slurm_conf_template_location
        target = self._slurm_conf_path

        if not type(context) == dict:
            raise TypeError("Incorrect type for config.")

        if not source.exists():
            raise FileNotFoundError(
                "The slurm config template cannot be found."
            )

        rendered_template = Environment(
            loader=FileSystemLoader(str(self._TEMPLATE_DIR))
        ).get_template(template_name)

        if target.exists():
            target.unlink()

        target.write_text(
            rendered_template.render(
                {**context, **common_config}
            )
        )

    def restart_slurm_component(self):
        """Restart the slurm component."""
        self._slurm_systemctl("restart")

    def write_munge_key(self, munge_key):
        """Write the munge key."""
        key = b64decode(munge_key.encode())
        self._munge_key_path.write_bytes(key)

    def get_munge_key(self) -> str:
        """Read, encode, decode and return the munge key."""
        munge_key = self._munge_key_path.read_bytes()
        return b64encode(munge_key).decode()

    def restart_munged(self):
        """Restart munged."""
        try:
            return subprocess.call([
                "systemctl",
                "restart",
                self._munged_systemd_service,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying systemd - {e}")
            return -1
