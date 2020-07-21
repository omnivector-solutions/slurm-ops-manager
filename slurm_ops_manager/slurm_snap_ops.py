import os
import logging
import socket
import subprocess
from base64 import b64decode, b64encode
from pathlib import Path
from time import sleep


from ops.model import (
    ModelError,
)
from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger()

class SlurmSnapManager:
    _CHARM_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    _TEMPLATE_DIR = _CHARM_DIR / 'templates'
    def __init__(self, component, res_path):
        self._slurm_component =  component
        self._resource = res_path
        if component == "slurmdbd":
            self._template_name = "slurmdbd.conf.tmpl"
            self._target = Path("/var/snap/slurm/common/etc/slurm/slurmdbd.conf")
        else:
            self._template_name = "slurm.conf.tmpl"
            self._target = Path("/var/snap/slurm/common/etc/slurm/slurm.conf")
        
        self._source = Path(self._TEMPLATE_DIR / self._template_name)
        self._systemd_service = "snap.slurm." + self._slurm_component
        self._MUNGE_KEY_PATH = Path("/var/snap/slurm/common/etc/munge/munge.key")
        self.config_values = { 
                "clustername": "slurm",
                "munge_socket": "/tmp/munged.socket.2",
                "mail_prog": "/snap/slurm/current/usr/bin/mail.mailutils",
                "slurm_user": "root",
                "slurmctld_pid_file": "/tmp/slurmctld.pid",
                "slurmd_pid_file": "/tmp/slurmd.pid",
                "slurmctld_log_file": "/var/snap/slurm/common/var/log/slurm/slurmctld.log",
                "slurmd_log_file": "/var/snap/slurm/common/var/log/slurm/slurmd.log",
                "slurm_spool_dir": "/var/snap/slurm/common/var/spool/slurm/d",
                "slurm_state_dir": "/var/snap/slurm/common/var/spool/slurm/ctld",
                "slurm_plugin_dir": "/snap/slurm/current/lib/slurm",
                "slurm_plugstack_conf": 
                "/var/snap/slurm/common/etc/slurm/plugstack.d/plugstack.conf",
                "munge_socket": "/tmp/munged.socket.2",
                "slurmdbd_pid_file": "/tmp/slurmdbd.pid",
                "slurmdbd_log_file": "/var/snap/slurm/common/var/log/slurm/slurmdbd.log",
        }
    
    @property
    def config(self):
        return self.config_values

    def get_systemd_name(self):
        return "snap.slurm." + self._slurm_component

    def get_munge_key_path(self):
        return self._MUNGE_KEY_PATH

    def get_template(self):
        logger.debug(self._source)
        return self._source

    def get_target(self):
        return self._target

    def get_tmpl_name(self):
        return self._template_name

    def install(self):
        self._install_snap()
        self._snap_connect()
        self._set_snap_mode()

    def _install_snap(self):
        snap_install_cmd = ["snap", "install"]
        resource_path = None
        try:
            resource_path = self._resource
        except ModelError as e:
            logger.error(
                f"Resource could not be found when executing: {e}",
                exc_info=True,
            )
        if resource_path:
            snap_install_cmd.append(resource_path)
            snap_install_cmd.append("--dangerous")
            snap_install_cmd.append("--classic")
        else:
            snap_store_channel = self.fw_adapter.get_config("snap-store-channel")
            snap_install_cmd.append("slurm")
            snap_install_cmd.append(f"--{snap_store_channel}")
        try:
            subprocess.call(snap_install_cmd)
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Could not install the slurm snap using the command: {e}"
            )
    
    def _snap_connect(self, slot=None):
        connect_commands = [
            ["snap", "connect", "slurm:network-control"],
            ["snap", "connect", "slurm:system-observe"],
            ["snap", "connect", "slurm:hardware-observe"],
        ]

        for connect_command in connect_commands:
            if slot:
                connect_command.append(slot)
            try:
                subprocess.call(connect_command)
            except subprocess.CalledProcessError as e:
                logger.error(
                    f"Could not connect snap interface: {e}"
                )

    def _set_snap_mode(self):
        """Set the snap mode, thorw an exception if it fails.
        """
        try:
            subprocess.call([
                "snap",
                "set",
                "slurm",
                f"snap.mode={self._slurm_component}",
            ])
        except subprocess.CalledProcessError as e:
            logger.error(
               f"Setting the snap.mode failed. snap.mode={self._slurm_component} - {e}"
            )
