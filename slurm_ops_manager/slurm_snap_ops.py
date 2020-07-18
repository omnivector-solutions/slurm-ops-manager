import os
import logging
import socket
import subprocess
from base64 import b64decode, b64encode
from pathlib import Path
from time import sleep

from jinja2 import Environment, FileSystemLoader

class SlurmSnapManager:
    def __init__(self, component, res_path):
        self.config = {}
        self._template_name = 'slurm.conf.tmpl'
        self._source = "templates/" + self.template_name
        self._target = "var/snap/slurm/common/slurm-configurator/slurm.conf"
        self._slurm_component =  component
        self._systemd_service = "snap.slurm." + self._slurm_component
        self._MUNGE_KEY_PATH = Path("/var/snap/slurm/common/etc/munge/munge.key")

    def render_config_and_restart(self, slurm_config) -> None:
        """Render the slurm.conf and munge key, restart slurm and munge."""
        if not type(slurm_config) == dict:
            raise TypeError("Incorrect type for config.")

        self._write_config(slurm_config)
        self._write_munge_key_and_restart(slurm_config['munge_key'])

        if self.is_active:
            self._slurm_systemctl("restart")
        else:
            self._slurm_systemctl("start")

        if not self.is_active:
            raise Exception(f"SLURM {self._slurm_component}: not starting")
    
    def _slurm_systemctl(self, operation) -> None:
        """Start systemd services for slurmd."""
        if operation not in ["enable", "start", "stop", "restart"]:
            msg = f"Unsupported systemctl command for {self._slurm_component}"
            raise Exception(msg)

        try:
            subprocess.call([
                "systemctl",
                operation,
                self._systemd_service,
            ])
            # Fix this later
            if operation == "start":
                self._store.slurm_started = True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying systemd - {e}")

    def _write_config(self, context) -> None:
        """Render the context to a template."""
        template_name = self._template_name
        source = self._source
        target = self._target

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

        target.write_text(rendered_template.render(context))
    
    def install():
        self._install_snap()
        self._snap_connect()
        self._set_snap_mode()

    def _install_snap(self):
        snap_install_cmd = ["snap", "install"]
        resource_path = None
        try:
            resource_path = self.model.resources.fetch('slurm')
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
    def _write_munge_key_and_restart(self, munge_key) -> None:
        key = b64decode(munge_key.encode())
        self._MUNGE_KEY_PATH.write_bytes(key)
        try:
            subprocess.call(["service", "munge", "restart"])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def get_munge_key(self) -> str:
        """Read, encode, decode and return the munge key as a string."""
        munge_key = self._MUNGE_KEY_PATH.read_bytes()
        return b64encode(munge_key).decode()
