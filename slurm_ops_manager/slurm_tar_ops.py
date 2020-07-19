import os
import logging
import socket
import subprocess
from base64 import b64decode, b64encode
from pathlib import Path
from time import sleep

from jinja2 import Environment, FileSystemLoader
from .install import SlurmTarinstall

class SlurmTarManager:
      """Slurm installation of lifecycle ops."""
    _SLURM_CONF_DIR = Path('/etc/slurm')
    _MUNGE_KEY_PATH = Path("/etc/munge/munge.key")

    def __init__(self, component, res_path):
        """Determine values based on slurm component."""
        super().__init__(charm, component)
        self._slurm_installer = SlurmTarInstall(component, res_path)

        if component in ['slurmd', 'slurmctld', 'slurmrestd']:
            self._slurm_conf_template_name = 'slurm.conf.tmpl'
            self._slurm_conf = self._SLURM_CONF_DIR / 'slurm.conf'
        elif component == "slurmdbd":
            self._slurm_conf_template_name = 'slurmdbd.conf.tmpl'
            self._slurm_conf = self._SLURM_CONF_DIR / 'slurmdbd.conf'
        else:
            raise Exception(f'slurm component {component} not supported')

        self._slurm_component = component

        self.hostname = socket.gethostname().split(".")[0]
        self.port = port_map[component]

        self._slurm_conf_template_location = \
            self._TEMPLATE_DIR / self._slurm_conf_template_name

    def get_systemd_name(self):
        return self._slurm_component

    def get_tmpl_name():
        return self.slurm_conf_template_name

    def get_template():
        return self._slurm_conf_template_location

    def get_target():
        return self._slurm_conf

    def get_munge_key_path():
        return self._MUNGE_KEY_PATH

    def install():
        self._slurm_installer.prepare_system_for_slurm()