#! /usr/bin/env python3
import os
from pathlib import Path
from slurm_ops_manager.install import TarInstall

class SlurmTarManager:
    "config values of slurm tar binary install."
    _CHARM_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    _TEMPLATE_DIR = _CHARM_DIR / 'templates'

    def __init__(self, component, res_path):
        """Determine values based on slurm component."""    
        super().__init__(charm, component)
        self._slurm_installer = TarInstall(component, res_path)
        self._munge_key_path = Path("/etc/munge/munge.key")
        if component in ['slurmd', 'slurmctld', 'slurmrestd', "none"]:
            self._template_name = 'slurm.conf.tmpl'
            self._target = '/etc/slurm/slurm.conf'
        elif component == "slurmdbd":
            self._template_name = 'slurmdbd.conf.tmpl'
            self._target = '/etc/slurm/slurmdbd.conf'
        else:
            raise Exception(f'slurm component {component} not supported')
        self._slurm_component = component
        self._template = self._TEMPLATE_DIR / self._template_name

    def get_systemd_name(self):
        return self._slurm_component

    def get_tmpl_name():
        return self._template_name

    def get_template():
        return self._template

    def get_target():
        return self._target

    def get_munge_key_path():
        return self._munge_key_path

    def install():
        self._slurm_installer.prepare_system_for_slurm()
