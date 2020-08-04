import os
from ops.framework import Object
from pathlib import Path
import logging
import os
import socket
import subprocess
import tarfile
from base64 import (
    b64encode,
    b64decode,
)
from ops.model import BlockedStatus
from jinja2 import Environment, FileSystemLoader
from slurm_ops_manager.slurm_snap_ops import SlurmSnapManager 
from slurm_ops_manager.slurm_tar_ops import SlurmTarManager 

logger = logging.getLogger()

class SlurmOpsManager(Object):

    _CHARM_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    _TEMPLATE_DIR = _CHARM_DIR / 'templates'
    def __init__(self, charm, component):
        super().__init__(charm, component)
        self.charm = charm
        self._slurm_component = component
        self._resource_path = None
        self._is_tar = None
        try:
            self._resource_path = self.model.resources.fetch('slurm')
        except:
            logger.debug("no resource was supplied installing from snap store")
        try:
            self._is_tar = tarfile.is_tarfile(self._resource_path)
        except:
            logger.debug("no resource path")
        
        if self._is_tar:
            self.slurm_resource = SlurmTarManager(component, self._resource_path)
        else:
            self.slurm_resource = SlurmSnapManager(component, self._resource_path)

    def install(self):
        self.slurm_resource.install()
        #version = self.slurm_resource.get_version()
        #self.charm.unit.set_workload_version(version)

    def render_config_and_restart(self, slurm_config) -> None:
        """Render the slurm.conf and munge key, restart slurm and munge."""
        self._write_config(slurm_config)
        self._write_munge_key_and_restart(slurm_config['munge_key'])
        is_active = None
        try:
            is_active = subprocess.call([
                "systemctl",
                "is-active",
                self.slurm_resource.get_systemd_name(),
            ]) == 0
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running restarting slurm daemon - {e}")

        if is_active:
            self._slurm_systemctl("restart")
        else:
            self._slurm_systemctl("start")


    def _slurm_systemctl(self, operation) -> None:
        """Start systemd services for slurmd."""
        try:
            subprocess.call([
                "systemctl",
                operation,
                self.slurm_resource.get_systemd_name(),
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying systemd - {e}")

    def _write_config(self, context) -> None:
        """Render the context to a template.
        target: /var/snap/slurm/common/etc/slurm/slurm.conf
        source: /templates/slurm.conf.tmpl
        file name can also be slurmdbdb.conf
        """
        template_name = self.slurm_resource.get_tmpl_name()
        source = self.slurm_resource.get_template()
        target = self.slurm_resource.get_target()
        ctxt = { **context, **self.slurm_resource.config}
        
        rendered_template = Environment(
            loader=FileSystemLoader(str(self._TEMPLATE_DIR))
        ).get_template(template_name)
        
        target.write_text(rendered_template.render(ctxt))

    def _write_munge_key_and_restart(self, munge_key) -> None:
        key = b64decode(munge_key.encode())
        self.slurm_resource.get_munge_key_path().write_bytes(key)

        try:
            subprocess.call([
                "service",
                self.slurm_resource.munge_sysd,
                "restart"
            ])
        except subprocess.CalledProcessError as e:
            logger.debug(e)
