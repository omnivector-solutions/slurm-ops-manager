import os
from ops.framework import (
    Object,
    StoredState,
)
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

from slurm_ops_manager.slurm_snap_ops import SlurmSnapManager 
from slurm_ops_manager.slurm_tar_ops import SlurmTarManager 
from slurm_ops_manager.utils import get_hostname, get_inventory

class SlurmOpsManager(Object):

    _TEMPLATE_DIR = Path(f"{os.getcwd()}/templates")
    _stored = StoredState()

    def __init__(self, charm, component):
        super().__init__(charm, component)
        self._slurm_component = component
        self._stored.set_default(slurm_installed=False)
        self._resource_path = None
        try:
            self.model.resources.fetch('slurm')
        except:
            raise Exception("no resource was given")

        self.hostname = socket.gethostname().split(".")[0]
        self._is_tar = tarfile.is_tarfile(self._resource_path)
        
        if self._is_tar:
            self.slurm_resource = SlurmTarManager(component, self._resource_path)
        else:
            self.slurm_resource = SlurmSnapManager(component, self._resource_path)

    def install(self):
        self.slurm_resource.install()
        self._stored.slurm_installed = True
    

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
                self.slurm_resource.get_systemd_name(),
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying systemd - {e}")

    def _write_config(self, context) -> None:
        """Render the context to a template."""
        template_name = self._slurm_resoure.get_tmpl_name()
        source = self.slurm_resource.get_template()
        target = self.slurm_resource.get_target()

        ctxt = { **context, **self.slurm_resource.config}
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

        target.write_text(rendered_template.render(ctxt))

    def _write_munge_key_and_restart(self, munge_key) -> None:
        key = b64decode(munge_key.encode())
        self.slurm_resource.get_munge_key_path().write_bytes(key)

        try:
            subprocess.call(["service", "munge", "restart"])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def get_munge_key(self) -> str:
        """Read, encode, decode and return the munge key as a string."""
        path = self.slurm_resource.get_munge_key_path()
        munge_key = path.read_bytes()
        return b64encode(munge_key).decode()
