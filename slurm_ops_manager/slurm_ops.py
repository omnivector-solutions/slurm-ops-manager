#!/usr/bin/env python3
"""This module provides the SlurmManager."""
import logging
import tarfile
from pathlib import Path
from time import sleep

from ops.framework import (
    Object,
    StoredState,
)
from ops.model import ModelError
from slurm_ops_manager.slurm_ops_managers import (
    SlurmSnapManager,
    SlurmTarManager,
)
from slurm_ops_manager.utils import get_inventory


logger = logging.getLogger()


class SlurmManager(Object):
    """SlurmOpsManager."""

    _stored = StoredState()

    def __init__(self, charm, component):
        """Set the initial attribute values."""
        super().__init__(charm, component)
        self._stored.set_default(slurm_installed=False)
        self._stored.set_default(slurm_version_set=False)
        self._stored.set_default(resource_path=None)
        self._stored.set_default(resource_checked=False)

        self._charm = charm
        self._slurm_component = component

        if not self._stored.resource_checked:
            try:
                self._stored.resource_path = str(
                    self.model.resources.fetch('slurm')
                )
            except ModelError as e:
                logger.debug(e)
            self._stored.resource_checked = True

        if self._stored.resource_path is not None:
            resource_size = Path(self._stored.resource_path).stat().st_size
            if resource_size > 0:
                if tarfile.is_tarfile(self._stored.resource_path):
                    self._slurm_resource_manager = SlurmTarManager(
                        self._slurm_component,
                        self._stored.resource_path
                    )
                else:
                    self._slurm_resource_manager = SlurmSnapManager(
                        self._slurm_component,
                        self._stored.resource_path
                    )
            else:
                self._slurm_resource_manager = SlurmSnapManager(
                    self._slurm_component,
                    self._stored.resource_path
                )
        else:
            self._slurm_resource_manager = SlurmSnapManager(
                self._slurm_component,
                self._stored.resource_path
            )

    @property
    def hostname(self):
        """Return the hostname."""
        return self._slurm_resource_manager.hostname

    @property
    def port(self):
        """Return the port."""
        return self._slurm_resource_manager.port

    @property
    def inventory(self) -> str:
        """Return the node inventory and gpu count."""
        return get_inventory()

    @property
    def slurm_installed(self) -> bool:
        """Return the bool from the stored state."""
        return self._stored.slurm_installed

    def get_munge_key(self) -> str:
        """Return the munge key."""
        return self._slurm_resource_manager.get_munge_key()

    def install(self) -> None:
        """Prepare the system for slurm."""
        while True:
            try:
                ret = subprocess.check_call(['snap', 'list'], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
                logger.debug(ret)
                if ret == 0:
                    break
            except:
                sleep(1)
                logger.debug("snapd not installed")

        self._slurm_resource_manager.setup_system()
        self._stored.slurm_installed = True

    def upgrade(self) -> None:
        """Upgrade slurm."""
        self._slurm_resource_manager.upgrade()

    def render_config_and_restart(self, slurm_config) -> None:
        """Render the slurm.conf and munge key, restart slurm and munge."""
        if not type(slurm_config) == dict:
            raise TypeError("Incorrect type for config.")

        # Write munge.key and restart munged.
        self._slurm_resource_manager.write_munge_key(slurm_config['munge_key'])
        self._slurm_resource_manager.restart_munged()
        sleep(1)

        # Write slurm.conf and restart the slurm component.
        self._slurm_resource_manager.write_slurm_config(slurm_config)
        self._slurm_resource_manager.restart_slurm_component()
        sleep(1)

        if not self._stored.slurm_version_set:
            self._charm.unit.set_workload_version(
                self._slurm_resource_manager.slurm_version
            )
            self._stored.slurm_version_set = True
