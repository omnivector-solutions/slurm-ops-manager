#!/usr/bin/env python3
"""This module provides the SlurmManager."""
import logging
import subprocess
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

        self._charm = charm
        self._slurm_component = component

        self._stored.set_default(slurm_installed=False)
        self._stored.set_default(slurm_version_set=False)
        self._stored.set_default(resource_path=None)
        self._stored.set_default(resource_checked=False)

        if not self._stored.resource_checked:
            try:
                self._stored.resource_path = str(
                    self.model.resources.fetch('slurm')
                )
            except ModelError as e:
                logger.debug(e)
            self._stored.resource_checked = True

        logger.debug(
            "__init__(): self._stored.resource_path="
            f"{self._stored.resource_path}"
        )

        if self._stored.resource_path is not None:
            resource_size = Path(self._stored.resource_path).stat().st_size

            logger.debug(f'__init__(): resource_size={resource_size}')

            if resource_size > 0:
                if tarfile.is_tarfile(self._stored.resource_path):
                    logger.debug('__init__(): slurm resource is tar file.')
                    self._slurm_resource_manager = SlurmTarManager(
                        self._slurm_component,
                        self._stored.resource_path
                    )
                else:
                    logger.debug('__init__(): slurm resource is a snap file.')
                    self._slurm_resource_manager = SlurmSnapManager(
                        self._slurm_component,
                        self._stored.resource_path
                    )
            else:
                logger.debug('__init__(): slurm resource is a snap file.')

                self._slurm_resource_manager = SlurmSnapManager(
                    self._slurm_component,
                    self._stored.resource_path
                )
        else:
            logger.debug('__init__(): slurm resource from snapstore.')

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
            check = check_snapd()
            if check == 0:
                break
            sleep(1)

        self._slurm_resource_manager.setup_system()
        self._stored.slurm_installed = True

    def upgrade(self, slurm_config) -> None:
        """Upgrade slurm."""
        logger.debug('upgrade(): entering')
        # Pull the slurm snap from the controller on upgrade.
        try:
            self._stored.resource_path = str(
                self.model.resources.fetch('slurm')
            )
        except ModelError as e:
            logger.debug(e)
        self._slurm_resource_manager.upgrade()
        self.render_config_and_restart(slurm_config)

    def render_config_and_restart(self, slurm_config) -> None:
        """Render the slurm.conf and munge key, restart slurm and munge."""
        logger.debug('render_config_and_restart(): entering')

        if not type(slurm_config) == dict:
            raise TypeError("Incorrect type for config.")

        # cgroup_config setting is not there for slurmdbd, will raise error.
        if 'cgroup_config' in slurm_config:
            # write cgroup.conf
            content = slurm_config['cgroup_config']
            self._slurm_resource_manager.write_cgroup_conf(content)

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


def check_snapd():
    """Check to see if snapd is installed."""
    try:
        subprocess.check_call(
            ['snap', 'list'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )
        return 0
    except subprocess.CalledProcessError as e:
        logger.debug(f"snapd error: {e}")
        return 1
