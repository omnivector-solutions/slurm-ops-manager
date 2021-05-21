#!/usr/bin/env python3
"""This module provides the SlurmManager."""
import logging

from ops.framework import (
    Object,
    StoredState,
)
from slurm_ops_manager import utils
from slurm_ops_manager.infiniband import Infiniband
from slurm_ops_manager.slurm_ops_managers import (
    SlurmDebManager,
    SlurmRpmManager,
)


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

        operating_system = utils.operating_system()

        if operating_system == "ubuntu":
            self._slurm_resource_manager = SlurmDebManager(component)
        elif operating_system == "centos":
            self._slurm_resource_manager = SlurmRpmManager(component)
        else:
            raise Exception("Unsupported OS")

        self.infiniband = Infiniband(charm, component)

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
        return utils.get_inventory()

    @property
    def slurm_installed(self) -> bool:
        """Return the bool from the stored state."""
        return self._stored.slurm_installed

    @property
    def slurm_component(self) -> str:
        """Return the slurm component."""
        return self._slurm_resource_manager.slurm_component

    def get_munge_key(self) -> str:
        """Return the munge key."""
        return self._slurm_resource_manager.get_munge_key()

    def get_slurm_conf(self) -> str:
        """Return the slurm.conf."""
        return self._slurm_resource_manager.slurm_conf_path.read_text()

    def install(self) -> None:
        """Prepare the system for slurm."""

        self._slurm_resource_manager.setup_slurm()
        if "slurmd" == self._slurm_component:
            self._slurm_resource_manager.setup_nhc()

        self._slurm_resource_manager.create_systemd_override_for_nofile()
        self._slurm_resource_manager.daemon_reload()
        self._stored.slurm_installed = True

        # Set application version
        self._set_slurm_version()

    def configure_munge_key(self, munge_key):
        """Configure the munge_key."""
        self._slurm_resource_manager.write_munge_key(munge_key)

    def configure_jwt_rsa(self, jwt_rsa):
        """Configure jwt_rsa."""
        self._slurm_resource_manager.write_jwt_rsa(jwt_rsa)

    def configure_slurmctld_hostname(self, slurmctld_hostname):
        """Configure the slurmctld_hostname."""
        self._slurm_resource_manager.configure_slurmctld_hostname(
            slurmctld_hostname
        )

    def slurm_config_nhc_values(self, interval=600, state='ANY,CYCLE') -> dict:
        """Craft NHC bits for slurm.conf."""
        params = {'nhc': self._slurm_resource_manager.slurm_config_nhc_values(
            interval, state)
        }
        return params

    def render_nhc_config(self, extra_configs):
        """Write NHC.conf using extra_configs."""
        self._slurm_resource_manager.render_nhc_config(extra_configs)

    def get_nhc_config(self):
        """Get the current nhc configuration."""
        return self._slurm_resource_manager.get_nhc_config()

    def render_slurm_configs(self, slurm_config) -> None:
        """Render the slurm.conf and munge key, restart slurm and munge."""
        if not type(slurm_config) == dict:
            raise TypeError("Incorrect type for config.")

        # cgroup config will not always exist. We need to check for
        # cgroup_config and only write the cgroup.conf if
        # cgroup_config exists in the slurm_config object.
        if slurm_config.get('cgroup_config'):
            cgroup_config = slurm_config['cgroup_config']
            self._slurm_resource_manager.write_cgroup_conf(cgroup_config)

        # acct_gather config will not always exist. We need to check for
        # acct_gather and only write the acct_gather.conf if we have
        # acct_gather in the slurm_config object.
        if slurm_config.get('acct_gather'):
            self._slurm_resource_manager.write_acct_gather_conf(slurm_config)

        # Write slurm.conf and restart the slurm component.
        self._slurm_resource_manager.write_slurm_config(slurm_config)

    def create_configless_systemd_override(self, host, port):
        """Proxy for slurm_ops_base.create_configless_systemd_override."""
        self._slurm_resource_manager.create_configless_systemd_override(host,
                                                                        port)

    def slurm_systemctl(self, cmd):
        """Proxy for slurm_systemctl."""
        self._slurm_resource_manager.slurm_systemctl(cmd)

    def slurm_is_active(self) -> bool:
        """Proxy for slurm_is_active."""
        return self._slurm_resource_manager.slurm_is_active

    def daemon_reload(self):
        """Proxy for daemon_reload."""
        self._slurm_resource_manager.daemon_reload()

    def _set_slurm_version(self):
        """Set the unit workload_version."""
        self._charm.unit.set_workload_version(
            self._slurm_resource_manager.slurm_version
        )

    def restart_slurm_component(self):
        """Restart slurm component."""
        self._slurm_resource_manager.restart_slurm_component()

    def restart_munged(self):
        """Restart munged."""
        self._slurm_resource_manager.restart_munged()

    def start_munged(self):
        """Start munged."""
        self._slurm_resource_manager.start_munged()

    def slurm_cmd(self, command, arg_string):
        """Run a slurm command."""
        self._slurm_resource_manager.slurm_cmd(command, arg_string)

    def generate_jwt_rsa(self) -> str:
        """Generate the jwt rsa key."""
        return self._slurm_resource_manager.generate_jwt_rsa()
