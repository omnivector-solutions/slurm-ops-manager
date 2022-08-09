"""This module provides the SlurmManager."""
import logging
import subprocess
from pathlib import Path

from ops.framework import (
    Object,
    StoredState,
)
from slurm_ops_manager import utils
from slurm_ops_manager.infiniband import Infiniband
from slurm_ops_manager.nvidia import NvidiaGPU
from slurm_ops_manager.singularity import Singularity
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
        if self._slurm_component == "slurmd":
            self.nvidia = NvidiaGPU(charm, component)
            self.singularity = Singularity(charm, component)

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

    @property
    def fluentbit_config_nhc(self) -> list:
        """Return Fluentbit configuration parameters to forward NHC logs."""
        cfg = [{"input": [("name",             "tail"),
                          ("path",             "/var/log/nhc.log"),
                          ("path_key",         "filename"),
                          ("tag",              "nhc"),
                          ("multiline.parser", "nhc")]},
               {"multiline_parser": [("name",          "nhc"),
                                     ("type",          "regex"),
                                     ("flush_timeout", "1000"),
                                     ("rule",          '"start_state"', '"/^([\d]{8} [\d:]*) (.*)/"', '"cont"'), # noqa
                                     ("rule",          '"cont"',        '"/^([^\d].*)/"',             '"cont"')]}, # noqa
               {"filter": [("name",    "record_modifier"),
                           ("match",   "nhc"),
                           ("record",  "hostname ${HOSTNAME}"),
                           ("record", f"cluster-name {self._charm.cluster_name}"),
                           ("record",  "service nhc")]}]

        if self._slurm_component == "slurmd":
            partition_cfg = ("record", f"partition-name {self._charm.get_partition_name()}")
            cfg[2]["filter"].append(partition_cfg)
        return cfg

    @property
    def fluentbit_config_slurm(self) -> list:
        """Return Fluentbit configuration parameters to forward Slurm logs."""
        if self._slurm_component == "slurmd":
            log_file = self._slurm_resource_manager._slurmd_log_file
        elif self._slurm_component == "slurmdbd":
            log_file = self._slurm_resource_manager._slurmdbd_log_file
        elif self._slurm_component == "slurmctld":
            log_file = self._slurm_resource_manager._slurmctld_log_file
        elif self._slurm_component == "slurmrestd":
            # slurmrestd does not have log files :(
            return []

        cfg = [{"input": [("name",     "tail"),
                          ("path",     log_file.as_posix()),
                          ("path_key", "filename"),
                          ("tag",      self._slurm_component),
                          ("parser",   "slurm")]},
               {"parser": [("name",        "slurm"),
                           ("format",      "regex"),
                           ("regex",      r"^\[(?<time>[^\]]*)\] (?<log>.*)$"),
                           ("time_key",    "time"),
                           ("time_format", "%Y-%m-%dT%H:%M:%S.%L")]},
               {"filter": [("name",    "record_modifier"),
                           ("match",   self._slurm_component),
                           ("record",  "hostname ${HOSTNAME}"),
                           ("record", f"cluster-name {self._charm.cluster_name}"),
                           ("record", f"service {self._slurm_component}")]}]

        if self._slurm_component == "slurmd":
            partition_cfg = ("record", f"partition-name {self._charm.get_partition_name()}")
            cfg[2]["filter"].append(partition_cfg)

        return cfg

    def get_munge_key(self) -> str:
        """Return the munge key."""
        return self._slurm_resource_manager.get_munge_key()

    def get_slurm_conf(self) -> str:
        """Return the slurm.conf."""
        return self._slurm_resource_manager.slurm_conf_path.read_text()

    def upgrade(self) -> bool:
        """Upgrade Slurm component."""
        return self._slurm_resource_manager.upgrade()

    def install(self, custom_repo: str = "") -> bool:
        """Prepare the system for slurm.

        Args:
            custom_repo: URL to a custom repository. Setting it to any value
                         superseeds the Omnivector repository.
        Returns:
            bool: True on success, False otherwise.
        """

        success = self._slurm_resource_manager.setup_slurm(custom_repo)
        if not success:
            return False

        # remove slurm.conf, as the charms setup configless mode
        if self._slurm_resource_manager.slurm_conf_path.exists():
            self._slurm_resource_manager.slurm_conf_path.unlink()

        if "slurmd" == self._slurm_component:
            success = self._slurm_resource_manager.setup_nhc()
            if not success:
                return False

        self._slurm_resource_manager.setup_logrotate()
        self._slurm_resource_manager.create_systemd_override_for_nofile()
        self._slurm_resource_manager.daemon_reload()

        # At this point, munged and slurmxxxd are enabled, we stop them to have
        # a consistent startup sequence in the charms
        self._slurm_resource_manager.slurm_systemctl("stop")
        self._slurm_resource_manager.stop_munged()

        self._stored.slurm_installed = True

        return True

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

    def render_nhc_wrapper(self, params):
        """Proxy to render the /usr/sbin/omni-nhc-wrapper script."""
        self._slurm_resource_manager.render_nhc_wrapper(params)

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
        else:
            self._slurm_resource_manager.remove_acct_gather_conf()

        # Write slurm.conf and restart the slurm component.
        self._slurm_resource_manager.write_slurm_config(slurm_config)

    def create_configless_systemd_override(self, host, port):
        """Proxy for slurm_ops_base.create_configless_systemd_override."""
        self._slurm_resource_manager.create_configless_systemd_override(host,
                                                                        port)

    def slurm_systemctl(self, cmd) -> bool:
        """Proxy for slurm_systemctl."""
        return self._slurm_resource_manager.slurm_systemctl(cmd)

    def slurm_is_active(self) -> bool:
        """Proxy for slurm_is_active."""
        return self._slurm_resource_manager.slurm_is_active

    def daemon_reload(self):
        """Proxy for daemon_reload."""
        self._slurm_resource_manager.daemon_reload()

    def restart_slurm_component(self):
        """Restart slurm component."""
        return self._slurm_resource_manager.restart_slurm_component()

    def restart_munged(self) -> bool:
        """Restart munged.

        Returns True if munge restarts successfully, and False otherwise.
        """
        return self._slurm_resource_manager.handle_restart_munged()

    def start_munged(self) -> bool:
        """Start munged.

        Returns True if munge starts successfully, and False otherwise.
        """
        return self._slurm_resource_manager.start_munged()

    def check_munged(self) -> bool:
        """Check wether munge is working correctly.

        Returns True if munge is running, and False otherwise.
        """
        return self._slurm_resource_manager.check_munged()

    def slurm_cmd(self, command, arg_string):
        """Run a slurm command."""
        return self._slurm_resource_manager.slurm_cmd(command, arg_string)

    def generate_jwt_rsa(self) -> str:
        """Generate the jwt rsa key."""
        return self._slurm_resource_manager.generate_jwt_rsa()

    def slurm_version(self) -> str:
        """Return the installed slurm version."""
        return self._slurm_resource_manager.slurm_version

    def munge_version(self) -> str:
        """Return the installed munge version."""
        return self._slurm_resource_manager.munge_version

    def nhc_version(self) -> str:
        """Return the installed nhc version."""
        return self._slurm_resource_manager.nhc_version

    def infiniband_version(self) -> str:
        """Return the installed infiniband version."""
        return self.infiniband.version

    @property
    def needs_reboot(self) -> bool:
        """Return True if the machine needs to be rebooted."""
        if Path("/var/run/reboot-required").exists():
            return True
        if Path("/bin/needs-restarting").exists(): # only on CentOS
            p = subprocess.run(["/bin/needs-restarting", "--reboothint"])
            if p.returncode == 1:
                return True

        return False
