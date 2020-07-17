#!/usr/bin/env python3
"""This module provides the SlurmInstallManager."""
import logging
import os
import subprocess
from base64 import b64decode, b64encode
from pathlib import Path
from time import sleep

from jinja2 import Environment, FileSystemLoader
from ops.framework import (
    Object,
    StoredState,
)
from ops.model import ModelError
from slurm_ops_manager.utils import get_inventory, get_hostname


logger = logging.getLogger()



class SlurmOpsManager(Object):

    _store = StoredState()


    def __init__(self, charm, component):
        self._store.set_default(slurm_installed=False)

        self._slurm_component = component
        self._resource_path = self.model.resources.fetch('slurm')
        self._is_tar = tarfile.is_tarfile(self.resource_path)

        if self._is_tar:
            self.slurm_resource_manager = SlurmTarManager(component, self._resource_path)
        else:
            self.slurm_resource_manager = SlurmSnapManager(component, self._resource_path)

    @property
    def inventory(self) -> str:
        """Return the node inventory and gpu count."""
        return get_inventory()

    @property
    def slurm_installed(self) -> bool:
        """Return the bool from the underlying _state."""
        return self._store.slurm_installed

    def get_munge_key(self) -> str:
        return self.slurm_resource_manager.get_munge_key()

    def prepare_system_for_slurm(self) -> None:
        self.slurm_resource_manager.setup_system()
        self._state.slurm_installed = True

    def render_config_and_restart(self, slurm_config) -> None:
        """Render the slurm.conf and munge key, restart slurm and munge."""
        if not type(slurm_config) == dict:
            raise TypeError("Incorrect type for config.")

        # Write slurm.conf and restart the slurm component.
        self.slurm_resource_manager.write_slurm_config(slurm_config)
        self.slurm_resource_manager.restart_slurm_component()

        # Write munge.key and restart munged.
        self.slurm_resource_manager.write_munge_key(slurm_config['munge_key'])
        self.slurm_resource_manager.restart_munged()

        if not self.slurm_resource_manager.is_active:
            raise Exception(f"SLURM {self._slurm_component}: not starting")


class SlurmOpsManagerBase:

    _CHARM_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    _TEMPLATE_DIR = _CHARM_DIR / 'templates'

    def __init__(self, component, resource_path):
        self._resource_path = resource_path

        port_map = {
            'slurmdbd': 6819,
            'slurmd': 6818,
            'slurmctld': 6817,
            'slurmrestd': 6820,
        }

        if component in ['slurmd', 'slurmctld', 'slurmrestd']:
            self._slurm_conf_template_name = 'slurm.conf.tmpl'
            self._slurm_conf = self._SLURM_CONF_DIR / 'slurm.conf'
        elif component == "slurmdbd":
            self._slurm_conf_template_name = 'slurmdbd.conf.tmpl'
            self._slurm_conf = self._SLURM_CONF_DIR / 'slurmdbd.conf'
        else:
            raise Exception(f'slurm component {component} not supported')

        self._slurm_component = component

        self.hostname = get_hostname()
        self.port = port_map[self._slurm_component]

        self._slurm_conf_template_location = \
            self._TEMPLATE_DIR / self._slurm_conf_template_name

    @property
    def is_active(self) -> bool:
        return self._slurm_systemctl("is-active") == 0

    def _slurm_systemctl(self, operation) -> int:
        """Start systemd services for slurmd."""
        if operation not in ["enable", "start", "stop", "restart", "is-active"]:
            msg = f"Unsupported systemctl command for {self._slurm_systemd_service}"
            raise Exception(msg)

        try:
            return subprocess.call([
                "systemctl",
                operation,
                self._slurm_systemd_service,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying systemd - {e}")
            return -1

    @property
    def _mail_prog(self) -> str:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def munge_socket(self):
        """Return the munge socket."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def slurm_user(self) -> str:
        """Return the slurm user."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def slurm_group(self) -> str:
        """Return the slurm group."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_systemd_service(self) -> str:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _munge_key_path(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _munged_systemd_service(self) -> str:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_conf_path(self) -> str:
        raise Exception("Inheriting object needs to define this property.")

    def setup_system(self):
        raise Exception("Inheriting object needs to define this method.")

    def write_slurm_config(self, context) -> None:
        """Render the context to a template."""

        common_config = {
            'log_file_location': self._log_file,
            'munge_socket': self._munge_socket,
            'mail_prog': self._mail_prog,
        }

        template_name = self._slurm_conf_template_name
        source = self._slurm_conf_template_location
        target = self._slurm_conf_path

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

    def restart_slurm_component(self):
        self._slurm_systemctl("restart")

    def write_munge_key(self, munge_key):
        key = b64decode(munge_key.encode())
        self._munge_key_path.write_bytes(key)

    def restart_munged(self):
        try:
            return subprocess.call([
                "systemctl",
                "restart",
                self._munged_systemd_service,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying systemd - {e}")
            return -1

        raise Exception("Inheriting object needs to define this method.")

    def get_munge_key(self) -> str:
        """Read, encode, decode and return the munge key."""
        munge_key = self._munge_key_path.read_bytes()
        return b64encode(munge_key).decode()


class SlurmTarManager(SlurmOpsManagerBase):

    _SLURM_CONF_DIR = Path('/etc/slurm')
    _SLURM_PID_DIR = Path('/srv/slurm')
    _SLURM_SBIN_DIR = Path('/usr/local/sbin')
    _SLURM_SYSCONFIG_DIR = Path("/etc/sysconfig")
    _SLURM_LOG_DIR = Path('/var/log/slurm')
    _SLURM_SPOOL_DIR = Path("/var/spool/slurmd")
    _SLURM_STATE_DIR = Path("/var/lib/slurmd")
    _SLURM_PLUGIN_DIR = Path("/usr/local/lib/slurm")

    _SLURM_UID = 995
    _SLURM_GID = 995

    _SLURM_TMP_RESOURCE = "/tmp/slurm-resource"

    def __init__(self, component, resource_path):
        super().__init__(component, resource_path)
        self._source_systemd_template = \
            self._TEMPLATE_DIR / f'{self._slurm_component}.service'
        self._target_systemd_template = \
            Path(f'/etc/systemd/system/{self._slurm_component}.service')
        self._log_file = self._SLURM_LOG_DIR / f'{self._slurm_component}.log'
        self._environment_file = \
            self._SLURM_SYSCONFIG_DIR / f'{self._slurm_component}'


    @property
    def _mail_prog(self) -> str:
        return "/usr/bin/mail"

    @property
    def _slurm_systemd_service(self) -> str:
        return self._slurm_component

    @property
    def _munge_key_path(self) -> str:
        return Path("/etc/munge/munge.key")

    @property
    def munge_socket(self) -> Path:
        """Return the munge socket."""
        return Path("/var/run/munge/munge.socket.2")

    @property
    def _munged_systemd_service(self) -> str:
        return f"munge"

    @property
    def _slurm_conf_path(self) -> str:
        return "/etc/slurm/slurm.conf"

    @property
    def slurm_user(self) -> str:
        """Return the slurm user."""
        return "slurm"

    @property
    def slurm_group(self) -> str:
        """Return the slurm group."""
        return "slurm"

    @property
    def _slurm_conf_path(self) -> str:
        return "/etc/slurm/slurm.conf"

    def setup_system(self) -> None:
        """Prepare the system for slurm.

        * create slurm user/group
        * create filesystem for slurm
        * provision slurm resource
        """
        self._install_os_deps()
        self._create_slurm_user_and_group()
        self._prepare_filesystem()
        self._create_environment_files()

        self._provision_slurm_resource()

        self._set_ld_library_path()
        self._setup_systemd()

    def _install_os_deps(self) -> None:
        try:
            subprocess.call([
                'apt',
                'install',
                'libmunge2',
                'libmysqlclient-dev',
                'munge',
                '-y',
            ])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def _create_slurm_user_and_group(self) -> None:
        """Create the slurm user and group."""
        try:
            subprocess.call([
                "groupadd",
                "-r",
                f"--gid={self._SLURM_GID}",
                self.slurm_user,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error creating {self._SLURM_GROUP} - {e}")

        try:
            subprocess.call([
                "useradd",
                "-r",
                "-g",
                self.slurm_group,
                f"--uid={self._SLURM_UID}",
                self.slurm_user,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error creating {self._SLURM_USER} - {e}")

    def _prepare_filesystem(self) -> None:
        """Create the needed system directories needed by slurm."""
        slurm_dirs = [
            self._SLURM_CONF_DIR,
            self._SLURM_LOG_DIR,
            self._SLURM_PID_DIR,
            self._SLURM_SYSCONFIG_DIR,
            self._SLURM_SPOOL_DIR,
            self._SLURM_STATE_DIR,
        ]
        for slurm_dir in slurm_dirs:
            slurm_dir.mkdir(parents=True, exist_ok=True)
            self._chown_slurm_user_and_group_recursive(str(slurm_dir))

        slurm_state_files = [
            "/var/lib/slurmd/node_state",
            "/var/lib/slurmd/front_end_state",
            "/var/lib/slurmd/job_state",
            "/var/lib/slurmd/resv_state",
            "/var/lib/slurmd/trigger_state",
            "/var/lib/slurmd/assoc_mgr_state",
            "/var/lib/slurmd/assoc_usage",
            "/var/lib/slurmd/qos_usage",
            "/var/lib/slurmd/fed_mgr_state",
        ]
        for slurmd_file in slurm_state_files:
            Path(slurmd_file).touch()
        self._chown_slurm_user_and_group_recursive('/var/lib/slurmd')

    def _chown_slurm_user_and_group_recursive(self, slurm_dir) -> None:
        """Recursively chown filesystem location to slurm user/slurm group."""
        try:
            subprocess.call([
                "chown",
                "-R",
                f"{self.slurm_user}:{self.slurm_group}",
                slurm_dir,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error chowning {slurm_dir} - {e}")

    def _create_environment_files(self) -> None:
        slurm_conf = f"\nSLURM_CONF={str(self._slurm_conf_path)}\n"
        self._environment_file.write_text(slurm_conf)
        with open("/etc/environment", 'a') as f:
            f.write(slurm_conf)

    def _provision_slurm_resource(self) -> None:
        """Provision the slurm resource."""
        try:
            resource_path = self.model.resources.fetch('slurm')
        except ModelError as e:
            logger.error(
                f"Resource could not be found when executing: {e}",
                exc_info=True,
            )

        try:
            subprocess.call([
                "tar",
                "-xzvf",
                resource_path,
                f"--one-top-level={self._SLURM_TMP_RESOURCE}",
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error untaring slurm bins - {e}")

        # Wait on the existence of slurmd bin to verify that the untaring
        # of the slurm.tar.gz resource has completed before moving on.
        while not Path(f"{self._SLURM_TMP_RESOURCE}/sbin/slurmd").exists():
            sleep(1)

        for slurm_resource_dir in ['bin', 'sbin', 'lib', 'include']:
            cmd = (
                f"cp -R {self._SLURM_TMP_RESOURCE}/{slurm_resource_dir}/* "
                f"/usr/local/{slurm_resource_dir}/"
            )
            try:
                subprocess.call(cmd, shell=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error provisioning fs - {e}")

    def _set_ld_library_path(self) -> None:
        """Set the LD_LIBRARY_PATH."""
        Path('/etc/ld.so.conf.d/slurm.conf').write_text("/usr/local/lib/slurm")
        try:
            subprocess.call(["ldconfig"])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error setting LD_LIBRARY_PATH - {e}")

    def _setup_systemd(self) -> None:
        """Preforms setup the systemd service."""
        try:
            subprocess.call([
                "cp",
                self._source_systemd_template,
                self._target_systemd_template
            ])
            subprocess.call([
                "systemctl",
                "daemon-reload",
            ])
            self._slurm_systemctl("enable")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error setting up systemd - {e}")


class SlurmSnapManager(SlurmOpsManagerBase):

    _SLURM_CONF_DIR = Path("/var/snap/slurm/common/etc/slurm")
    _SLURM_LOG_DIR = Path('/var/snap/slurm/common/log/slurm')
    _SLURM_SPOOL_DIR = Path("/var/snap/slurm/common/var/spool/slurm/d")
    _SLURM_STATE_DIR = Path("var/snap/slurm/common/var/spool/slurm/ctld")
    _SLURM_PLUGIN_DIR = Path("/snap/slurm/current/lib/slurm")

    def __init__(self, component, resource_path):
        super().__init__(component, resource_path)
        self._log_file = self._SLURM_LOG_DIR / f'{self._slurm_component}.log'

    @property
    def _mail_prog(self) -> str:
        return "/snap/slurm/current/usr/bin/mail.mailutils"

    @property
    def slurm_user(self) -> str:
        """Return the slurm user."""
        return "root"

    @property
    def slurm_group(self) -> str:
        """Return the slurm group."""
        return "root"

    @property
    def _slurm_systemd_service(self) -> str:
        return f"snap.slurm.{self._slurm_component}"

    @property
    def _munge_key_path(self) -> str:
        return Path("/var/snap/slurm/common/etc/munge/munge.key")

    @property
    def munge_socket(self) -> Path:
        """Return the munge socket."""
        return Path("/tmp/munged.socket.2")

    @property
    def _munged_systemd_service(self) -> str:
        return f"snap.slurm.munged"

    @property
    def _slurm_conf_path(self) -> str:
        return "/var/snap/slurm/common/etc/slurm/slurm.conf"

    def setup_system(self) -> None:
        self._provision_slurm_snap()
