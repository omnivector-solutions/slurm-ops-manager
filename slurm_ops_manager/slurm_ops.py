#!/usr/bin/env python3
"""This module provides the SlurmInstallManager."""
import logging
import os
import subprocess
import sys
import tarfile
from base64 import b64decode, b64encode
from pathlib import Path
from time import sleep

from jinja2 import Environment, FileSystemLoader
from ops.framework import (
    Object,
    StoredState,
)
from ops.model import ModelError
from slurm_ops_manager.utils import get_hostname, get_inventory

logger = logging.getLogger()


class SlurmOpsManager(Object):
    """SlurmOpsManager."""

    _state = StoredState()

    def __init__(self, charm, component):
        super().__init__(charm, component)
        """Set the initial attribute values."""
        self._state.set_default(slurm_installed=False)
        self._state.set_default(slurm_version_set=False)

        self._slurm_component = component
        self._charm = charm
        self._resource_path = self.model.resources.fetch('slurm')
        self._is_tar = tarfile.is_tarfile(self._resource_path)

        if self._is_tar:
            self._slurm_resource_manager = \
                SlurmTarManager(component, self._resource_path)
        else:
            self._slurm_resource_manager = \
                SlurmSnapManager(component, self._resource_path)
    @property
    def hostname(self):
        return self._slurm_resource_manager.hostname

    @property
    def port(self):
        return self._slurm_resource_manager.port

    @property
    def inventory(self) -> str:
        """Return the node inventory and gpu count."""
        return get_inventory()

    @property
    def slurm_installed(self) -> bool:
        """Return the bool from the underlying _state."""
        return self._state.slurm_installed

    def get_munge_key(self) -> str:
        """Return the munge key."""
        return self._slurm_resource_manager.get_munge_key()

    def prepare_system_for_slurm(self) -> None:
        """Prepare the system for slurm."""
        self._slurm_resource_manager.setup_system()
        self._state.slurm_installed = True

    def render_config_and_restart(self, slurm_config) -> None:
        """Render the slurm.conf and munge key, restart slurm and munge."""
        if not type(slurm_config) == dict:
            raise TypeError("Incorrect type for config.")

        # Write slurm.conf and restart the slurm component.
        self._slurm_resource_manager.write_slurm_config(slurm_config)
        self._slurm_resource_manager.restart_slurm_component()

        # Write munge.key and restart munged.
        self._slurm_resource_manager.write_munge_key(slurm_config['munge_key'])
        self._slurm_resource_manager.restart_munged()

        if not self._slurm_resource_manager.slurm_is_active:
            raise Exception(f"SLURM {self._slurm_component}: not starting")
        else:
            if not self._state.slurm_version_set:
                if self._slurm_resource_manager.slurm_conf_path.exists():
                    self._charm.unit.set_workload_version(
                        self._slurm_resource_manager.slurm_version
                    )
                    self._state.slurm_version_set = True


class SlurmOpsManagerBase:
    """Base class for slurm ops."""

    _CHARM_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    _TEMPLATE_DIR = _CHARM_DIR / 'templates'

    def __init__(self, component, resource_path):
        """Set the initial values for attributes in the base class."""
        self._resource_path = resource_path

        port_map = {
            'slurmdbd': 6819,
            'slurmd': 6818,
            'slurmctld': 6817,
            'slurmrestd': 6820,
        }

        self._slurm_cmds = [
            "sacct",
            "sacctmgr",
            "salloc",
            "sattach",
            "sbatch",
            "sbcast",
            "scancel",
            "scontrol",
            "sdiag",
            "sinfo",
            "sprio",
            "squeue",
            "sreport",
            "srun",
            "sshare",
            "sstat",
            "strigger",
        ]

        if component in ['slurmd', 'slurmctld', 'slurmrestd']:
            self._slurm_conf_template_name = 'slurm.conf.tmpl'
            self._slurm_conf_path = self._slurm_conf_dir / 'slurm.conf'
        elif component == "slurmdbd":
            self._slurm_conf_template_name = 'slurmdbd.conf.tmpl'
            self._slurm_conf_path = self._slurm_conf_dir / 'slurmdbd.conf'
        else:
            raise Exception(f'slurm component {component} not supported')

        self._slurm_component = component

        self._slurmd_log_file = self._slurm_log_dir / 'slurmd.log'
        self._slurmctld_log_file = self._slurm_log_dir / 'slurmctld.log'
        self._slurmdbd_log_file = self._slurm_log_dir / 'slurmdbd.log'

        self._slurmd_pid_file = self._slurm_pid_dir / 'slurmd.pid'
        self._slurmctld_pid_file = self._slurm_pid_dir / 'slurmctld.pid'
        self._slurmdbd_pid_file = self._slurm_pid_dir / 'slurmdbd.pid'

        self._hostname = get_hostname()
        self._port = port_map[self._slurm_component]

        self._slurm_conf_template_location = \
            self._TEMPLATE_DIR / self._slurm_conf_template_name

    @property
    def hostname(self) -> str:
        return self._hostname

    @property
    def port(self) -> str:
        return self._port

    @property
    def slurm_conf_path(self) -> Path:
        return self._slurm_conf_path

    @property
    def slurm_is_active(self) -> bool:
        """Return True if the slurm component is running."""
        return self._slurm_systemctl("is-active") == 0

    def _slurm_systemctl(self, operation) -> int:
        """Start systemd services for slurmd."""
        supported_systemctl_cmds = [
            "enable",
            "start",
            "stop",
            "restart",
            "is-active",
        ]

        if operation not in supported_systemctl_cmds:
            msg = f"Unsupported systemctl command: {operation}"
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
    def _slurm_conf_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_spool_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_state_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_plugin_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_log_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_plugstack_conf(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_plugstack_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_pid_dir(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _mail_prog(self) -> Path:
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _munge_socket(self) -> Path:
        """Return the munge socket."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_user(self) -> str:
        """Return the slurm user."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_group(self) -> str:
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

    def setup_system(self):
        """Preform the install and setup operations."""
        raise Exception("Inheriting object needs to define this method.")

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        raise Exception("Inheriting object needs to define this property.")

    def write_slurm_config(self, context) -> None:
        """Render the context to a template, adding in common configs."""
        common_config = {
            'munge_socket': str(self._munge_socket),
            'mail_prog': str(self._mail_prog),
            'slurm_state_dir': str(self._slurm_state_dir),
            'slurm_spool_dir': str(self._slurm_spool_dir),
            'slurm_plugin_dir': str(self._slurm_plugin_dir),
            'slurmdbd_log_file': str(self._slurmdbd_log_file),
            'slurmd_log_file': str(self._slurmd_log_file),
            'slurmctld_log_file': str(self._slurmctld_log_file),
            'slurmdbd_pid_file': str(self._slurmdbd_pid_file),
            'slurmd_pid_file': str(self._slurmd_pid_file),
            'slurmctld_pid_file': str(self._slurmctld_pid_file),
            'slurm_plugstack_conf': str(self._slurm_plugstack_conf),
            'slurm_user': str(self._slurm_user),
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

        target.write_text(
            rendered_template.render(
                {**context, **common_config}
            )
        )

    def restart_slurm_component(self):
        """Restart the slurm component."""
        self._slurm_systemctl("restart")

    def write_munge_key(self, munge_key):
        """Write the munge key."""
        key = b64decode(munge_key.encode())
        self._munge_key_path.write_bytes(key)

    def get_munge_key(self) -> str:
        """Read, encode, decode and return the munge key."""
        munge_key = self._munge_key_path.read_bytes()
        return b64encode(munge_key).decode()

    def restart_munged(self):
        """Restart munged."""
        try:
            return subprocess.call([
                "systemctl",
                "restart",
                self._munged_systemd_service,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying systemd - {e}")
            return -1


class SlurmTarManager(SlurmOpsManagerBase):
    """Operations for slurm tar resource."""

    _SLURM_SBIN_DIR = Path('/usr/local/sbin')
    _SLURM_SYSCONFIG_DIR = Path("/etc/sysconfig")

    _SLURM_UID = 995
    _SLURM_GID = 995

    _SLURM_TMP_RESOURCE = "/tmp/slurm-resource"

    def __init__(self, component, resource_path):
        """Set initial class attribute values."""
        super().__init__(component, resource_path)
        self._source_systemd_template = \
            self._TEMPLATE_DIR / f'{self._slurm_component}.service'
        self._target_systemd_template = \
            Path(f'/etc/systemd/system/{self._slurm_component}.service')
        self._environment_file = \
            self._SLURM_SYSCONFIG_DIR / f'{self._slurm_component}'

    @property
    def _slurm_conf_dir(self) -> Path:
        return Path("/etc/slurm")

    @property
    def _slurm_spool_dir(self) -> Path:
        return Path("/var/spool/slurmd")

    @property
    def _slurm_state_dir(self) -> Path:
        return Path("/var/lib/slurmd")

    @property
    def _slurm_plugin_dir(self) -> Path:
        return Path("/usr/local/lib/slurm")

    @property
    def _slurm_log_dir(self) -> Path:
        return Path("/var/log/slurm")

    @property
    def _slurm_pid_dir(self) -> Path:
        return Path("/srv/slurm")

    @property
    def _slurm_plugstack_dir(self) -> Path:
        return Path("/etc/slurm/plugstack.d")

    @property
    def _slurm_plugstack_conf(self) -> Path:
        return Path("/etc/slurm/plugstack.d/plugstack.conf")

    @property
    def _mail_prog(self) -> Path:
        return Path("/usr/bin/mail")

    @property
    def _slurm_systemd_service(self) -> str:
        return self._slurm_component

    @property
    def _munge_key_path(self) -> str:
        return Path("/etc/munge/munge.key")

    @property
    def _munge_socket(self) -> Path:
        """Return the munge socket."""
        return Path("/var/run/munge/munge.socket.2")

    @property
    def _munged_systemd_service(self) -> str:
        return "munge"

    @property
    def _slurm_user(self) -> str:
        """Return the slurm user."""
        return "slurm"

    @property
    def _slurm_group(self) -> str:
        """Return the slurm group."""
        return "slurm"

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        os.environ['SLURM_CONF'] = str(self._slurm_conf_path)
        try:
            return subprocess.check_output(
                [self._slurm_component, "-V"]
            ).decode().strip().split()[1]
        except subprocess.CalledProcessError as e:
            print(f"Cannot get slurm version - {e}")
            sys.exit(-1)

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
                self._slurm_user,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error creating {self._slurm_group} - {e}")

        try:
            subprocess.call([
                "useradd",
                "-r",
                "-g",
                self._slurm_group,
                f"--uid={self._SLURM_UID}",
                self._slurm_user,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error creating {self._slurm_user} - {e}")

    def _prepare_filesystem(self) -> None:
        """Create the needed system directories needed by slurm."""
        slurm_dirs = [
            self._slurm_plugstack_dir, 
            self._slurm_conf_dir,
            self._slurm_log_dir,
            self._slurm_pid_dir,
            self._slurm_spool_dir,
            self._slurm_state_dir,
            self._SLURM_SYSCONFIG_DIR,
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
                f"{self._slurm_user}:{self._slurm_group}",
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
            subprocess.call([
                "tar",
                "-xzvf",
                self._resource_path,
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
    """Snap operations manager."""

    def __init__(self, component, resource_path):
        """Set initial attribute values."""
        super().__init__(component, resource_path)

    @property
    def _slurm_conf_dir(self) -> Path:
        return Path("/var/snap/slurm/common/etc/slurm")

    @property
    def _slurm_spool_dir(self) -> Path:
        return Path("/var/snap/slurm/common/var/spool/slurm/d")

    @property
    def _slurm_state_dir(self) -> Path:
        return Path("/var/snap/slurm/common/var/spool/slurm/ctld")

    @property
    def _slurm_plugin_dir(self) -> Path:
        return Path("/snap/slurm/current/lib/slurm")

    @property
    def _slurm_log_dir(self) -> Path:
        return Path("/var/snap/slurm/common/var/log/slurm")

    @property
    def _slurm_pid_dir(self) -> Path:
        return Path("/tmp")

    @property
    def _mail_prog(self) -> Path:
        return Path("/snap/slurm/current/usr/bin/mail.mailutils")

    @property
    def _munge_key_path(self) -> Path:
        return Path("/var/snap/slurm/common/etc/munge/munge.key")

    @property
    def _slurm_plugstack_dir(self) -> Path:
        return Path("/etc/slurm/plugstack.d")

    @property
    def _slurm_plugstack_conf(self) -> Path:
        return "/var/snap/slurm/common/etc/slurm/plugstack.d/plugstack.conf"

    @property
    def _slurm_user(self) -> str:
        """Return the slurm user."""
        return "root"

    @property
    def _slurm_group(self) -> str:
        """Return the slurm group."""
        return "root"

    @property
    def _slurm_systemd_service(self) -> str:
        return f"snap.slurm.{self._slurm_component}"

    @property
    def _munge_socket(self) -> Path:
        """Return the munge socket."""
        return Path("/tmp/munged.socket.2")

    @property
    def _munged_systemd_service(self) -> str:
        return "snap.slurm.munged"

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        try:
            return subprocess.check_output(['slurm.version']).decode().strip()
        except subprocess.CalledProcessError as e:
            print(f"Cannot get slurm version - {e}")
            sys.exit(-1)

    def setup_system(self) -> None:
        """Install the slurm snap, set the snap.mode, create the aliases."""
        # Install the slurm snap
        try:
            subprocess.call([
                "snap",
                "install",
                self._resource_path,
                "--dangerous",
                "--classic",
            ])
        except subprocess.CalledProcessError as e:
            print(f"Error installing slurm snap - {e}")

        # Set the snap.mode
        try:
            subprocess.call([
                "snap",
                "set",
                "slurm",
                f"snap.mode={self._slurm_component}",
            ])
        except subprocess.CalledProcessError as e:
            print(f"Error setting snap.mode - {e}")

        # Create the aliases for the slurm cmds
        for cmd in self._slurm_cmds:
            try:
                subprocess.call([
                    "snap",
                    "alias",
                    f"slurm.{cmd}",
                    cmd,
                ])
            except subprocess.CalledProcessError as e:
                print(f"Cannot create snap alias for: {cmd} - {e}")
