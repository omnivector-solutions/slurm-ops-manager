from time import sleep
from jinja2 import Environment, FileSystemLoader
import sys
import tarfile
from base64 import b64decode, b64encode
import socket
import sys
import subprocess
import logging
import os
from Pathlib import path

logger = logging.getLogger()


class TarInstall:
    """Slurm installation of lifecycle ops."""

    _CHARM_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    _TEMPLATE_DIR = _CHARM_DIR / 'templates'

    _SLURM_CONF_DIR = Path('/etc/slurm')
    _SLURM_PID_DIR = Path('/srv/slurm')
    _SLURM_LOG_DIR = Path('/var/log/slurm')
    _SLURM_SBIN_DIR = Path('/usr/local/sbin')
    _SLURM_SYSCONFIG_DIR = Path("/etc/sysconfig")
    _SLURM_SPOOL_DIR = Path("/var/spool/slurmd")
    _SLURM_STATE_DIR = Path("/var/lib/slurmd")
    _SLURM_PLUGIN_DIR = Path("/usr/local/lib/slurm")

    _SLURM_USER = "slurm"
    _SLURM_UID = 995
    _SLURM_GROUP = "slurm"
    _SLURM_GID = 995
    _SLURM_TMP_RESOURCE = "/tmp/slurm-resource"
    _MUNGE_KEY_PATH = Path("/etc/munge/munge.key")

    def __init__(self, component, res_path):
        """Determine values based on slurm component."""
        super().__init__(charm, component)

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
        
        self._res_path = res_path
        self._slurm_component = component

        self.hostname = socket.gethostname().split(".")[0]
        self.port = port_map[component]

        self._slurm_conf_template_location = \
            self._TEMPLATE_DIR / self._slurm_conf_template_name
        self._source_systemd_template = \
            self._TEMPLATE_DIR / f'{self._slurm_component}.service'
        self._target_systemd_template = \
            Path(f'/etc/systemd/system/{self._slurm_component}.service')

        self._log_file = self._SLURM_LOG_DIR / f'{self._slurm_component}.log'
        self._daemon = self._SLURM_SBIN_DIR / f'{self._slurm_component}'
        self._environment_file = \
            self._SLURM_SYSCONFIG_DIR / f'{self._slurm_component}'
    
    def prepare_system_for_slurm(self) -> None:
        """Prepare the system for slurm.
        * create slurm user/group
        * create filesystem for slurm
        * provision slurm resource
        """
        self._install_os_deps()
        self._create_slurm_user_and_group()
        self._prepare_filesystem()
        self._create_environment_files()

        self._install_munge()
        self._provision_slurm_resource()

        self._set_ld_library_path()

    def _install_os_deps(self) -> None:
        try:
            subprocess.call([
                'apt',
                'install',
                'libmunge2',
                'libmysqlclient-dev',
                '-y',
            ])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def _install_munge(self) -> None:
        try:
            subprocess.call(["apt", "install", "munge", "-y"])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def _write_munge_key_and_restart(self, munge_key) -> None:
        key = b64decode(munge_key.encode())
        self._MUNGE_KEY_PATH.write_bytes(key)
        try:
            subprocess.call(["service", "munge", "restart"])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def get_munge_key(self) -> str:
        """Read, encode, decode and return the munge key."""
        munge_key = self._MUNGE_KEY_PATH.read_bytes()
        return b64encode(munge_key).decode()

    def _create_environment_files(self) -> None:
        slurm_conf = f"\nSLURM_CONF={str(self._slurm_conf)}\n"
        self._environment_file.write_text(slurm_conf)
        with open("/etc/environment", 'a') as f:
            f.write(slurm_conf)

    def _chown_slurm_user_and_group_recursive(self, slurm_dir) -> None:
        """Recursively chown filesystem location to slurm user/slurm group."""
        try:
            subprocess.call([
                "chown",
                "-R",
                f"{self._SLURM_USER}:{self._SLURM_GROUP}",
                slurm_dir,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error chowning {slurm_dir} - {e}")

    def _create_slurm_user_and_group(self) -> None:
        """Create the slurm user and group."""
        try:
            subprocess.call([
                "groupadd",
                "-r",
                f"--gid={self._SLURM_GID}",
                self._SLURM_USER,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error creating {self._SLURM_GROUP} - {e}")

        try:
            subprocess.call([
                "useradd",
                "-r",
                "-g",
                self._SLURM_GROUP,
                f"--uid={self._SLURM_UID}",
                self._SLURM_USER,
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

    def _provision_slurm_resource(self) -> None:
        """Provision the slurm resource."""
        try:
            resource_path = self._res_path 
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

