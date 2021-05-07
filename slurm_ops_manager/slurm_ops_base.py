#!/usr/bin/env python3
"""This module provides the SlurmInstallManager."""
import logging
import os
import shlex
import subprocess

from base64 import b64decode, b64encode
from pathlib import Path
from shutil import rmtree

from Crypto.PublicKey import RSA

from jinja2 import Environment, FileSystemLoader
from slurm_ops_manager.utils import get_hostname
from slurm_ops_manager.utils import operating_system


logger = logging.getLogger()


class SlurmOpsManagerBase:
    """Base class for slurm ops."""

    def __init__(self, component):
        """Set the initial values for attributes in the base class."""
        self._template_dir = Path(
            os.path.dirname(os.path.abspath(__file__))) / 'templates'

        port_map = {
            'slurmctld': "6817",
            'slurmd': "6818",
            'slurmdbd': "6819",
            'slurmrestd': "6820",
        }

        # Note: missing slurm cmds
        # Body: Need to extend this list to include all slurm user cmds
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

        logger.debug(f'__init__(): component={component}')

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

        # Note: Come back to mitigate this configless cruft
        self._slurmctld_parameters = ["enable_configless"]

        self._hostname = get_hostname()
        self._port = port_map[self._slurm_component]

        self._slurm_conf_template_location = \
            self._template_dir / self._slurm_conf_template_name

    @property
    def hostname(self) -> str:
        """Return the hostname."""
        return self._hostname

    @property
    def port(self) -> str:
        """Return the port."""
        return self._port

    @property
    def slurm_conf_path(self) -> Path:
        """Return the slurm conf path."""
        return self._slurm_conf_path

    @property
    def slurm_is_active(self) -> bool:
        """Return True if the slurm component is running."""
        try:
            cmd = f"systemctl is-active {self._slurm_systemd_service}"
            r = subprocess.check_output(shlext.split(cmd))
            return 'active' == r.decode().strip().lower()
        except subprocess.CalledProcessError as e:
            return False
        return False

    def slurm_systemctl(self, operation):
        """Start systemd services for slurmd."""
        supported_systemctl_cmds = [
            "enable",
            "start",
            "stop",
            "restart",
            "daemon-reload",
        ]

        if operation not in supported_systemctl_cmds:
            msg = f"Unsupported systemctl command: {operation}"
            logger.error(msg)
            raise Exception(msg)
        try:
            subprocess.call([
                "systemctl",
                operation,
                self._slurm_systemd_service,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running {operation} - {e}")

    @property
    def _slurm_bin_dir(self) -> Path:
        """Return the directory where the slurm bins live."""
        return Path("/usr/bin")

    @property
    def _slurm_conf_dir(self) -> Path:
        """Return the directory for Slurm configuration files."""
        return Path("/etc/slurm")

    @property
    def _slurm_spool_dir(self) -> Path:
        """Return the directory for slurmd's state information."""
        return Path("/var/spool/slurmd")

    @property
    def _slurm_state_dir(self) -> Path:
        """Return the directory for slurmctld's state information."""
        return Path("/var/spool/slurmctld")

    @property
    def _slurm_plugin_dir(self) -> Path:
        """Return the path for the Slurm plugin dir."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _slurm_log_dir(self) -> Path:
        """Return the directory for Slurm logs."""
        return Path("/var/log/slurm")

    @property
    def _slurm_pid_dir(self) -> Path:
        """Return the directory for Slurm PID file."""
        return Path("/var/run/")

    @property
    def _jwt_rsa_key_file(self) -> Path:
        """Return the jwt rsa key file path."""
        return self._slurm_state_dir / "jwt_hs256.key"

    @property
    def _mail_prog(self) -> Path:
        """Return the full path for the mailing program."""
        raise Exception("Inheriting object needs to define this property.")

    @property
    def _munge_key_path(self) -> Path:
        """Return the full path to the munge key."""
        return Path("/etc/munge/munge.key")

    @property
    def _munge_socket(self) -> Path:
        """Return the munge socket."""
        return Path("/var/run/munge/munge.socket.2")

    @property
    def _munged_systemd_service(self) -> str:
        """Return the name of the Munge Systemd unit file."""
        return "munge"

    @property
    def _munge_user(self) -> str:
        """The user for munge daemon."""
        return "munge"

    @property
    def _munge_group(self) -> str:
        """The group for munge daemon."""
        return "munge"

    @property
    def _slurm_plugstack_dir(self) -> Path:
        """Return the directory to the SPANK plugins."""
        return Path("/etc/slurm/plugstack.d")

    @property
    def _slurm_plugstack_conf(self) -> Path:
        """Return the full path to the SPANK configuration file."""
        return self._slurm_plugstack_dir / 'plugstack.conf' # TODO check this on CentOS

    @property
    def _slurm_systemd_service(self) -> str:
        """Return the Slurm systemd unit file."""
        return f"{self._slurm_component}"

    @property
    def _slurm_user(self) -> str:
        """Return the slurm user."""
        return "slurm"

    @property
    def _slurm_user_id(self) -> str:
        """Return the slurm user ID."""
        return "64030"

    @property
    def _slurm_group(self) -> str:
        """Return the slurm group."""
        return "slurm"

    @property
    def _slurm_group_id(self) -> str:
        """Return the slurm group ID."""
        return "64030"

    @property
    def _slurmd_user(self) -> str:
        """Return the slurmd user."""
        return "root"

    @property
    def _slurmd_group(self) -> str:
        """Return the slurmd group."""
        return "root"

    @property
    def slurm_component(self) -> str:
        """Return the slurm component we are."""
        return self._slurm_component

    def create_systemd_override_for_nofile(self):
        """Create the override.conf file for slurm systemd service."""
        systemd_override_dir = Path(
            f"/etc/systemd/system/{self._slurm_systemd_service}.d"
        )
        if not systemd_override_dir.exists():
            systemd_override_dir.mkdir(exist_ok=True)

        systemd_override_conf = systemd_override_dir / 'override.conf'
        systemd_override_conf_tmpl = self._template_dir / 'override.conf'

        systemd_override_conf.write_text(
            systemd_override_conf_tmpl.read_text()
        )

    def upgrade(self):
        """Preform upgrade-charm operations."""
        raise Exception("Inheriting object needs to define this method.")

    def setup_slurm(self):
        """Install and setup Slurm and its dependencies."""
        raise Exception("Inheriting object needs to define this method.")

    @property
    def nhc_version(self) -> str:
        """Return NHC version."""
        return "1.4.2-omni-1.0"

    def _install_nhc_from_git(self) -> None:
        """Install NHC from Omnivector fork."""
        version = self.nhc_version
        src = f"https://codeload.github.com/omnivector-solutions/nhc/tar.gz/refs/tags/{version}"

        logger.info(f"#### downloading and installing NHC {version}")

        base_path = Path("/tmp/nhc")
        full_path = base_path / f"nhc-{version}"
        nhc_tar = base_path / "nhc.tar.gz"

        # cleanup old installations
        if base_path.exists():
            rmtree(base_path)
        base_path.mkdir()

        cmd = f"curl -o {nhc_tar} -s {src}".split()
        subprocess.run(cmd)

        cmd = f"tar --extract --directory {base_path} --file {nhc_tar}".split()
        subprocess.run(cmd)

        if operating_system() == 'ubuntu':
            libdir = "/usr/lib"
        else:
            libdir = "/usr/libexec"

        # NOTE: this requires make. We install it using the dispatch file in
        # the slurmd charm.
        try:
            locale = {'LC_ALL':'C', 'LANG':'C.UTF-8'}
            cmd = f"./autogen.sh --prefix=/usr --sysconfdir=/etc \
                                 --libexecdir={libdir}".split()
            logger.info(f'##### NHC - running autogen')
            r = subprocess.run(cmd, cwd=full_path, env=locale,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
            logger.debug(f'##### autogen: {r.stdout.decode()}')
            r.check_returncode()

            logger.info(f'##### NHC - running tests')
            r = subprocess.run(["make", "test"], cwd=full_path,
                               env=locale, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
            logger.debug(f'##### NHC make test: {r.stdout.decode()}')
            r.check_returncode()
            if not "tests passed" in r.stdout.decode():
                logger.error(f"##### NHC tests failed")
                logger.error(f"##### Error installing NHC")
                return -1

            logger.info(f'##### NHC - installing')
            r = subprocess.run(["make", "install"], cwd=full_path,
                               env=locale, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
            logger.debug(f'##### NHC make install: {r.stdout.decode()}')
            r.check_returncode()
        except subprocess.CalledProcessError as e:
            logger.error(f"#### Error installing NHC: {e.cmd}")
            return -1

        logger.info("#### NHC succesfully installed")

    def render_nhc_config(self, extra_configs=None) -> None:
        """Render basic NHC.conf during installation."""
        target = Path('/etc/nhc/nhc.conf')

        context = {'munge_user': self._munge_user,
                   'extra_configs': extra_configs}

        environment = Environment(loader=FileSystemLoader(self._template_dir))
        template = environment.get_template('nhc.conf.tmpl')
        try:
            target.write_text(template.render(context))
        except FileNotFoundError as e:
            logger.error(f"#### Error rendering NHC.conf: {e}")
            return -1

    def get_nhc_config(self) -> None:
        """Get current nhc.conf."""
        target = Path('/etc/nhc/nhc.conf')
        if target.exists():
            return target.read_text()
        else:
            return f"{target} not found."

    def setup_nhc(self) -> None:
        """Install NHC and its dependencies."""
        self._install_nhc_from_git()
        self.render_nhc_config()

    def slurm_config_nhc_values(self, interval=600, state='ANY,CYCLE'):
        """NHC parameters for slurm.conf."""
        return {'nhc_bin': '/usr/sbin/nhc-wrapper',
                'health_check_interval': interval,
                'health_check_node_state': state}

    @property
    def slurm_version(self) -> str:
        """Return slurm version."""
        raise Exception("Inheriting object needs to define this property.")

    def write_acct_gather_conf(self, context) -> None:
        """Render the acct_gather.conf."""
        template_name = 'acct_gather.conf.tmpl'
        source = self._template_dir / template_name
        target = self._slurm_conf_dir / 'acct_gather.conf'

        if not type(context) == dict:
            raise TypeError("Incorrect type for config.")

        if not source.exists():
            raise FileNotFoundError(
                "The acct_gather template cannot be found."
            )

        rendered_template = Environment(
            loader=FileSystemLoader(str(self._template_dir))
        ).get_template(template_name)

        if target.exists():
            target.unlink()

        target.write_text(
            rendered_template.render(context)
        )

    def write_slurm_config(self, context) -> None:
        """Render the context to a template, adding in common configs."""
        if operating_system() == 'ubuntu':
            enable_jwt = True
        else:
            enable_jwt = False

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
            'enable_jwt': enable_jwt,
            'jwt_rsa_key_file': str(self._jwt_rsa_key_file),
            'slurmctld_parameters': ",".join(self._slurmctld_parameters),
            'slurm_plugstack_conf': str(self._slurm_plugstack_conf),
            'slurm_user': str(self._slurm_user),
            'slurmd_user': str(self._slurmd_user),
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

        # Preprocess merging slurmctld_parameters if they exist in the context
        context_slurmctld_parameters = context.get("slurmctld_parameters")
        if context_slurmctld_parameters:

            slurmctld_parameters = list(
                set(
                    common_config["slurmctld_parameters"].split(
                        ","
                    ) + context_slurmctld_parameters.split(",")
                )
            )

            common_config["slurmctld_parameters"] = ",".join(
                slurmctld_parameters
            )
            context.pop("slurmctld_parameters")

        rendered_template = Environment(
            loader=FileSystemLoader(str(self._template_dir))
        ).get_template(template_name)

        if target.exists():
            target.unlink()

        target.write_text(
            rendered_template.render(
                {**context, **common_config}
            )
        )

        # set correct permissions and ownership for configuration file
        if self._slurm_component == "slurmdbd":
            target.chmod(0o600)

        if "slurmd" == self._slurm_component:
            user_group = f"{self._slurmd_user}:{self._slurmd_group}"
        else:
            user_group = f"{self._slurm_user}:{self._slurm_group}"
        subprocess.call(["chown", user_group, target])

    def restart_slurm_component(self):
        """Restart the slurm component."""
        self.slurm_systemctl("restart")

    def write_munge_key(self, munge_key):
        """Write the munge key."""
        key = b64decode(munge_key.encode())
        self._munge_key_path.write_bytes(key)

    def write_jwt_rsa(self, jwt_rsa):
        """Write the jwt_rsa key."""

        # Remove jwt_rsa if exists.
        if self._jwt_rsa_key_file.exists():
            self._jwt_rsa_key_file.write_bytes(os.urandom(2048))
            self._jwt_rsa_key_file.unlink()

        # Write the jwt_rsa key to the file and chmod 0600,
        # chown to slurm_user.
        self._jwt_rsa_key_file.write_text(jwt_rsa)
        self._jwt_rsa_key_file.chmod(0o600)
        subprocess.call([
            "chown",
            self._slurm_user,
            str(self._jwt_rsa_key_file),
        ])

    def write_cgroup_conf(self, content):
        """Write the cgroup.conf file."""
        cgroup_conf_path = self._slurm_conf_dir / 'cgroup.conf'
        cgroup_conf_path.write_text(content)

    def get_munge_key(self) -> str:
        """Read the bytes, encode to base64, decode to a string, return."""
        munge_key = self._munge_key_path.read_bytes()
        return b64encode(munge_key).decode()

    def start_munged(self):
        """Enable and start munge.service"""
        munge = self._munged_systemd_service
        subprocess.call(["systemctl", "enable", munge])
        subprocess.call(["systemctl", "start", munge])

        try:
            status = subprocess.check_output(f"systemctl is-active {munge}",
                                             shell=True)
            status = status.decode().strip()
            if 'active' in status:
                logger.debug("#### munge.service started and enabled")
            else:
                logger.error(f"Error starting munge: {status}")
                return -1
        except subprocess.CalledProcessError as e:
            logger.error(f"Error starting munged - {e}")
            return -1

    def restart_munged(self):
        """Restart the munged process."""
        try:
            return subprocess.call([
                "service",
                self._munged_systemd_service,
                "restart",
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error restarting munged - {e}")
            return -1

    def slurm_cmd(self, command, arg_string):
        """Run a slurm command."""
        if command not in self._slurm_cmds:
            logger.error(f"{command} is not a slurm command.")
            return -1

        try:
            return subprocess.call([f"{command}"] + arg_string.split())
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running {command} - {e}")
            return -1

    def generate_jwt_rsa(self) -> str:
        """Generate the rsa key to encode the jwt with."""
        return RSA.generate(2048).export_key('PEM').decode()
