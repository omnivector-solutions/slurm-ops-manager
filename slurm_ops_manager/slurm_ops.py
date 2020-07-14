#!/usr/bin/env python3
"""This module provides the SlurmInstallManager."""
import json
import logging
import os
import re
import socket
import subprocess
import sys
from pathlib import Path
from time import sleep
from base64 import b64encode, b64decode

from jinja2 import Environment, FileSystemLoader

from ops.framework import (
    EventBase,
    EventSource,
    Object,
    ObjectEvents,
    StoredState,
)

from ops.model import ModelError


logger = logging.getLogger()


# Regex explanation:
#  \b           # Start at a word boundary
#  (\w+)        # Match and capture a single word (1+ alnum characters)
#  \s*=\s*      # Match a equal, optionally surrounded by whitespace
#  ([^=]*)      # Match any number of non-equal characters
#  (?=          # Make sure that we stop when the following can be matched:
#   \s+\w+\s*=  #  the next dictionary key
#  |            # or
#  $            #  the end of the string
#  )            # End of lookahead

def _get_inv():
    try:
        inventory = subprocess.check_output(
            "slurmd -C", shell=True
        ).strip().decode('ascii')
    except subprocess.CalledProcessError as e:
        logger.debug(f"Failed getting inventory - {e}")

    regex = re.compile(r"\b(\w+)\s*=\s*([^=]*)(?=\s+\w+\s*=|$)")
    return dict(regex.findall(inventory))


# Get the number of GPUs and check that they exist at /dev/nvidiaX
def _get_gpu():
    try:
        gpu = int(
            subprocess.check_output(
                "lspci | grep -i nvidia | awk '{print $1}' "
                "| cut -d : -f 1 | sort -u | wc -l",
                shell=True
            )
        )
    except subprocess.CalledProcessError as e:
        print(e)

    for i in range(gpu):
        gpu_path = "/dev/nvidia" + str(i)
        if not os.path.exists(gpu_path):
            return 0
    return gpu


def get_inventory():
    inv = _get_inv()
    inv['gpus'] = _get_gpu()
    return inv


class ConfigureAndRestartEvent(EventBase):
    """Emits the slurm_config."""

    def __init__(self, handle, slurm_config):
        super().__init__(handle)
        self._slurm_config = slurm_config

    @property
    def slurm_config(self):
        return self._slurm_config

    def snapshot(self):
        return {'slurm_config': self._slurm_config}

    def restore(self, snapshot):
        self._slurm_config = snapshot.get('slurm_config')


class SlurmOpsManagerEvents(ObjectEvents):
    configure_and_restart = EventSource(ConfigureAndRestartEvent)


class SlurmOpsManager(Object):
    """Slurm installation of lifecycle ops."""

    on = SlurmOpsManagerEvents()

    _store = StoredState()

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

    def __init__(self, charm, component):
        """Determine values based on slurm component."""
        super().__init__(charm, component)
        self._store.set_default(slurm_installed=False)
        self._store.set_default(slurm_started=False)

        self.framework.observe(
            self.on.configure_and_restart,
            self._on_configure_and_restart
        )

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
        self._environment_file = self._SLURM_SYSCONFIG_DIR / f'{self._slurm_component}'

    def _on_configure_and_restart(self, event):
        slurm_config = json.loads(event.slurm_config)
        self.render_config_and_restart(slurm_config)

    def render_config_and_restart(self, slurm_config):

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

    @property
    def is_active(self):
        return subprocess.call(['systemctl', 'is-active', self._slurm_component]) == 0

    @property
    def inventory(self):
        return json.dumps(get_inventory())

    @property
    def slurm_installed(self):
        return self._store.slurm_installed

    @property
    def slurm_component_started(self):
        return self._store.slurm_started

    def _slurm_systemctl(self, operation):
        """Start systemd services for slurmd."""

        if operation not in ["enable", "start", "stop", "restart"]:
            msg = f"Unsupported systemctl command for {self._slurm_component}"
            raise Exception(msg)

        try:
            subprocess.call([
                "systemctl",
                operation,
                self._slurm_component,
            ])
            # Fix this later
            if operation == "start":
                self._store.slurm_started = True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying systemd - {e}")

    def _write_config(self, context):
        """Render the context to a template."""

        #ctxt = {}
        template_name = self._slurm_conf_template_name
        source = self._slurm_conf_template_location
        target = self._slurm_conf

        if not type(context) == dict:
            raise TypeError("Incorrect type for config.")
            #ctxt = {**{"hostname": self._hostname}, **context}

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

    def prepare_system_for_slurm(self):
        """Prepare the system for slurm.

        * create slurm user/group
        * create filesystem for slurm
        * provision slurm resource
        """
        self._install_os_deps()
        self._create_slurm_user_and_group()
        self._prepare_filesystem()
        self._create_environment_file()
   
        self._install_munge()
        self._provision_slurm_resource()

        self._set_ld_library_path()

        self._setup_systemd()
        self._store.slurm_installed = True

    def _install_os_deps(self):
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
            

    def _install_munge(self):
        try:
            subprocess.call(["apt", "install", "munge", "-y"])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def _write_munge_key_and_restart(self, munge_key):
        key = b64decode(munge_key.encode())
        self._MUNGE_KEY_PATH.write_bytes(key)
        try:
            subprocess.call(["service", "munge", "restart"])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def get_munge_key(self, munge_key):
        munge_key = self._MUNGE_KEY_PATH.read_bytes()
        return base64.b64encode(munge_key).decode()

    def _create_environment_file(self):
        self._environment_file.write_text(
            f"SLURM_CONF={str(self._slurm_conf)}"
        )

    def _chown_slurm_user_and_group_recursive(self, slurm_dir):
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

    def _create_slurm_user_and_group(self):
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

    def _prepare_filesystem(self):
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

    def _provision_slurm_resource(self):
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

    def _set_ld_library_path(self):
        """Set the LD_LIBRARY_PATH."""
        Path('/etc/ld.so.conf.d/slurm.conf').write_text("/usr/local/lib/slurm")
        try:
            subprocess.call(["ldconfig"])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error setting LD_LIBRARY_PATH - {e}")

    def _setup_systemd(self):
        """Setup systemd services for slurm components."""
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
