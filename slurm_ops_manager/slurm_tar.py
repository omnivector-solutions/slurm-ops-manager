from .install import prepare_system_for_slurm

class SlurmTarManager:
    def __init__(self, component, res_path):
        _store = StoredState()

    _CHARM_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    _TEMPLATE_DIR = _CHARM_DIR / 'templates'

    _SLURM_CONF_DIR = Path('/etc/slurm')
    _SLURM_SNAP_CONF_DIR = Path('/var/snap/slurm/common/etc/slurm-configurator/')
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

    def __init__(self, charm, component):
        """Determine values based on slurm component."""
        super().__init__(charm, component)
        self._store.set_default(slurm_installed=False)
        self._store.set_default(slurm_started=False)

        port_map = {
            'slurmdbd': 6819,
            'slurmd': 6818,
            'slurmctld': 6817,
            'slurmrestd': 6820,
        }
        self.resource_path = self.model.resources.fetch('slurm')
        logger.debug(self.resource_path)
        self._is_tar = tarfile.is_tarfile(self.resource_path)
        logger.debug("inside init #########################################")
        logger.debug(self._is_tar)
        if self._is_tar:
            self._MUNGE_KEY_PATH = Path("/etc/munge/munge.key")
        else:
            self._MUNGE_KEY_PATH = Path("/var/snap/slurm/common/etc/munge/munge.key")

        if component in ['slurmd', 'slurmctld', 'slurmrestd']:
            self._slurm_conf_template_name = 'slurm.conf.tmpl'
            if self._is_tar:
                self._slurm_conf = self._SLURM_CONF_DIR / 'slurm.conf'
            else:
                self._slurm_conf = self._SLURM_SNAP_CONF_DIR / 'slurm.conf'
        elif component == "slurmdbd":
            self._slurm_conf_template_name = 'slurmdbd.conf.tmpl'
            if self._is_tar:
                self._slurm_conf = self._SLURM_CONF_DIR / 'slurmdbd.conf'
            else:
                self._slurm_conf = self._SLURM_SNAP_CONF_DIR / 'slurmdbd.conf'

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
        self._environment_file = \
            self._SLURM_SYSCONFIG_DIR / f'{self._slurm_component}'

    def install():
       prepare_system_for_slurm()


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

        if not self._is_tar:
            component = "snap.slurm." + self._slurm_component
            try:
                subprocess.call([
                    "systemctl",
                    operation,
                    component,
                ])
            except subprocess.CalledProcessError as e:
                logger.error(f"Error exectuing _slurm_systemctl - {e}")
        else:
            try:
                subprocess.call([
                    "systemctl",
                    operation,
                    self._slurm_component,
            ])
            except subprocess.CalledProcessError as e:
                logger.error(f"Error exectuing systemctl - {e}")

            # Fix this later
            if operation == "start":
                self._store.slurm_started = True


    def _write_config(self, context) -> None:
        """Render the context to a template."""
        template_name = self._slurm_conf_template_name
        source = self._slurm_conf_template_location
        target = self._slurm_conf

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

    @property
    def is_active(self) -> bool:
        """Return True if slurm is running and false if it isn't."""
        if self._is_tar:
            cmd = ['systemctl', 'is-active', self._slurm_component]
        else:
            component = "snap.slurm." + self._slurm_component
            cmd = ['systemctl', 'is-active', component]
        return subprocess.call(cmd) == 0
    
    def _write_munge_key_and_restart(self, munge_key) -> None:
        key = b64decode(munge_key.encode())
        self._MUNGE_KEY_PATH.write_bytes(key)
        try:
            subprocess.call(["service", "munge", "restart"])
        except subprocess.CalledProcessError as e:
            logger.debug(e)

    def get_munge_key(self) -> str:
        """Read, encode, decode and return the munge key as a string."""
        munge_key = self._MUNGE_KEY_PATH.read_bytes()
        return b64encode(munge_key).decode()
