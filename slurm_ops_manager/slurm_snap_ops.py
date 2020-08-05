#!/usr/bin/python3
"""install slurm via snap."""
import os
import subprocess
from pathlib import Path


class SlurmSnapManager:
    """Class to install slurm as snap."""

    _CHARM_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    _TEMPLATE_DIR = _CHARM_DIR / 'templates'

    def __init__(self, component, res_path):
        """Determine values based on component."""
        self._slurm_component = component
        self._resource = res_path
        if component == "slurmdbd":
            self._template_name = "slurmdbd.conf.tmpl"
            self._target = Path(
                    "/var/snap/slurm/common/etc/slurm/slurmdbd.conf")
        else:
            self._template_name = "slurm.conf.tmpl"
            self._target = Path("/var/snap/slurm/common/etc/slurm/slurm.conf")
        self._source = Path(self._TEMPLATE_DIR / self._template_name)
        self._systemd_service = "snap.slurm." + self._slurm_component
        self._munge_key_path = Path(
            "/var/snap/slurm/common/etc/munge/munge.key")
        self.config_values = {
            "munge_socket": "/tmp/munged.socket.2",
            "mail_prog": "/snap/slurm/current/usr/bin/mail.mailutils",
            "slurm_user": "root",
            "slurmctld_pid_file": "/tmp/slurmctld.pid",
            "slurmd_pid_file": "/tmp/slurmd.pid",
            "slurmctld_log_file":
            "/var/snap/slurm/common/var/log/slurm/slurmctld.log",
            "slurmd_log_file":
            "/var/snap/slurm/common/var/log/slurm/slurmd.log",
            "slurm_spool_dir": "/var/snap/slurm/common/var/spool/slurm/d",
            "slurm_state_dir": "/var/snap/slurm/common/var/spool/slurm/ctld",
            "slurm_plugin_dir": "/snap/slurm/current/lib/slurm",
            "slurm_plugstack_conf":
            "/var/snap/slurm/common/etc/slurm/plugstack.d/plugstack.conf",
            "munge_socket": "/tmp/munged.socket.2",
            "slurmdbd_pid_file": "/tmp/slurmdbd.pid",
            "slurmdbd_log_file":
            "/var/snap/slurm/common/var/log/slurm/slurmdbd.log",
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

    @property
    def config(self):
        """Get config."""
        return self.config_values

    def get_version(self):
        """Get slurm version."""
        cp = subprocess.run(
            ["/snap/bin/slurm.version"],
            universal_newlines=True,
            stdout=subprocess.PIPE,
        )
        return cp.stdout[0:-1]

    def get_systemd_name(self):
        """Get systemd name."""
        return "snap.slurm." + self._slurm_component

    def get_munge_key_path(self):
        """Get munge key path."""
        return self._munge_key_path

    def get_template(self):
        """Get template."""
        return self._source

    def get_target(self):
        """Get target."""
        return self._target

    def get_tmpl_name(self):
        """Get template name."""
        return self._template_name

    @property
    def munge_sysd(self):
        """Get munge sysd name."""
        return "snap.slurm.munged"

    def install(self):
        """Install slurm."""
        self._install_snap()

    def _install_snap(self):
        """Install slurm via resource or from snap store."""
        cmd = ["snap", "install"]
        if self._resource:
            cmd.append(self._resource)
            cmd.append("--dangerous")
            cmd.append("--classic")
        else:
            cmd.append("slurm")
            cmd.append("--classic")
        try:
            subprocess.call(cmd)
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
