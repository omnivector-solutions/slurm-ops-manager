#!/usr/bin/env python3
"""This module provides the SlurmRpmManager."""
import logging
import subprocess
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from slurm_ops_manager.slurm_ops_base import SlurmOpsManagerBase


logger = logging.getLogger()


class SlurmRpmManager(SlurmOpsManagerBase):
    """Slurm debian operations manager."""

    def __init__(self, component):
        """Set initial attribute values."""
        super().__init__(component)

    @property
    def _slurm_plugin_dir(self) -> Path:
        return Path("/usr/lib64/slurm/")

    @property
    def _mail_prog(self) -> Path:
        return Path("/usr/bin/mailx")

    @property
    def slurm_version(self) -> str:
        """Return slurm verion."""
        cmd = 'yum info -C slurm | grep "^Version"'
        locale = {'LC_ALL': 'C', 'LANG': 'C.UTF-8'}
        version = subprocess.check_output(cmd, shell=True, env=locale)
        return version.decode().split(":")[-1].strip()

    @property
    def munge_version(self) -> str:
        """Return munge verion."""
        cmd = 'yum info -C munge | grep "^Version"'
        locale = {'LC_ALL': 'C', 'LANG': 'C.UTF-8'}
        version = subprocess.check_output(cmd, shell=True, env=locale)
        return version.decode().split(":")[-1].strip()

    def _install_slurm_from_rpm(self) -> bool:
        """Install Slurm rpms.

        Returns True on success and False otherwise.
        """
        slurm_component = self._slurm_component
        logger.debug(f"## Installing dependencies for {slurm_component}")

        # update/install specific needed dependencies
        subprocess.check_output(["yum", "install", "--assumeyes",
                                 "pciutils", "logrotate", "mailx", "munge"])
        subprocess.check_output(["systemctl", "enable", self._munged_systemd_service])

        logger.debug(f"## Installing {slurm_component}")
        try:
            subprocess.check_output(["yum", "install", "--assumeyes",
                                     f"slurm-{slurm_component}", "slurm"])
        except subprocess.CalledProcessError as e:
            logger.error(f"Error installing {slurm_component} - {e}")
            return False

        logger.info("#### All packages installed!")

        # munge rpm does not create a munge key, so we need to create one
        logger.info("#### Creating munge key")
        keycmd = f"dd if=/dev/urandom of={str(self._munge_key_path)} bs=1 count=1024"
        subprocess.check_output(keycmd.split())
        usergroup = f"{self._munge_user}:{self._munge_group}"
        subprocess.check_output(f"chown {usergroup} {str(self._munge_key_path)}".split())
        subprocess.check_output(f"chmod 0400 {str(self._munge_key_path)}".split())
        logger.info("#### Created munge key")

        # current rpms do not create a slurm user and group, so we create it
        logger.info("#### Creating slurm user and group")

        try:
            subprocess.check_output(["groupadd", "--gid", self._slurm_group_id,
                                                 self._slurm_group])
        except subprocess.CalledProcessError as e:
            if e.returncode == 9:
                logger.warning("## Group already exists.")
            else:
                logger.error(f"## Error creating group: {e}")
                return False

        try:
            subprocess.check_output(["adduser", "--system",
                                                "--gid", self._slurm_group_id,
                                                "--uid", self._slurm_user_id,
                                                "--no-create-home",
                                                "--home", "/nonexistent",
                                                self._slurm_user])
        except subprocess.CalledProcessError as e:
            if e.returncode == 9:
                logger.warning("## User already exists.")
            else:
                logger.error(f"## Error creating user: {e}")
                return False

        logger.info("#### Created slurm user and group")

        # we need to override the default service unit for slurmrestd only
        if "slurmrestd" == self._slurm_component:
            self.setup_slurmrestd_systemd_unit()

        return True

    def _setup_repo(self, custom_repo: str) -> bool:
        """Set up RPM configuration for slurm rpms.

        Args:
            custom_repo: string with URL of the custom repository. Setting it
                         to any value overrides the default Omnivector stable
                         repo. Example value:
                         "https://omnivector-solutions.github.io/repo/centos7/stable/$basearch"
        Returns:
            bool: wether the operation was successful.
        """
        if custom_repo:
            context = {"title": "omni-custom",
                       "baseurl": custom_repo}
        else:
            context = {"title": "omni-stable",
                       "baseurl": "https://omnivector-solutions.github.io/repo/centos7/stable/$basearch"} # noqa
        logger.debug(f"## Configuring repository for Slurm rpms: {context}")

        template_dir = Path(__file__).parent / "templates/"
        environment = Environment(loader=FileSystemLoader(template_dir))
        template = environment.get_template("omnirepo_centos.repo.tmpl")

        target = Path("/etc/yum.repos.d/omni.repo")
        target.write_text(template.render(context))

        try:
            subprocess.check_output(["yum", "makecache"])
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"## Error setting up repo: {e}")
            return False

    def upgrade(self) -> bool:
        """Run upgrade operations."""
        logger.warning("## This operation is not yet supported on CentOS.")
        return True

    def setup_slurm(self, custom_repo: str = "") -> bool:
        """Install Slurm and its dependencies.

        Args:
            custom_repo: URL to a custom repository. Setting it to any value
                         superseeds the Omnivector Repository.
        Returns:
            bool: whether the installation succeds or not.
        """
        if not self._setup_repo(custom_repo):
            return False

        successful_installation = self._install_slurm_from_rpm()

        # create needed paths with correct permisions
        self._setup_paths()

        if self._slurm_component in ["slurmctld", "slurmd"]:
            self._setup_plugstack_dir_and_config()

        self.slurm_systemctl('enable')

        return successful_installation
