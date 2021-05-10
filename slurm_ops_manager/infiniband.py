#!/usr/bin/env python3
"""Infiniband Class."""

import logging
import os
import shlex
import shutil
import subprocess
from pathlib import Path

from ops.framework import Object, StoredState
from slurm_ops_manager.utils import operating_system


logger = logging.getLogger()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Infiniband(Object):
    """Responsible for Infiniband operations."""

    _stored = StoredState()

    def __init__(self, parent, key):
        """Initialize class."""
        super().__init__(parent, key)

        self._stored.set_default(ib_installed=False,
                                 ib_repo_configured=False,
                                 ib_custom_repo=False,
                                 ib_repo_path=str(), # it doesn't accept Path
                                 ib_default_repo=str(),
                                 ib_package_name='mlnx-ofed-all')

        self._ib_systemd_service = 'openibd.service'
        self._template_dir = Path(BASE_DIR) / 'templates'
        self._operating_system = operating_system()

        # setup repositories based on official Mellanox docs
        if self._operating_system == 'ubuntu':
            self._stored.ib_repo_path = "/etc/apt/sources.list.d/infiniband.list"
            repo_path = self._template_dir / "mellanox_repo_ubuntu.list"
            self._stored.ib_default_repo = repo_path.as_posix()
        elif self._operating_system == 'centos':
            self._stored.ib_repo_path = "/etc/yum.repos.d/infiniband.repo"
            repo_path = self._template_dir / "mellanox_repo_centos7.repo"
            self._stored.ib_default_repo = repo_path.as_posix()
        else:
            logger.error(f'#### Unsupported OS to install infiniband: {self._operating_system}')

    @property
    def repository(self):
        """Return the repository used for Infiniband drivers."""
        if self._stored.ib_repo_configured:
            return Path(self._stored.ib_repo_path).read_text()
        else:
            return "Repository not set up"

    @repository.setter
    def repository(self, repo: str):
        """Set a custom repository to install Infiniband drivers.

        If the new repository string is empty, re-sets to the default.
        """
        if repo:
            logger.debug(f'#### Infiniband - setting custom repo: {repo}')
            Path(self._stored.ib_repo_path).write_text(repo)
        else:
            logger.debug('#### Infiniband - setting default repo')
            shutil.copyfile(self._stored.ib_default_repo,
                            self._stored.ib_repo_path)

            if self._operating_system == 'ubuntu':
                logger.debug('#### Infiniband - adding GPG keys')
                key_url = 'http://www.mellanox.com/downloads/ofed/RPM-GPG-KEY-Mellanox'
                key_path = '/tmp/mellanox.gpg'
                cmd = f'curl -s -L -o {key_path} {key_url}'
                subprocess.run(shlex.split(cmd))
                r = subprocess.run(f'apt-key add {key_path}', shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
                if 'OK' not in r.stdout.decode():
                    logger.error('#### Infiniband - failed to add GPG key')
                    return -1

        self._stored.ib_repo_configured = True

    def install(self):
        """Install Mellanox Infiniband packages.

        If a custom repository was not previously configured, this method will
        setup the default repository before installing the drivers.
        """

        if not self._stored.ib_repo_configured:
            self.repository = ""

        logger.debug('#### Infiniband - detecting OS to install drivers')

        if self._operating_system == 'ubuntu':
            cmd = f"apt install --yes {self._stored.ib_package_name}"
        elif self._operating_system == 'centos':
            cmd = f"yum install --assumeyes {self._stored.ib_package_name}"
        else:
            logger.error(f'#### Unsupported OS: {self._operating_system}')
            return -1

        logger.debug(f'#### Installing on {self._operating_system}')
        logger.debug(f'#### Infiniband package: {self._stored.ib_package_name}')

        subprocess.run(shlex.split(cmd))
        self._stored.ib_installed = True

    def uninstall(self):
        """Uninstall Mellanox Infiniband packages."""

        logger.debug('#### Infiniband - detecting OS to uninstall drivers')

        if self._operating_system == 'ubuntu':
            cmd = f"apt purge --yes {self._stored.ib_package_name}"
        elif self._operating_system == 'centos':
            cmd = f"yum remove --assumeyes {self._stored.ib_package_name}"
        else:
            logger.error(f'#### Unsupported OS: {self._operating_system}')
            return -1

        logger.debug(f'#### Uninstalling on {self._operating_system}')
        logger.debug(f'#### Infiniband package: {self._stored.ib_package_name}')

        subprocess.run(shlex.split(cmd))
        self._stored.ib_installed = False

    def start(self):
        """Start infiniband systemd service."""
        subprocess.run(shlex.split(f"systemctl start {self._ib_systemd_service}"))

    def enable(self):
        """Enable infiniband systemd service."""
        subprocess.run(shlex.split(f"systemctl enable {self._ib_systemd_service}"))

    def stop(self):
        """Stop infiniband systemd service."""
        subprocess.run(shlex.split(f"systemctl stop {self._ib_systemd_service}"))

    def is_active(self) -> bool:
        """Check if systemd infiniband service is active."""
        try:
            cmd = f"systemctl is-active {self._ib_systemd_service}"
            r = subprocess.check_output(shlex.split(cmd))
            return 'active' == r.decode().strip().lower()
        except subprocess.CalledProcessError as e:
            logger.error(f'#### Could not check infiniband: {e}')
            return False
        return False
