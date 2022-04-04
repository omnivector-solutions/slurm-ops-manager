"""Nvidia Class."""

import logging
import shlex
import subprocess
from pathlib import Path

from ops.framework import Object, StoredState
from slurm_ops_manager.utils import operating_system


logger = logging.getLogger()


class NvidiaGPU(Object):
    """Responsible for Nvdia GPU operations."""

    _stored = StoredState()

    def __init__(self, parent, key):
        """Initialize class."""
        super().__init__(parent, key)

        self._stored.set_default(gpu_installed=False,
                                 gpu_repo_configured=False,
                                 gpu_repo_path=str(),
                                 gpu_package_name=str())

        self._operating_system = operating_system()

        # setup repositories based on official instructions
        if self._operating_system == "ubuntu":
            self._stored.gpu_repo_path = "/etc/apt/sources.list.d/nvidia_gpu.list"
        elif self._operating_system == "centos":
            self._stored.gpu_repo_path = "/etc/yum.repos.d/cuda-rhel7.repo"
        else:
            logger.error(f"#### Unsupported OS: {self._operating_system}")

        # set a default package name
        if not self.package:
            self.package = ""

    @property
    def installed(self) -> bool:
        """Return wether nvidia driver is installed."""
        return self._stored.gpu_installed

    @property
    def repository(self) -> str:
        """Return the repository used for nvidia drivers."""
        repo = ""
        if self._stored.gpu_repo_configured:
            repo = Path(self._stored.gpu_repo_path).read_text()
        return repo

    @repository.setter
    def repository(self, repo: str):
        """Set a custom repository to install nvidia drivers.

        If the new repository string is empty, re-sets to the default.
        """
        if repo:
            logger.debug(f"#### Nvidia - setting custom repo: {repo}")
            Path(self._stored.gpu_repo_path).write_text(repo)
        else:
            logger.debug("#### Nvidia - setting default repo")
            # instructions from nvdia website:
            # https://docs.nvidia.com/datacenter/tesla/tesla-installation-notes/index.html

            if self._operating_system == "ubuntu":
                distro = "ubuntu2004"
                repo = f"deb http://developer.download.nvidia.com/compute/cuda/repos/{distro}/x86_64 /" # noqa
                Path(self._stored.gpu_repo_path).write_text(repo)

                # download pin file
                url = f"https://developer.download.nvidia.com/compute/cuda/repos/{distro}/x86_64/cuda-{distro}.pin" # noqa
                path = "/etc/apt/preferences.d/nvidia_gpu-pin-600"
                cmd = f"curl -s -L -o {path} {url}"
                subprocess.run(shlex.split(cmd))

                # add key
                cmd = f"apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/{distro}/x86_64/7fa2af80.pub" # noqa
                subprocess.run(shlex.split(cmd))

                # it is necessary to update apt's db after every repo change
                subprocess.run(["apt-get", "update"])
            elif self._operating_system == "centos":
                distro = "rhel7"
                url = f"http://developer.download.nvidia.com/compute/cuda/repos/{distro}/x86_64/cuda-{distro}.repo" # noqa
                cmd = f"yum-config-manager --add-repo {url}"
                subprocess.run(shlex.split(cmd))

                subprocess.run(["yum", "clean", "expire-cache"])

        self._stored.gpu_repo_configured = True

    @property
    def package(self) -> str:
        """Return nVdia package name."""
        return self._stored.gpu_package_name

    @package.setter
    def package(self, pkg: str):
        """Change the package to install."""
        if not pkg:
            # if empty, sets the default
            if self._operating_system == "ubuntu":
                self._stored.gpu_package_name = "cuda-drivers"
            elif self._operating_system == "centos":
                self._stored.gpu_package_name = "nvidia-driver-latest-dkms cuda cuda-drivers"
        else:
            self._stored.gpu_package_name = pkg

    def install(self):
        """Install Nvidia GPU packages.

        If a custom repository was not previously configured, this method will
        setup the default repository before installing the drivers.
        """

        if not self._stored.gpu_repo_configured:
            self.repository = "" # set the default

        logger.debug("#### Nvidia - detecting OS to install drivers")
        uname = subprocess.check_output(shlex.split("uname -r")).strip().decode()

        cmds = {"ubuntu": f"apt-get install --yes linux-headers-{uname} {self.package}",
                "centos": f"yum install --assumeyes kernel-devel-{uname} kernel-headers-{uname} {self.package}"} # noqa

        os_ = self._operating_system
        logger.debug(f"#### Installing on {self._operating_system}")
        logger.debug(f"#### GPU package: {self.package}")
        logger.debug(f"#### command: {cmds[os_]}")

        subprocess.run(shlex.split(cmds[os_]))
        self._stored.gpu_installed = True
