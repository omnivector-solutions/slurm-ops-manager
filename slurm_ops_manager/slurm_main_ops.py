from ops.framework import (
    Object,
    StoredState,
)
class SlurmOpsManager(Object):

    def __init__(self, charm, component):
        self._slurm_component = component
        self._stored.set_default(slurm_installed=False)
        self._resource_path = self.model.resources.fetch('slurm')
        self._is_tar = tarfile.is_tarfile(self.resource_path)

        if self._is_tar:
            self.slurm_resource = SlurmTarManager(component, self._resource_path)
        else:
            self.slurm_resource = SlurmSnapManager(component, self._resource_path)

    def install(self):
        self.slurm_resource.install()
        self._stored.slurm_installed = True
    
    def render_config_and_restart(self, config):
        self.slurm_resource.render_config_and_restart(config)
