# slurm-ops-manager

This library is used to facilitate the installation and configuration of slurm in an operator charm.

## Usage

To get slurm installed in your charm, you need to define a resource "slurm" in your metadata.yaml of your
charm. This interface allows two different resource types, a slurm snap, and a slurm binary tar file.
The SlurmOpsManager class retrieves the model resource checks to see if it is binary or a snap and 
proceeds to install slurm.


Example:

```python

class SlurmCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
	snap_mode = "all"
        self.slurm_ops = SlurmOpsManager(self, snap_mode)

    def _on_install(self, event):
	self.slurm_ops.install()



```

# render_config_and_restart(self, slurm_config)
This function accomplishes 4 main tasks

* writes the slurm.conf

* restarts the slurm daemon

* writes the munge key

* restarts the munge daemon

to use this function correctly the user needs to supply a dictionary full of the correct key value pairs to be loaded into the template found in the templates folder of this repository. Either slurmbd.conf.tmpl if you're configuring the slurmdbd component or slurm.conf.tmppl for all other components of slurm. In that same dictionary the munge key value should be provided as a dict item with the key being "munge_key" and the value being a string representation of the munge key.

## values to be supplied to slurm.conf.tmpl

* clustername
* active_controller_hostname
* active_controller_ingress_address 
* backup_controller_hostname
* backup_controller_ingress_address 
* munge_socket 
* mail_prog 
* slurm_user 
* slurmctld_pid_file 
* slurmd_pid_file 
* slurmctld_log_file 
* slurmd_log_file 
* slurm_spool_dir 
* slurm_state_dir 
* slurm_plugin_dir
* slurm_plugstack_conf 
* slurmdbd_hostname 
* slurmdbd_port 
* munge_socket 


# COMPUTE NODES 
{% for node in nodes %}
{{node.inventory}}
{%- endfor -%}
{% for partition, values in partitions.items() %}
PartitionName={{ partition }} Nodes={{ values.hosts|join(',') }} Default={{ 'YES' if values.default else 'NO' }} State=UP
{% endfor %}

#### License
* [MIT](LICENSE)


#### Contact
* OmniVector Solutions <admin@omnivector.solutions>
