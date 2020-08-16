import subprocess


class PrometheusSlurmExporterManager:
    def __init__(self):
        self.name = 'PrometheusSlurmExporterManager'

    def install(self, resource_path):
        try:
            subprocess.call([
                "snap",
                "install",
                resource_path,
                "--dangerous",
                "--classic",
            ])
        except subprocess.CalledProcessError as e:
            print("Cannot install prometheus-slurm-exporter")
