from setuptools import find_packages, setup


__version__ = "0.0.2+dev"

setup(
    name='slurm-ops-manager',
    packages=find_packages(include=['slurm_ops_manager']),
    version=__version__,
    license='MIT',
    long_description=open('README.md', 'r').read(),
    url='https://github.com/omnivector-solutions/slurm-ops-manager',
    install_requires=['jinja2'],
    python_requires='>=3.6',
    package_data={'slurm_ops_manager': ['templates/*']},
)
