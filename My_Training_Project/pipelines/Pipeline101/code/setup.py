from setuptools import setup, find_packages
setup(
    name = 'Pipeline101',
    version = '1.0',
    packages = find_packages(include = ('pipeline101*', )) + ['prophecy_config_instances.pipeline101'],
    package_dir = {'prophecy_config_instances.pipeline101' : 'configs/resources/pipeline101'},
    package_data = {'prophecy_config_instances.pipeline101' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.3'],
    entry_points = {
'console_scripts' : [
'main = pipeline101.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
