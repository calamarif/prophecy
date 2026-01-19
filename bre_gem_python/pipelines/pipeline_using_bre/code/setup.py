from setuptools import setup, find_packages
setup(
    name = 'pipeline_using_bre',
    version = '1.0',
    packages = find_packages(include = ('pipeline_using_bre*', )) + ['prophecy_config_instances.pipeline_using_bre'],
    package_dir = {'prophecy_config_instances.pipeline_using_bre' : 'configs/resources/pipeline_using_bre'},
    package_data = {'prophecy_config_instances.pipeline_using_bre' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.9'],
    entry_points = {
'console_scripts' : [
'main = pipeline_using_bre.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
