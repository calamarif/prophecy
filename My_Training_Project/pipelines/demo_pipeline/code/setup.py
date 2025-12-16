from setuptools import setup, find_packages
setup(
    name = 'demo_pipeline',
    version = '1.0',
    packages = find_packages(include = ('demo_pipeline*', )) + ['prophecy_config_instances.demo_pipeline'],
    package_dir = {'prophecy_config_instances.demo_pipeline' : 'configs/resources/demo_pipeline'},
    package_data = {'prophecy_config_instances.demo_pipeline' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'gender-guesser==0.4.0', 'prophecy-libs==2.1.7'],
    entry_points = {
'console_scripts' : [
'main = demo_pipeline.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
