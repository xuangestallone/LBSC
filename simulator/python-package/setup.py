from setuptools import setup

setup(
    name='pywebcachesim',
    author='Zhenyu Song',
    author_email='sunnyszy@gmail.com',
    description='Cache Simulator',
    long_description='',
    install_requires=[
        'pyyaml',
        'numpy',
        'pandas',
        'pymongo',
        'sklearn',
        'loguru',
    ],
    zip_safe=False,
    packages=['pywebcachesim'],
    # entry_points = {
    #     'console_scripts': ['pywebcachesim=pywebcachesim.runner:main'],
    # },
)
