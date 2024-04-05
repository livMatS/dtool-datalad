from setuptools import setup

url = "https://github.com/livMatS/dtool-datalad"
version = "0.14.1"
readme = open('README.rst').read()

setup(
    name="dtool-datalad",
    packages=["dtool_datalad"],
    version=version,
    description="Make datalad and dtool interoperable",
    long_description=readme,
    include_package_data=True,
    author="distribits 2024 ",
    author_email="johannes.laurin@gmail.com",
    url=url,
    download_url="{}/tarball/{}".format(url, version),
    install_requires=[
        "click",
        "dtoolcore>=3.17",
        "dtool_cli",
    ],
    entry_points={
        "dtool.storage_brokers": [
            "DataLadStorageBroker=dtool_datalad.storagebroker:DataLadStorageBroker",
        ],
    },
    license="MIT"
)
