Add datalad support to dtool
============================

.. image:: https://badge.fury.io/py/dtool-datalad.svg
   :target: http://badge.fury.io/py/dtool-datalad
   :alt: PyPi package

- GitHub: https://github.com/livMatS/dtool-datalad
- PyPI: https://pypi.python.org/livMatS/dtool-datalad
- Free software: MIT License

Features
--------

- Convert dtool datasets to datalad datasets and vice versa

Installation
------------

To install the dtool-datalad package::

    pip install dtool-datalad

Usage
-----

To copy a dtool dataset from local disk to a datalad dataset bucket
use the command below::

    dtool copy ./my-dataset datalad://path/to/datalad/dataset

To list all the dtool-compatible datalad datasets in a directory, use the command below::

    dtool ls datalad://path/to/folder/with/datalad/datasets

See the `dtool documentation <http://dtool.readthedocs.io>`_ for more detail.

Related packages
----------------

- `dtoolcore <https://github.com/jic-dtool/dtoolcore>`_
- `dtool-cli <https://github.com/jic-dtool/dtool-cli>`_
- `dtool-ecs <https://github.com/jic-dtool/dtool-ecs>`_
- `dtool-http <https://github.com/jic-dtool/dtool-http>`_
- `dtool-azure <https://github.com/jic-dtool/dtool-azure>`_
- `dtool-irods <https://github.com/jic-dtool/dtool-irods>`_
- `dtool-smb <https://github.com/livMatS/dtool-smb>`_
- `dtool-s3 <https://github.com/jic-dtool/dtool-s3>`_
