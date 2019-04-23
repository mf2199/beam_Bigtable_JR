# Create Package to use in Dataflow

## Description

This code demonstrates how to connect create, upload, and run an extra package at Google Dataflow environment.

## Build and Run
1.  In order to create a new installation, the following file structure may be used:
    ```    
    ├── my_package              # Your package folder
        ├─── __init__.py        # Package initialization file.
        ├─── __version__.py     # Version file.
        └─── my_source.py       # Your package source code
    ├── setup.py                # Package installation instructions
    ├── LICENSE                 # License terms
    └── README.md               # This file
    ```
1.  Create the installation file, 'setup.py', with the package name in the NAME field.
2.  Create (or copy) the file describing the license terms.
3.  [recommended] Add a copy of this file, 'README.md', for future reference.
4.  Create a subfolder named the same as the package.
5.  In that subfolder, create '__init\__.py' file containing the following directive:
    ```python
    from .bigtableio import *
    ```
6.  In the same subfolder, create '__version\__.py' file specifying the package version:
    ```python
    VERSION = (0, 1, 234)
    __version__ = '.'.join(map(str, VERSION))
    ```
7.  Add the source code file(s) to the package subfolder.
9.  Run the setup file to create the new package tarball:
    ```sh
    $ python setup.py sdist --formats=gztar
    ```
10. This will create the installation archive named something like this:
    ```
    my_package-0.1.234.tar.gz
    ```
11. If necessary, install the package locally, e.g. by running 'pip install':
    ```sh
    $ pip install my_package-0.1.234.tar.gz
    ```
12. In order to run an Apache Beam Pipeline, the following PipelineOptions must be set:
    ```python
    [
        '--experiments=beam_fn_api',
        '--project=my-project-id',
        '--requirements_file=requirements.txt',
        '--runner=dataflow',
        '--staging_location=gs://my-storage-folder/stage',
        '--temp_location=gs://my-storage-folder/temp',
        '--setup_file=./my_package/setup.py',
        '--extra_package=./my_package/dist/my_package-0.1.234.tar.gz'
    ]
    ```
    _*** the 'setup_file' option specifies the package installation file ***_

    _*** the 'extra_package' option provides the path to the compressed archive ***_

    _*** replace 'my-project-id' with the actual project name ***_
    
    _*** to run the code locally, change the '--runner' parameter to 'direct' ***_
    
    _*** replace 'my-storage-folder' with actual Google Storage folder name ***_
    
    _*** replace 'my_package' with the actual package name ***_
    

## Contributing changes

* See [CONTRIBUTING.md](../../CONTRIBUTING.md)

## Licensing

* See [LICENSE](../../LICENSE)