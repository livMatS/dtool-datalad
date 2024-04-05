import os
import json
import shutil
import logging
import datetime
import socket

from dtoolcore import __version__
from dtoolcore.utils import (
    mkdir_parents,
    generate_identifier,
    generous_parse_uri,
    timestamp,
    IS_WINDOWS,
    windows_to_unix_path,
    unix_to_windows_path,
    handle_to_osrelpath,
)
from dtoolcore.filehasher import FileHasher, md5sum_hexdigest

logger = logging.getLogger(__name__)


_STRUCTURE_PARAMETERS = {
    "data_directory": ["data"],
    "dataset_readme_relpath": ["README.yml"],
    "dtool_directory": [".dtool"],
    "admin_metadata_relpath": [".dtool", "dtool"],
    "structure_metadata_relpath": [".dtool", "structure.json"],
    "dtool_readme_relpath": [".dtool", "README.txt"],
    "manifest_relpath": [".dtool", "manifest.json"],
    "overlays_directory": [".dtool", "overlays"],
    "annotations_directory": [".dtool", "annotations"],
    "tags_directory": [".dtool", "tags"],
    "metadata_fragments_directory": [".dtool", "tmp_fragments"],
    "storage_broker_version": __version__,
}

_DTOOL_README_TXT = """README
======

This is a Dtool dataset stored on traditional file system storage.

Content provided during the dataset creation process
----------------------------------------------------

Dataset descriptive metadata: README.yml
Dataset items: data/

Automatically generated files and directories
---------------------------------------------

This file: .dtool/README.txt
Administrative metadata describing the dataset: .dtool/dtool
Structural metadata describing the dataset: .dtool/structure.json
Structural metadata describing the data items: .dtool/manifest.json
Per item descriptive metadata: .dtool/overlays/
Dataset key/value pairs metadata: .dtool/annotations/
Dataset tags metadata: .dtool/tags/
"""


class DataLadStorageBroker(BaseStorageBroker):
    """
    Storage broker to interact with datalad datasets (on local disk storage).
    """

    #: Attribute used to define the type of storage broker.
    key = "datalad"

    #: Attribute used by :class:`dtoolcore.ProtoDataSet` to write the hash
    #: function name to the manifest.
    hasher = FileHasher(md5sum_hexdigest)

    # Attribute used to define the structure of the dataset.
    _structure_parameters = _STRUCTURE_PARAMETERS

    # Attribute used to document the structure of the dataset.
    _dtool_readme_txt = _DTOOL_README_TXT

    def __init__(self, uri, config_path=None):

        logger.debug("Initialising {}...".format(self))

        # Get the abspath to the dataset.
        self._abspath = _get_abspath_from_uri(uri)

        # Define some other more abspaths.
        self._data_abspath = self._generate_abspath("data_directory")
        self._overlays_abspath = self._generate_abspath("overlays_directory")
        self._annotations_abspath = self._generate_abspath(
            "annotations_directory"
        )
        self._tags_abspath = self._generate_abspath(
            "tags_directory"
        )
        self._metadata_fragments_abspath = self._generate_abspath(
            "metadata_fragments_directory"
        )

        # Define some essential directories to be created.
        self._essential_subdirectories = [
            self._generate_abspath("dtool_directory"),
            self._data_abspath,
            self._overlays_abspath,
            self._annotations_abspath,
            self._tags_abspath,
        ]

    # Generic helper functions.

    def _generate_abspath(self, key):
        return os.path.join(self._abspath, *self._structure_parameters[key])

    def _fpath_from_handle(self, handle):
        return os.path.join(self._data_abspath, handle)

    def _handle_to_fragment_absprefixpath(self, handle):
        stem = generate_identifier(handle)
        return os.path.join(self._metadata_fragments_abspath, stem)

    # Class methods to override.

    @classmethod
    def list_dataset_uris(cls, base_uri, config_path):
        """Return list containing URIs in location given by base_uri."""

        parsed_uri = generous_parse_uri(base_uri)
        uri_list = []

        path = parsed_uri.path
        if IS_WINDOWS:
            path = unix_to_windows_path(parsed_uri.path)

        for d in os.listdir(path):
            dir_path = os.path.join(path, d)

            if not os.path.isdir(dir_path):
                continue

            storage_broker = cls(dir_path, config_path)

            if not storage_broker.has_admin_metadata():
                continue

            uri = storage_broker.generate_uri(
                name=d,
                uuid=None,
                base_uri=base_uri
            )
            uri_list.append(uri)

        return uri_list

    @classmethod
    def generate_uri(cls, name, uuid, base_uri):
        logger.debug("In DiskStorageBroker.generate_uri...")
        parsed_uri = generous_parse_uri(base_uri)
        base_dir_path = parsed_uri.path
        if IS_WINDOWS:
            base_dir_path = unix_to_windows_path(base_dir_path)
        dataset_path = os.path.join(base_dir_path, name)
        dataset_abspath = os.path.abspath(dataset_path)
        if IS_WINDOWS:
            dataset_abspath = windows_to_unix_path(dataset_abspath)
            return "{}:///{}".format(cls.key, dataset_abspath)
        else:
            return "{}://{}{}".format(
                cls.key,
                socket.gethostname(),
                dataset_abspath
            )

    # Methods to override.
    def get_text(self, key):
        """Return the text associated with the key."""
        with open(key) as fh:
            return fh.read()

    def put_text(self, key, text):
        """Put the text into the storage associated with the key."""
        parent_directory = os.path.dirname(key)
        mkdir_parents(parent_directory)
        with open(key, "w") as fh:
            fh.write(text)

    def delete_key(self, key):
        """Delete the file/object associated with the key."""
        try:
            os.unlink(key)
        except OSError:
            pass

    def get_admin_metadata_key(self):
        "Return the path to the admin metadata file."""
        return self._generate_abspath("admin_metadata_relpath")

    def get_readme_key(self):
        "Return the path to the readme file."""
        return self._generate_abspath("dataset_readme_relpath")

    def get_manifest_key(self):
        "Return the path to the readme file."""
        return self._generate_abspath("manifest_relpath")

    def get_structure_key(self):
        "Return the path to the structure parameter file."""
        return self._generate_abspath("structure_metadata_relpath")

    def get_dtool_readme_key(self):
        "Return the path to the dtool readme file."""
        return self._generate_abspath("dtool_readme_relpath")

    def get_overlay_key(self, overlay_name):
        "Return the path to the overlay file."""
        return os.path.join(self._overlays_abspath, overlay_name + '.json')

    def get_annotation_key(self, annotation_name):
        "Return the path to the annotation file."""
        return os.path.join(
            self._annotations_abspath,
            annotation_name + '.json'
        )

    def get_tag_key(self, tag):
        "Return the path to the tag file."""
        return os.path.join(
            self._tags_abspath,
            tag
        )

    def get_size_in_bytes(self, handle):
        """Return the size in bytes."""
        fpath = self._fpath_from_handle(handle)
        return os.stat(fpath).st_size

    def get_utc_timestamp(self, handle):
        """Return the UTC timestamp."""
        fpath = self._fpath_from_handle(handle)
        datetime_obj = datetime.datetime.utcfromtimestamp(
            os.stat(fpath).st_mtime
        )
        return timestamp(datetime_obj)

    def get_hash(self, handle):
        """Return the hash."""
        fpath = self._fpath_from_handle(handle)
        return DiskStorageBroker.hasher(fpath)

    def has_admin_metadata(self):
        """Return True if the administrative metadata exists.

        This is the definition of being a "dataset".
        """
        return os.path.isfile(self.get_admin_metadata_key())

    def list_overlay_names(self):
        """Return list of overlay names."""
        overlay_names = []
        if not os.path.isdir(self._overlays_abspath):
            return overlay_names
        for fname in os.listdir(self._overlays_abspath):
            name, ext = os.path.splitext(fname)
            overlay_names.append(name)
        return overlay_names

    def list_annotation_names(self):
        """Return list of annotation names."""
        annotation_names = []
        if not os.path.isdir(self._annotations_abspath):
            return annotation_names
        for fname in os.listdir(self._annotations_abspath):
            name, ext = os.path.splitext(fname)
            annotation_names.append(name)
        return annotation_names

    def list_tags(self):
        """Return list of tags."""
        tags = []
        if not os.path.isdir(self._tags_abspath):
            return tags
        for fname in os.listdir(self._tags_abspath):
            tags.append(fname)
        return tags

    def get_item_abspath(self, identifier):
        """Return absolute path at which item content can be accessed.

        :param identifier: item identifier
        :returns: absolute path from which the item content can be accessed
        """
        manifest = self.get_manifest()
        item = manifest["items"][identifier]
        relpath = handle_to_osrelpath(item["relpath"], IS_WINDOWS)
        item_abspath = os.path.join(self._data_abspath, relpath)
        return item_abspath

    def _create_structure(self):
        """Create necessary structure to hold a dataset."""

        # Ensure that the specified path does not exist and create it.
        if os.path.exists(self._abspath):
            raise(StorageBrokerOSError(
                "Path already exists: {}".format(self._abspath)
            ))

        # Make sure the parent directory exists.
        parent, _ = os.path.split(self._abspath)
        if not os.path.isdir(parent):
            raise(StorageBrokerOSError(
                "No such directory: {}".format(parent)))

        os.mkdir(self._abspath)

        # Create more essential subdirectories.
        for abspath in self._essential_subdirectories:
            if not os.path.isdir(abspath):
                os.mkdir(abspath)

    def put_item(self, fpath, relpath):
        """Put item with content from fpath at relpath in dataset.

        Missing directories in relpath are created on the fly.

        :param fpath: path to the item on disk
        :param relpath: relative path name given to the item in the dataset as
                        a handle, i.e. a Unix-like relpath
        :returns: the handle given to the item
        """

        # Define the destination path and make any missing parent directories.
        osrelpath = handle_to_osrelpath(relpath, IS_WINDOWS)
        dest_path = os.path.join(self._data_abspath, osrelpath)
        dirname = os.path.dirname(dest_path)
        mkdir_parents(dirname)

        # Copy the file across.
        shutil.copyfile(fpath, dest_path)

        return relpath

    def iter_item_handles(self):
        """Return iterator over item handles."""

        path = self._data_abspath
        path_length = len(path) + 1

        for dirpath, dirnames, filenames in os.walk(path):
            for fn in filenames:
                path = os.path.join(dirpath, fn)
                relative_path = path[path_length:]
                if IS_WINDOWS:
                    relative_path = windows_to_unix_path(relative_path)
                yield relative_path

    def add_item_metadata(self, handle, key, value):
        """Store the given key:value pair for the item associated with handle.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :param key: metadata key
        :param value: metadata value
        """
        if not os.path.isdir(self._metadata_fragments_abspath):
            os.mkdir(self._metadata_fragments_abspath)

        prefix = self._handle_to_fragment_absprefixpath(handle)
        fpath = prefix + '.{}.json'.format(key)

        with open(fpath, 'w') as fh:
            json.dump(value, fh)

    def get_item_metadata(self, handle):
        """Return dictionary containing all metadata associated with handle.

        In other words all the metadata added using the ``add_item_metadata``
        method.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :returns: dictionary containing item metadata
        """

        if not os.path.isdir(self._metadata_fragments_abspath):
            return {}

        prefix = self._handle_to_fragment_absprefixpath(handle)

        def list_abspaths(dirname):
            for f in os.listdir(dirname):
                yield os.path.join(dirname, f)

        files = [f for f in list_abspaths(self._metadata_fragments_abspath)
                 if f.startswith(prefix)]

        metadata = {}
        for f in files:
            key = f.split('.')[-2]  # filename: identifier.key.json
            with open(f) as fh:
                value = json.load(fh)
            metadata[key] = value

        return metadata

    def pre_freeze_hook(self):
        """Pre :meth:`dtoolcore.ProtoDataSet.freeze` actions.

        This method is called at the beginning of the
        :meth:`dtoolcore.ProtoDataSet.freeze` method.

        It may be useful for remote storage backends to generate
        caches to remove repetitive time consuming calls
        """
        allowed = set([v[0] for v in _STRUCTURE_PARAMETERS.values()])
        for d in os.listdir(self._abspath):
            if d not in allowed:
                msg = "Rogue content in base of dataset: {}".format(d)
                raise(DiskStorageBrokerValidationWarning(msg))

    def post_freeze_hook(self):
        """Post :meth:`dtoolcore.ProtoDataSet.freeze` cleanup actions.

        This method is called at the end of the
        :meth:`dtoolcore.ProtoDataSet.freeze` method.

        In the :class:`dtoolcore.storage_broker.DiskStorageBroker` it removes
        the temporary directory for storing item metadata fragment files.
        """
        if os.path.isdir(self._metadata_fragments_abspath):
            shutil.rmtree(self._metadata_fragments_abspath)

    def _list_historical_readme_keys(self):
        historical_readme_keys = []
        for name in os.listdir(self._abspath):
            if name.startswith("README.yml-"):
                key = os.path.join(self._abspath, name)
                historical_readme_keys.append(key)
        return historical_readme_keys
