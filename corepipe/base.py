# Copyright the WorldWide Telescope project
# Licensed under the MIT License.

"""
Base classes for the pipeline framework
"""

__all__ = """
CandidateInput
ImageSource
IMAGE_SOURCE_CLASS_LOADERS
NotActionableError
PipelineIo
PIPELINE_IO_LOADERS
""".split()

from abc import ABC, abstractmethod
from io import BytesIO
import os.path
import sys
from typing import BinaryIO, Iterable, Tuple
import yaml

import toasty


class NotActionableError(Exception):
    """
    Raised when an image is provided to the pipeline but for some reason we're
    not going to be able to get it into a WWT-compatible form.
    """

    def __init__(self, reason):
        super(NotActionableError, self).__init__(reason)


# This will be populated by other imports
PIPELINE_IO_LOADERS = {}


class PipelineIo(ABC):
    """
    An abstract base class for I/O relating to pipeline processing. An instance
    of this class might be used to fetch files from, and send them to, a cloud
    storage system like S3 or Azure Storage.
    """

    @abstractmethod
    def _export_config(self) -> dict:
        """
        Export this object's configuration for serialization.

        Returns
        -------
        A dictionary of settings that can be saved as YAML format. There should
        be a key named "_type" with a string value identifying the I/O
        implementation type.
        """

    def save_config(self, path: str):
        """
        Save this object's configuration to the specified filesystem path.
        """
        cfg = self._export_config()

        # The config contains secrets, so create it privately and securely.
        opener = lambda path, _mode: os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode=0o600
        )

        with open(path, "wt", opener=opener, encoding="utf8") as f:
            yaml.dump(cfg, f, yaml.SafeDumper)

    @classmethod
    @abstractmethod
    def _new_from_config(cls, config: dict) -> "PipelineIo":
        """
        Create a new instance of this class based on serialized configuration.

        Parameters
        ----------
        config : dict
            A dict of configuration that was created with ``_export_config``

        Returns
        -------
        A new instance of the class.
        """

    @classmethod
    def load_from_config(self, path: str) -> "PipelineIo":
        """
        Create a new I/O backend from saved configuration.

        Parameters
        ----------
        path : path-like
            The path where the configuration was saved.

        Returns
        -------
        A new instance implementing the PipelineIO abstract base class.
        """

        with open(path, "rt", encoding="utf8") as f:
            config = yaml.safe_load(f)

        ty = config.get("_type")
        loader = PIPELINE_IO_LOADERS.get(ty)
        if loader is None:
            raise Exception(f"unrecognized pipeline I/O storage type {ty!r}")

        return loader(config)

    @abstractmethod
    def check_exists(self, *path: Iterable[str]) -> bool:
        """
        Test whether an item at the specified path exists.

        Parameters
        ----------
        *path : strings
            The path to the item, intepreted as components in a folder
            hierarchy.

        Returns
        -------
        A boolean indicating whether the item in question exists.
        """

    @abstractmethod
    def get_item(self, *path: Iterable[str], dest: BinaryIO = None):
        """
        Fetch a file-like item at the specified path, writing its contents into
        the specified file-like object *dest*.

        Parameters
        ----------
        *path : strings
            The path to the item, intepreted as components in a folder
            hierarchy.
        dest : writeable file-like object
            The object into which the item's data will be written as bytes.

        Returns
        -------
        None.
        """

    @abstractmethod
    def put_item(self, *path: Iterable[str], source: BinaryIO = None):
        """
        Put a file-like item at the specified path, reading its contents from
        the specified file-like object *source*.

        Parameters
        ----------
        *path : strings
            The path to the item, intepreted as components in a folder
            hierarchy.
        source : readable file-like object
            The object from which the item's data will be read, as bytes.

        Returns
        -------
        None.
        """

    @abstractmethod
    def list_items(self, *path: Iterable[str]) -> Iterable[Tuple[str, bool]]:
        """
        List the items contained in the folder at the specified path.

        Parameters
        ----------
        *path : strings
            The path to the item, intepreted as components in a folder
            hierarchy.

        Returns
        -------
        An iterable of ``(stem, is_folder)``, where *stem* is the "basename" of
        an item contained within the specified folder and *is_folder* is a
        boolean indicating whether this item appears to be a folder itself.
        """


class ImageSource(ABC):
    """
    An abstract base class representing a source of images to be processed in
    the image-processing pipeline. An instance of this class might fetch images
    from an RSS feed or an AstroPix search.
    """

    @classmethod
    @abstractmethod
    def get_config_key(cls) -> str:
        """
        Get the name of the section key used for this source's configuration
        data.

        Returns
        -------
        A string giving a key name usable in a YAML file.
        """

    @classmethod
    @abstractmethod
    def deserialize(cls, data: dict) -> "ImageSource":
        """
        Create an instance of this class by deserializing configuration data.

        Parameters
        ----------
        data : dict-like object
            A dict-like object containing configuration items deserialized from
            a format such as JSON or YAML. The particular contents can vary
            depending on the implementation.

        Returns
        -------
        An instance of *cls*.
        """

    @abstractmethod
    def query_candidates(self) -> Iterable["CandidateInput"]:
        """
        Generate a sequence of candidate input images that the pipeline may want
        to process.

        Returns
        -------
        A generator that yields a sequence of `CandidateInput` instances.
        """

    @abstractmethod
    def fetch_candidate(
        self, unique_id: str, cand_data_stream: BinaryIO, cachedir: str
    ):
        """
        Download a candidate image and prepare it for processing.

        Parameters
        ----------
        unique_id : str
            The unique ID returned by the `CandidateInput` instance that was
            returned from the initial query.
        cand_data_stream : readable stream returning bytes
            A data stream returning the data that were saved when the candidate
            was queried (`CandidateInput.save()`).
        cachedir : path-like
            A path pointing to a local directory inside of which the full image
            data and metadata should be cached.
        """

    @abstractmethod
    def process(
        self,
        unique_id: str,
        cand_data_stream: BinaryIO,
        cachedir: str,
        builder: "toasty.builder.Builder",
    ):
        """
        Process an input into WWT format.

        Parameters
        ----------
        unique_id : str
            The unique ID returned by the :class:`toasty.pipeline.CandidateInput` instance
            that was returned from the initial query.
        cand_data_stream : readable stream returning bytes
            A data stream returning the data that were saved when the candidate
            was queried (:meth:`toasty.pipeline.CandidateInput.save`).
        cachedir : path-like
            A path pointing to a local directory inside of which the
            full image data and metadata should be cached.
        builder : `toasty.builder.Builder`
            State object for constructing the WWT data files.

        Notes
        -----
        Your image processor should run the tile cascade, if needed, but the
        caller will take care of emitting the ``index_rel.wtml`` file.
        """


class CandidateInput(ABC):
    """
    An abstract base class representing an image from one of our sources. If it
    has not been processed before, we will fetch its data and queue it for
    processing.
    """

    @abstractmethod
    def get_unique_id(self) -> str:
        """
        Get an ID for this image that will be unique in its `ImageSource`.

        Returns
        -------
        An identifier as a string. Should be limited to path-friendly
        characters, i.e. ASCII without spaces.
        """

    @abstractmethod
    def save(self, stream: BinaryIO):
        """
        Serialize candidate information for future processing

        Parameters
        ----------
        stream : writeable stream accepting bytes
           The stream into which the candidate information should be serialized.

        Raises
        ------
        May raise :exc:`toasty.pipeline.NotActionableError` if it turns out that this
        candidate is not one that can be imported into WWT.

        Returns
        -------
        None.
        """


# The PipelineManager class that orchestrates it all

# This will be populated by other imports
IMAGE_SOURCE_CLASS_LOADERS = {}


class PipelineManager(object):
    _config: dict = None
    _pipeio: PipelineIo = None
    _workdir: str = None
    _img_source: ImageSource = None

    def __init__(self, workdir: str):
        self._workdir = workdir
        self._pipeio = PipelineIo.load_from_config(
            self._path("toasty-store-config.yaml")
        )

    def _path(self, *path: Iterable[str]) -> str:
        return os.path.join(self._workdir, *path)

    def _ensure_dir(self, *path: Iterable[str]) -> str:
        path = self._path(*path)
        os.makedirs(path, exist_ok=True)
        return path

    def ensure_config(self) -> dict:
        if self._config is not None:
            return self._config

        self._ensure_dir()
        cfg_path = self._path("toasty-pipeline-config.yaml")

        if not os.path.exists(cfg_path):  # racey
            with open(cfg_path, "wb") as f:
                self._pipeio.get_item("toasty-pipeline-config.yaml", dest=f)

        with open(cfg_path, "rt", encoding="utf8") as f:
            config = yaml.safe_load(f)

        if config is None:
            raise Exception("no toasty-pipeline-config.yaml found in the storage")

        self._config = config
        return self._config

    def get_image_source(self) -> ImageSource:
        if self._img_source is not None:
            return self._img_source

        self.ensure_config()

        source_type = self._config.get("source_type")
        if not source_type:
            raise Exception("toasty pipeline configuration must have a source_type key")

        cls_loader = IMAGE_SOURCE_CLASS_LOADERS.get(source_type)
        if cls_loader is None:
            raise Exception(f"unrecognized image source type `{source_type}`")

        cls = cls_loader()
        cfg_key = cls.get_config_key()
        source_config = self._config.get(cfg_key)
        if source_config is None:
            raise Exception(
                f"no image source configuration key `{cfg_key}` in the config file"
            )

        self._img_source = cls.deserialize(source_config)
        return self._img_source

    def process_todos(self):
        from toasty.builder import Builder
        from toasty import par_util
        from toasty.pyramid import PyramidIO

        src = self.get_image_source()
        cand_dir = self._path("candidates")
        self._ensure_dir("cache_done")

        # Lame hack to tidy up output slightly
        par_util.SHOW_INFORMATIONAL_MESSAGES = False

        for uniq_id in os.listdir(self._path("cache_todo")):
            cachedir = self._path("cache_todo", uniq_id)
            outdir = self._path("processed", uniq_id)

            pio = PyramidIO(outdir, scheme="LXY", default_format="png")
            builder = Builder(pio)
            cdata = open(os.path.join(cand_dir, uniq_id), "rb")

            print(f"processing {uniq_id} ... ", end="")
            sys.stdout.flush()

            src.process(uniq_id, cdata, cachedir, builder)
            cdata.close()
            builder.write_index_rel_wtml()
            print("done")

            # Woohoo, done!
            os.rename(cachedir, self._path("cache_done", uniq_id))

    def publish(self):
        done_dir = self._ensure_dir("published")
        todo_dir = self._path("approved")
        pfx = todo_dir + os.path.sep

        for uniq_id in os.listdir(todo_dir):
            # If there's a index.wtml file, save it for last -- that will
            # indicate that this directory has uploaded fully successfully.

            filenames = os.listdir(os.path.join(todo_dir, uniq_id))

            try:
                index_index = filenames.index("index.wtml")
            except ValueError:
                pass
            else:
                temp = filenames[-1]
                filenames[-1] = "index.wtml"
                filenames[index_index] = temp

            print(f"publishing {uniq_id} ...")

            for filename in filenames:
                # Get the components of the item path relative to todo_dir.
                sub_components = [todo_dir, uniq_id, filename]
                p = os.path.join(*sub_components)
                assert p.startswith(pfx)

                with open(p, "rb") as f:
                    self._pipeio.put_item(*sub_components[1:], source=f)

            os.rename(os.path.join(todo_dir, uniq_id), os.path.join(done_dir, uniq_id))

    def ignore_rejects(self):
        rejects_dir = self._path("rejects")
        n = 0

        # maybe one day this will be JSON with data?
        flag_content = BytesIO(b"{}")

        for uniq_id in os.listdir(rejects_dir):
            print(f"ignoring {uniq_id} ...")
            self._pipeio.put_item(uniq_id, "skip.flag", source=flag_content)
            n += 1

        if n > 1:
            print()
            print(f"marked a total of {n} images to be permanently ignored")
