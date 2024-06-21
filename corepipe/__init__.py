# Copyright the WorldWide Telescope project
# Licensed under the MIT License.

"""
Pipeline ingest framework.
"""

from .base import IMAGE_SOURCE_CLASS_LOADERS, PIPELINE_IO_LOADERS


def _load_local_pio(config):
    from .local_io import LocalPipelineIo

    return LocalPipelineIo._new_from_config(config)


PIPELINE_IO_LOADERS["local"] = _load_local_pio


def _load_azure_blob_pio(config):
    from .azure_io import AzureBlobPipelineIo

    return AzureBlobPipelineIo._new_from_config(config)


PIPELINE_IO_LOADERS["azure-blob"] = _load_azure_blob_pio


def _load_astropix_image_source_class():
    from .astropix import AstroPixImageSource

    return AstroPixImageSource


IMAGE_SOURCE_CLASS_LOADERS["astropix"] = _load_astropix_image_source_class


def _load_djangoplicity_image_source_class():
    from .djangoplicity import DjangoplicityImageSource

    return DjangoplicityImageSource


IMAGE_SOURCE_CLASS_LOADERS["djangoplicity"] = _load_djangoplicity_image_source_class
