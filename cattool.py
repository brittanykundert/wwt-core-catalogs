#! /usr/bin/env python
#
# Copyright 2022 the .NET Foundation
# Licensed under the MIT License

"""
Tool for working with the WWT core dataset catalog.
"""

import argparse
from io import BytesIO
import math
import os.path
from pathlib import Path
import re
import shutil
import sys
from typing import List, Dict
import uuid
from xml.etree import ElementTree as etree
import yaml

from wwt_data_formats import indent_xml, write_xml_doc
from wwt_data_formats.enums import (
    Bandpass,
    Classification,
    Constellation,
    DataSetType,
    FolderType,
    ProjectionType,
)
from wwt_data_formats.folder import Folder
from wwt_data_formats.imageset import ImageSet
from wwt_data_formats.place import Place


BASEDIR = Path(os.path.dirname(__file__))


def die(text, prefix="fatal error:", exitcode=1):
    print(prefix, text, file=sys.stderr)
    sys.exit(exitcode)


def warn(text):
    print("warning:", text, file=sys.stderr)


def write_multi_yaml(path, docs):
    with open(path, "wt", encoding="utf-8") as f:
        yaml.dump_all(
            docs,
            stream=f,
            allow_unicode=True,
            sort_keys=True,
            indent=2,
        )


def write_one_yaml(path, doc):
    with open(path, "wt", encoding="utf-8") as f:
        yaml.dump(
            doc,
            stream=f,
            allow_unicode=True,
            sort_keys=True,
            indent=2,
        )


# Imageset database


class ImagesetDatabase(object):
    db_dir: Path = None
    by_url: Dict[str, ImageSet] = None

    def __init__(self):
        self.by_url = {}
        self.db_dir = BASEDIR / "imagesets"

        for path in self.db_dir.glob("*.xml"):
            f = Folder.from_file(path)
            for c in f.children:
                assert isinstance(c, ImageSet)
                self.add_imageset(c)

    def add_imageset(self, imgset: ImageSet):
        if imgset.url in self.by_url:
            warn(f"dropping duplicated imageset `{imgset.url}`")
            return

        self.by_url[imgset.url] = imgset

    def get_by_url(self, url: str) -> ImageSet:
        return self.by_url[url]

    def rewrite(self):
        by_key = {}

        for imgset in self.by_url.values():
            k = [str(imgset.data_set_type.value)]
            if imgset.reference_frame is not None and imgset.reference_frame != "Sky":
                k.append(imgset.reference_frame)
            if imgset.band_pass is not None:
                k.append(str(imgset.band_pass.value))

            key = "_".join(k).lower()
            by_key.setdefault(key, []).append(imgset)

        tempdir = Path(str(self.db_dir) + ".new")
        tempdir.mkdir()

        for key, imgsets in by_key.items():
            f = Folder(name=key)
            f.children = sorted(imgsets, key=lambda s: s.url)

            with (tempdir / (key + ".xml")).open("wt", encoding="utf-8") as stream:
                prettify(f.to_xml(), stream)

        olddir = Path(str(self.db_dir) + ".old")
        self.db_dir.rename(olddir)
        tempdir.rename(self.db_dir)
        shutil.rmtree(olddir)


# Place database


class PlaceDatabase(object):
    db_dir: Path = None
    by_uuid: Dict[str, dict] = None

    def __init__(self):
        self.by_uuid = {}
        self.db_dir = BASEDIR / "places"

        for path in self.db_dir.glob("*.yml"):
            with path.open("rt", encoding="utf-8") as f:
                for info in yaml.load_all(f, yaml.SafeLoader):
                    self.by_uuid[info["_uuid"]] = info

    def ingest_place(self, place: Place, idb: ImagesetDatabase):
        if place.image_set is not None:
            idb.add_imageset(place.image_set)
        if place.foreground_image_set is not None:
            idb.add_imageset(place.foreground_image_set)
        if place.background_image_set is not None:
            idb.add_imageset(place.background_image_set)

        new_id = str(uuid.uuid4())
        info = {"_uuid": new_id}

        if place.angle != 0:
            info["angle"] = place.angle

        if place.angular_size != 0:
            info["angular_size"] = place.angular_size

        if place.annotation:
            info["annotation"] = place.annotation

        if place.background_image_set:
            info["background_image_set_url"] = place.background_image_set.url

        if place.classification:
            info["classification"] = place.classification.value

        if place.constellation:
            info["constellation"] = place.constellation.value

        info["data_set_type"] = place.data_set_type.value

        if place.dec_deg != 0:
            info["dec_deg"] = place.dec_deg

        if place.description:
            info["description"] = place.description

        if place.distance != 0:
            info["distance"] = place.distance

        if place.dome_alt != 0:
            info["dome_alt"] = place.dome_alt

        if place.dome_az != 0:
            info["dome_az"] = place.dome_az

        if place.foreground_image_set:
            info["foreground_image_set_url"] = place.foreground_image_set.url

        if place.image_set:
            info["image_set_url"] = place.image_set.url

        if place.latitude != 0:
            info["latitude"] = place.latitude

        if place.longitude != 0:
            info["longitude"] = place.longitude

        if place.magnitude != 0:
            info["magnitude"] = place.magnitude

        if place.msr_community_id != 0:
            info["msr_community_id"] = place.msr_community_id

        if place.msr_component_id != 0:
            info["msr_component_id"] = place.msr_component_id

        info["name"] = place.name

        if place.opacity != 100:
            info["opacity"] = place.opacity

        if place.permission != 0:
            info["permission"] = place.permission

        if place.ra_hr != 0:
            info["ra_hr"] = place.ra_hr

        if place.rotation_deg != 0:
            info["rotation_deg"] = place.rotation_deg

        if place.thumbnail:
            info["thumbnail"] = place.thumbnail

        if place.zoom_level != 0:
            info["zoom_level"] = place.zoom_level

        self.by_uuid[new_id] = info
        return new_id

    def reconst_by_id(self, pid: str, idb: ImagesetDatabase) -> Place:
        info = self.by_uuid[pid]

        place = Place()

        v = info.get("angle")
        if v is not None:
            place.angle = v

        v = info.get("angular_size")
        if v is not None:
            place.angular_size = v

        v = info.get("annotation")
        if v:
            place.annotation = v

        u = info.get("background_image_set_url")
        if u:
            place.background_image_set = idb.get_by_url(u)

        v = info.get("classification")
        if v:
            place.classification = Classification(v)

        v = info.get("constellation")
        if v:
            place.constellation = Constellation(v)

        place.data_set_type = DataSetType(info["data_set_type"])

        v = info.get("dec_deg")
        if v is not None:
            place.dec_deg = v

        v = info.get("description")
        if v:
            place.description = v

        v = info.get("distance")
        if v is not None:
            place.distance = v

        v = info.get("dome_alt")
        if v is not None:
            place.dome_alt = v

        v = info.get("dome_az")
        if v is not None:
            place.dome_az = v

        u = info.get("foreground_image_set_url")
        if u:
            place.foreground_image_set = idb.get_by_url(u)

        u = info.get("image_set_url")
        if u:
            place.image_set = idb.get_by_url(u)

        v = info.get("latitude")
        if v is not None:
            place.latitude = v

        v = info.get("longitude")
        if v is not None:
            place.longitude = v

        v = info.get("magnitude")
        if v is not None:
            place.magnitude = v

        v = info.get("msr_community_id")
        if v is not None:
            place.msr_community_id = v

        v = info.get("msr_component_id")
        if v is not None:
            place.msr_component_id = v

        place.name = info["name"]

        v = info.get("opacity")
        if v is not None:
            place.opacity = v

        v = info.get("permission")
        if v is not None:
            place.permission = v

        v = info.get("ra_hr")
        if v is not None:
            place.ra_hr = v

        v = info.get("rotation_deg")
        if v is not None:
            place.rotation_deg = v

        v = info.get("thumbnail")
        if v:
            place.thumbnail = v

        v = info.get("zoom_level")
        if v is not None:
            place.zoom_level = v

        return place

    def rewrite(self):
        by_key = {}

        for info in self.by_uuid.values():
            k = [info["data_set_type"]]

            ra = info.get("ra_hr")
            if ra is not None:
                ra = int(math.floor(ra)) % 24
                k.append(f"ra{ra:02d}")

            lon = info.get("longitude")
            if lon is not None:
                lon = (int(math.floor(lon)) // 10) * 10
                k.append(f"lon{lon:03d}")

            key = "_".join(k).lower()
            by_key.setdefault(key, []).append(info)

        def sortkey(info):
            k = []

            u = info.get("foreground_image_set_url")
            if u is not None:
                k += u

            u = info.get("image_set_url")
            if u is not None:
                k += u

            u = info.get("background_image_set_url")
            if u is not None:
                k += u

            dec = info.get("dec_deg")
            if dec is not None:
                k += [dec, info["ra_hr"]]

            lat = info.get("latitude")
            if lat is not None:
                k += [lat, info["longitude"]]

            k += [info["name"]]
            return tuple(k)

        tempdir = Path(str(self.db_dir) + ".new")
        tempdir.mkdir()

        for key, infos in by_key.items():
            infos = sorted(infos, key=sortkey)
            write_multi_yaml(tempdir / (key + ".yml"), infos)

        olddir = Path(str(self.db_dir) + ".old")
        self.db_dir.rename(olddir)
        tempdir.rename(self.db_dir)
        shutil.rmtree(olddir)


# emit


def _emit_one(path: Path, idb: ImagesetDatabase, pdb: PlaceDatabase):
    with path.open("rt", encoding="utf-8") as f:
        root_info = yaml.load(f, yaml.SafeLoader)

    def reconst_folder(info: dict):
        f = Folder()
        f.browseable = info["browseable"]

        v = info.get("group")
        if v:
            f.group = v

        v = info.get("msr_community_id")
        if v:
            f.msr_community_id = v

        v = info.get("msr_component_id")
        if v:
            f.msr_component_id = v

        f.name = info["name"]

        v = info.get("permission")
        if v:
            f.permission = v

        f.searchable = info["searchable"]

        v = info.get("sub_type")
        if v:
            f.sub_type = v

        v = info.get("thumbnail")
        if v:
            f.thumbnail = v

        v = info.get("type")
        if v:
            f.type = FolderType(v)

        v = info.get("url")
        if v:
            f.url = v

        f.children = []

        for spec in info["children"]:
            if isinstance(spec, str):
                if spec.startswith("imageset "):
                    f.children.append(idb.get_by_url(spec[9:]))
                elif spec.startswith("place "):
                    f.children.append(pdb.reconst_by_id(spec[6:], idb))
                else:
                    assert False, f"unexpected terse folder child `{spec}`"
            else:
                f.children.append(reconst_folder(spec))

        return f

    f = reconst_folder(root_info)
    catname = os.path.splitext(os.path.basename(path))[0]

    with open(f"{catname}.wtml", "wt", encoding="utf-8") as stream:
        prettify(f.to_xml(), stream)
        print(f"wrote `{catname}.wtml`")


def do_emit(_settings):
    idb = ImagesetDatabase()
    pdb = PlaceDatabase()

    for path in (BASEDIR / "catfiles").glob("*.yml"):
        _emit_one(path, idb, pdb)


# format-imagesets


def do_format_imagesets(_settings):
    idb = ImagesetDatabase()
    idb.rewrite()


# format-places


def do_format_places(_settings):
    pdb = PlaceDatabase()
    pdb.rewrite()


# ingest


def do_ingest(settings):
    catname = os.path.splitext(os.path.basename(settings.wtml))[0]
    f = Folder.from_file(settings.wtml)
    idb = ImagesetDatabase()
    pdb = PlaceDatabase()

    def folder_to_yaml(f):
        info = {}
        info["browseable"] = f.browseable

        if f.group:
            info["group"] = f.group

        if f.msr_community_id:
            info["msr_community_id"] = f.msr_community_id

        if f.msr_component_id:
            info["msr_component_id"] = f.msr_component_id

        info["name"] = f.name

        if f.permission:
            info["permission"] = f.permission

        info["searchable"] = f.searchable

        if f.sub_type:
            info["sub_type"] = f.sub_type

        if f.thumbnail:
            info["thumbnail"] = f.thumbnail

        info["type"] = f.type.value

        if f.url:
            info["url"] = f.url

        children = []

        for c in f.children:
            if isinstance(c, ImageSet):
                idb.add_imageset(c)
                children.append(f"imageset {c.url}")
            elif isinstance(c, Place):
                pid = pdb.ingest_place(c, idb)
                children.append(f"place {pid}")
            elif isinstance(c, Folder):
                if not c.children and c.url:
                    print(f"Consider ingesting: {c.url}")
                children.append(folder_to_yaml(c))
            else:
                assert False, "unexpected folder item??"

        info["children"] = children
        return info

    info = folder_to_yaml(f)
    idb.rewrite()
    pdb.rewrite()
    write_one_yaml(f"catfiles/{catname}.yml", info)


# prettify - generic XML prettification


def prettify(xml_element, out_stream):
    """
    We use our wwt_data_formats pretty-print, then go back and split attributes
    onto their own lines, alphabetizing them.
    """

    START_ATTR_TAG_RE = re.compile(r"^(\s*)<([-_a-zA-Z0-9]+)\s")
    ATTR_RE = re.compile(r'^\s*(\w+)="([^"]*)"')
    ATTRS_DONE_RE = re.compile(r"^\s*(/?)>$")

    indent_xml(xml_element)

    doc = etree.ElementTree(xml_element)

    with BytesIO() as dest:
        doc.write(dest, encoding="UTF-8", xml_declaration=True)
        bytes = dest.getvalue()

    text = bytes.decode("utf-8")

    for line in text.splitlines():
        m = START_ATTR_TAG_RE.match(line)
        if m is None:
            print(line, file=out_stream)
        else:
            indent = m[1]
            tag = m[2]
            attr_text = line[m.end() :].rstrip()
            attrs = {}
            m_done = ATTRS_DONE_RE.match(attr_text)

            while m_done is None:
                m = ATTR_RE.match(attr_text)
                assert m is not None, f"error chomping attrs in `{line!r}`"

                attr_name = m[1]
                attr_val = m[2]
                assert attr_name not in attrs
                attrs[attr_name] = attr_val

                attr_text = attr_text[m.end() :]
                m_done = ATTRS_DONE_RE.match(attr_text)

            self_ending = bool(m_done[1])

            print(f"{indent}<{tag}", file=out_stream)

            for attr_name, attr_val in sorted(attrs.items()):
                print(f'{indent}  {attr_name}="{attr_val}"', file=out_stream)

            if self_ending:
                print(f"{indent}></{tag}>", file=out_stream)
            else:
                print(f"{indent}>", file=out_stream)


def do_prettify(settings):
    with open(settings.xml, "rt", encoding="utf-8-sig") as f:
        text = f.read()
        elem = etree.fromstring(text)
        prettify(elem, sys.stdout)


# generic driver


def entrypoint():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")

    _emit = subparsers.add_parser("emit")
    _format_imagesets = subparsers.add_parser("format_imagesets")
    _format_places = subparsers.add_parser("format_places")

    ingest = subparsers.add_parser("ingest")
    ingest.add_argument(
        "wtml", metavar="WTML-PATH", help="Path to a catalog WTML file to ingest"
    )

    prettify = subparsers.add_parser("prettify")
    prettify.add_argument(
        "xml", metavar="XML-PATH", help="Path to an XML file to prettify"
    )

    settings = parser.parse_args()

    if settings.subcommand is None:
        die("you must specify a subcommand", prefix="usage error:")
    elif settings.subcommand == "emit":
        do_emit(settings)
    elif settings.subcommand == "format-imagesets":
        do_format_imagesets(settings)
    elif settings.subcommand == "format-places":
        do_format_places(settings)
    elif settings.subcommand == "ingest":
        do_ingest(settings)
    elif settings.subcommand == "prettify":
        do_prettify(settings)
    else:
        die(f"unknown subcommand `{settings.subcommand}`", prefix="usage error:")


if __name__ == "__main__":
    entrypoint()
