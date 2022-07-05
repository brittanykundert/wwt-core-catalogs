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

from wwt_data_formats import indent_xml
from wwt_data_formats.enums import (
    Classification,
    Constellation,
    DataSetType,
    FolderType,
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
    by_alturl: Dict[str, str] = None

    def __init__(self):
        self.by_url = {}
        self.by_alturl = {}
        self.db_dir = BASEDIR / "imagesets"

        for path in self.db_dir.glob("*.xml"):
            f = Folder.from_file(path)
            for c in f.children:
                assert isinstance(c, ImageSet)
                self.add_imageset(c)

    def add_imageset(self, imgset: ImageSet):
        if imgset.url in self.by_url:
            warn(f"dropping duplicated imageset `{imgset.url}`")
            return self.by_url[imgset.url]

        main_url = self.by_alturl.get(imgset.url)
        if main_url:
            warn(
                f"tried to add altUrl imageset `{imgset.url}`; use `{main_url}` instead"
            )
            return self.by_url[main_url]

        if imgset.alt_url:
            main_url = self.by_alturl.get(imgset.alt_url)
            if main_url:
                warn(
                    f"duplicated AltUrl: {imgset.alt_url} => {main_url} AND {imgset.url}"
                )
            else:
                self.by_alturl[imgset.alt_url] = imgset.url

        self.by_url[imgset.url] = imgset
        return imgset

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
        place.update_constellation()

        if place.image_set is not None:
            place.image_set = idb.add_imageset(place.image_set)
        if place.foreground_image_set is not None:
            place.foreground_image_set = idb.add_imageset(place.foreground_image_set)
        if place.background_image_set is not None:
            place.background_image_set = idb.add_imageset(place.background_image_set)

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

        if info["data_set_type"] == "Sky":
            info["ra_hr"] = place.ra_hr
            info["dec_deg"] = place.dec_deg
        else:
            info["latitude"] = place.latitude
            info["longitude"] = place.longitude

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
                k.append(lat)
                lon = info.get("longitude")  # there is a busted dataset like this
                if lon is not None:
                    k.append(lon)

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


# add-alt-urls


def do_add_alt_urls(settings):
    """
    Implemented to migrate the Mars panorama URLs, which differ in `imagesets6`
    and the Mars Explore database.
    """
    idb = ImagesetDatabase()

    with open(settings.spec_path, "rt") as f:
        for line in f:
            old_url, new_url = line.strip().split()

            imgset = idb.by_url.get(new_url)
            if imgset is None:
                die(f"missing new-url `{new_url}`")

            if imgset.alt_url and imgset.alt_url != old_url:
                die(f"preexisting AltUrl `{imgset.alt_url}` for `{new_url}`")

            imgset.alt_url = old_url

    idb.rewrite()


# emit


def _emit_one(path: Path, is_preview: bool, idb: ImagesetDatabase, pdb: PlaceDatabase):
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
            if is_preview and v.startswith(
                "http://www.worldwidetelescope.org/wwtweb/catalog.aspx?W="
            ):
                catname = v.split("=")[1]
                catpath = BASEDIR / "catfiles" / f"{catname}.yml"

                if catpath.exists():
                    v = f"./{catname}.wtml"

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
    rel = "_rel" if is_preview else ""
    extension = "xml" if root_info.get("_is_xml", False) else "wtml"

    with open(f"{catname}{rel}.{extension}", "wt", encoding="utf-8") as stream:
        prettify(f.to_xml(), stream)
        print(f"wrote `{catname}{rel}.{extension}`")


def do_emit(settings):
    idb = ImagesetDatabase()
    pdb = PlaceDatabase()

    for path in (BASEDIR / "catfiles").glob("*.yml"):
        _emit_one(path, settings.preview, idb, pdb)


# emit-searchdata


def _parse_classification(text):
    text = text.replace(" ", "")
    if text == "OpenStarCluster":
        return Classification.OPEN_CLUSTER
    if text == "TripleStar":
        return Classification.MULTIPLE_STARS
    return Classification(text)


def _compute_constellation(ra_deg, dec_deg):
    pl = Place()
    pl.set_ra_dec(ra_deg / 15, dec_deg)
    return pl.constellation


def _scan_cat_file(settings, name, need_constellation=False):
    with open(os.path.join(settings.catdir, name + ".txt")) as f:
        for line in f:
            bits = line.rstrip().split("\t")
            info = {}
            info["n"] = bits[0]  # name
            info["c"] = _parse_classification(bits[1]).to_numeric()
            info["r_deg"] = float(bits[2])
            info["d_deg"] = float(bits[3])

            if len(bits) > 4:
                if len(bits[4]) and bits[4] != "NULL":
                    info["m"] = float(bits[4])  # magnitude

            # bits[5] is the constellation, but in the Messier and NGC catalogs
            # it is often totally incorrect. In other cases, some objects are
            # really right at the borders (e.g., IC2036, IC3031) and WWT's
            # algorithm yields a different answer than some traditional
            # classifications. Either way, it works best to always rederive the
            # constellation.

            if need_constellation:
                info["constellation"] = _compute_constellation(
                    info["r_deg"], info["d_deg"]
                )

            if len(bits) > 6:
                info["z"] = float(bits[6])  # zoom

            yield info


def do_emit_searchdata(settings):
    # First prep hash by constellation:

    by_const = {}

    def _keys():
        for c in Constellation:
            if c != Constellation.UNSPECIFIED:
                yield c.value
        yield "SolarSystem"
        yield "Constellations"

    for k in _keys():
        by_const[k] = []

    # Populate main bulk

    for cat in ("messier", "ngc", "ic", "commonstars", "bsc"):
        for info in _scan_cat_file(settings, cat, need_constellation=True):
            place_list = by_const[info["constellation"].value]
            del info["constellation"]
            place_list.append(info)

    # Special solar-system section, with hack to add Earth.

    place_list = by_const["SolarSystem"]
    for info in _scan_cat_file(settings, "ssobjects"):
        place_list.append(info)

        if info["n"] == "Venus":
            earth = dict(info)
            earth["n"] = "Earth"
            place_list.append(earth)

    # Special Constellations section.

    place_list = by_const["Constellations"]
    for info in _scan_cat_file(settings, "constellationlist"):
        place_list.append(info)

    # Transform into final structure

    for place_list in by_const.values():
        for pl in place_list:
            ra_deg = pl.pop("r_deg")
            pl["r"] = round(ra_deg / 15, 4)  # convert to hours!

            dec_deg = pl.pop("d_deg")
            pl["d"] = round(dec_deg, 4)

            zoom = pl.get("z")
            if zoom is not None:
                pl["z"] = round(zoom, 5)
            else:
                pl["z"] = -1

            mag = pl.get("m")
            if settings.pretty_json:
                # For comparison with my base file, for now:
                if mag is not None:
                    mag = round(mag, 1)
                    if int(mag) == mag:
                        mag = int(mag)
                    pl["m"] = mag
                else:
                    pl["m"] = 0
            elif mag is not None:
                del pl["m"]

            if settings.pretty_json:
                # For comparison with my base file, for now:
                pl["i"] = 2

    wrapper = {"Constellations": [{"name": k, "places": by_const[k]} for k in _keys()]}

    if settings.pretty_json:
        import json

        json.dump(wrapper, sys.stdout, indent=2, ensure_ascii=False, sort_keys=True)
    else:
        print("TODO: dump it")


# format-imagesets


def do_format_imagesets(_settings):
    idb = ImagesetDatabase()
    idb.rewrite()


# format-places


def do_format_places(_settings):
    pdb = PlaceDatabase()
    pdb.rewrite()


# ground-truth


def do_ground_truth(_settings):
    import requests

    for path in (BASEDIR / "catfiles").glob("*.yml"):
        with path.open("rt", encoding="utf-8") as f:
            info = yaml.load(f, yaml.SafeLoader)

        catname = os.path.splitext(os.path.basename(path))[0]

        if info.get("_is_xml", False):
            extension = "xml"
            letter = "X"
        else:
            extension = "wtml"
            letter = "W"

        url = (
            f"http://www.worldwidetelescope.org/wwtweb/catalog.aspx?{letter}={catname}"
        )
        filename = f"{catname}.{extension}"

        with requests.get(url, stream=True) as r:
            with open(filename, "wb") as f:
                shutil.copyfileobj(r.raw, f)

            print(f"wrote `{filename}`")


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
                c = idb.add_imageset(c)
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

    if settings.emit:
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


# replace-urls


def do_replace_urls(settings):
    """
    A specialized utility to update imageset URLs to new URLs, using the
    "AltUrl" mechanism to "save" the old URL, and updating references to them.

    Implemented for Mars panoramas but then I realized I had it backwards! So
    this is untested, but I think it should work.
    """
    idb = ImagesetDatabase()
    replacements = {}

    warn("untested!!!!!")
    warn("tweaking QuadTreeMap and WidthFactor for Mars panos!!!")

    # Imageset database  ...

    with open(settings.spec_path, "rt") as f:
        for line in f:
            old_url, new_url = line.strip().split()
            replacements[old_url] = new_url

            imgset = idb.by_url.pop(old_url, None)
            if imgset is None:
                die(f"missing old-url `{old_url}`")

            imgset.url = new_url
            imgset.alt_url = old_url
            imgset.quad_tree_map = "0123"
            imgset.width_factor = 1
            idb.by_url[new_url] = imgset

    # Place database ...

    pdb = PlaceDatabase()

    def do_one(pinfo, key):
        url = pinfo.get(key)
        if url:
            new_url = replacements.get(url)
            if new_url:
                pinfo[key] = new_url

    for pinfo in pdb.by_uuid.values():
        do_one(pinfo, "image_set_url")
        do_one(pinfo, "background_image_set_url")
        do_one(pinfo, "foreground_image_set_url")

    idb.rewrite()
    pdb.rewrite()

    # Catalog files ...

    def do_one(path: Path):
        with path.open("rt", encoding="utf-8") as f:
            root_info = yaml.load(f, yaml.SafeLoader)

        def replace_folder(info: dict):
            for index in range(len(info["children"])):
                spec = info["children"][index]

                if isinstance(spec, str):
                    if spec.startswith("imageset "):
                        url = spec[9:]
                        new_url = replacements.get(url)
                        if new_url:
                            info["children"][index] = f"imageset {new_url}"
                        f.children.append(idb.get_by_url(spec[9:]))
                else:
                    replace_folder(spec)

        replace_folder(root_info)
        write_one_yaml(path, root_info)

    for path in (BASEDIR / "catfiles").glob("*.yml"):
        do_one(path)


# report


def do_report(_settings):
    idb = ImagesetDatabase()
    print(f"number of imagesets: {len(idb.by_url)}")


# trace


def _trace_catfile(path: Path, pdb: PlaceDatabase, idb: ImagesetDatabase):
    # TODO: fairly redundant with "emit"

    with path.open("rt", encoding="utf-8") as f:
        root_info = yaml.load(f, yaml.SafeLoader)

    def trace_folder(info: dict):
        for spec in info["children"]:
            if isinstance(spec, str):
                if spec.startswith("imageset "):
                    imgset = idb.get_by_url(spec[9:])
                    imgset.rmeta.touched = True
                elif spec.startswith("place "):
                    place = pdb.reconst_by_id(spec[6:], idb)
                    if place.image_set is not None:
                        place.image_set.rmeta.touched = True
                    if place.background_image_set is not None:
                        place.background_image_set.rmeta.touched = True
                    if place.foreground_image_set is not None:
                        place.foreground_image_set.rmeta.touched = True
                else:
                    assert False, f"unexpected terse folder child `{spec}`"
            else:
                # Folder
                url = spec.get("url")

                if not spec["children"] and url:
                    if url.startswith(
                        "http://www.worldwidetelescope.org/wwtweb/catalog.aspx?W="
                    ):
                        catname = url.split("=")[1]
                        catpath = BASEDIR / "catfiles" / f"{catname}.yml"

                        if catpath.exists():
                            print(f"Recursing into `{catname}`", file=sys.stderr)
                            _trace_catfile(catpath, pdb, idb)
                        else:
                            print(
                                f"Skipping `{catname}` which does not seem to be managed by this framework",
                                file=sys.stderr,
                            )
                else:
                    trace_folder(spec)

    trace_folder(root_info)


def do_trace(_settings):
    idb = ImagesetDatabase()
    pdb = PlaceDatabase()

    for imgset in idb.by_url.values():
        imgset.rmeta.touched = False

    _trace_catfile(BASEDIR / "catfiles" / "exploreroot6.yml", pdb, idb)

    for imgset in idb.by_url.values():
        if imgset.rmeta.touched:
            continue

        u = imgset.url
        places = []

        for place in pdb.by_uuid.values():
            if u in (
                place.get("image_set_url"),
                place.get("foreground_image_set_url"),
                place.get("background_image_set_url"),
            ):
                places.append(place["_uuid"])

        if places:
            places = " ".join(places)
        else:
            places = "(no places)"

        print(f"{imgset.url}: {imgset.name} -- {places}")


# generic driver


def entrypoint():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")

    add_alt_urls = subparsers.add_parser("add-alt-urls")
    add_alt_urls.add_argument(
        "spec_path", metavar="TEXT-PATH", help="Path to text file of URLs to update"
    )

    emit = subparsers.add_parser("emit")
    emit.add_argument(
        "--preview", action="store_true", help="Emit relative-URL files for previewing"
    )

    emit_searchdata = subparsers.add_parser("emit-searchdata")
    emit_searchdata.add_argument(
        "--pretty-json", action="store_true", help="Emit as indented JSON"
    )
    emit_searchdata.add_argument(
        "catdir",
        metavar="DIR-PATH",
        help="Directory with catalog files",
    )

    _format_imagesets = subparsers.add_parser("format-imagesets")
    _format_places = subparsers.add_parser("format-places")
    _ground_truth = subparsers.add_parser("ground-truth")

    ingest = subparsers.add_parser("ingest")
    ingest.add_argument(
        "--emit",
        action="store_true",
        help='Emit a new "catfile" YAML for the ingested folder',
    )
    ingest.add_argument(
        "wtml", metavar="WTML-PATH", help="Path to a catalog WTML file to ingest"
    )

    prettify = subparsers.add_parser("prettify")
    prettify.add_argument(
        "xml", metavar="XML-PATH", help="Path to an XML file to prettify"
    )

    replace_urls = subparsers.add_parser("replace-urls")
    replace_urls.add_argument(
        "spec_path", metavar="TEXT-PATH", help="Path to text file of URLs to update"
    )

    _report = subparsers.add_parser("report")
    _trace = subparsers.add_parser("trace")

    settings = parser.parse_args()

    if settings.subcommand is None:
        die("you must specify a subcommand", prefix="usage error:")
    elif settings.subcommand == "add-alt-urls":
        do_add_alt_urls(settings)
    elif settings.subcommand == "emit":
        do_emit(settings)
    elif settings.subcommand == "emit-searchdata":
        do_emit_searchdata(settings)
    elif settings.subcommand == "format-imagesets":
        do_format_imagesets(settings)
    elif settings.subcommand == "format-places":
        do_format_places(settings)
    elif settings.subcommand == "ground-truth":
        do_ground_truth(settings)
    elif settings.subcommand == "ingest":
        do_ingest(settings)
    elif settings.subcommand == "prettify":
        do_prettify(settings)
    elif settings.subcommand == "replace-urls":
        do_replace_urls(settings)
    elif settings.subcommand == "report":
        do_report(settings)
    elif settings.subcommand == "trace":
        do_trace(settings)
    else:
        die(f"unknown subcommand `{settings.subcommand}`", prefix="usage error:")


if __name__ == "__main__":
    entrypoint()
