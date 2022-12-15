# wwt-core-catalogs

The purpose of this repository is to manage the WTML files that catalog WWT's
core data holdings.


## Approach

The ultimate goal of this repo is create a group of WTML and XML files that can
be uploaded directly to WWT's cloud storage. These files are made available
through the `catalog.aspx` API endpoint: the file `exploreroot6.wtml` is
downloaded from

#### http://worldwidetelescope.org/wwtweb/catalog.aspx?W=exploreroot6

and so on.

Each of these WTML files defines a collection consisting of at least one
"folder" that may contain imagesets, "places", references to other folders, or
directly included nested folders, among other things. The structure of each
collection is defined by a YAML file in `catfiles/`; e.g.
`catfiles/exploreroot6.yml` defines the structure of the `exploreroot6.wtml`
output.

The particular names and relationships between various folders are longstanding
conventions that can't be changed lightly. Backwards compatibility is also an
issue. For instance, version 5.x clients load up `exploreroot.wtml` (NB: no "6")
and from thence files like `mars.wtml`, so these can't contain HiPS datasets
since the clients will reject the unrecognized dataset types.

In the folder YAML, `children` is a list of folder contents. Each entry can be:

- A dict, indicating a sub-folder
- A string of the form `imageset <URL>`, indicating the imageset associated with
  the given image data URL
- A string of the form `place <UUID>`, indicating the Place associated with the
  given UUID

Places are defined in `places/*.yml`, organized by dataset type and longitude
for convenience. Each item in a place's YAML dictionary corresponds fairly
directly to a WTML XML attribute or child element, except imagesets are once
again referenced by URL. Places are assigned random UUIDv4s because there's no
entirely sensible way to uniquely distinguish places, since their coordinates
matter a lot and those are floating-point numbers. The UUIDs aren't exposed
outside of this repo.

Imagesets are defined in `imagesets/*.xml`, organized by dataset type, reference
frame, and bandpass. Imagesets are uniquely identified by their data URLs. The
XML contents correspond directly to the WTML imageset representation.

In the current model there are two root catalog files: `exploreroot6.wtml` and
`imagesets6.wtml`. All other WTMLs are reachable from `exploreroot6`, which
populates the root of the client's "Explore" ribbon. The `imagesets6` file
defines the built-in imagesets accessible from the `Imagery:` dropdown. So it
shouldn't get too large.

The directory `sad_imagesets/` contains a structure like `imagesets/`, but with
known imagesets that shouldn't be used due to bad coordinates and the like.
Known problematic datasets should be moved there so that we don't lose track of
them. Add a `_Reason` attribute documenting why the imageset has a problem.
Sometimes the underlying data could in principle be rescued (e.g. the astrometry
of a study is just poor), sometimes not really (a planetary map is backwards).


## Driver

Operations are driven from the script `./cattool.py`, which has as Git-like
subcommand interface.

### `cattool emit`

The `emit` subcommand emits the WTML and XML files into the current directory.
This traces through the different collection definititions, expanding out XML
for places and imagesets. The `--preview` option emits `foo_rel.wtml` files with
relative URLs for the folder cross-references, allowing local previewing of the
catalog files using `wwtdatatool serve`.

(To preview locally in the webclient, you must edit `dataproxy/Places.js` to
replace the reference to `webclient-explore-root.wtml` to a locally-served
version. The webclient gets confused if you try to load `exploreroot6` as a
separate collection.)

(To preview locally in the Windows client, you also need to run a hacked custom
build, or maybe to monkey with your client's local cache.)

When a data update is ready to issue, these files should be able to be uploaded
directly into the `catalog` blob container of the `wwtfiles` storage account.
This can be done from the command line with something like this:

```sh
export AZURE_STORAGE_CONNECTION_STRING=secret-for-wwtfiles
az storage azcopy blob upload -c catalog -s jwst.wtml
```

After that:

- the `wwt6_login.txt` file needs to be updated to trigger the clients to pull
  down the new data,
- the Windows release datafiles cabinet needs to be updated to include the new
  data, and
- a new Windows release needs to be made including the updated cabinet

When the `imagesets6.xml` file is updated, the `builtin-image-sets.wtml` file
should be updated as well:

```sh
export AZURE_STORAGE_CONNECTION_STRING=secret-for-wwtwebstatic
az storage azcopy blob upload -c '$web' -s imagesets6.xml -d engine/assets/builtin-image-sets.wtml
```

The `wwtweb-prod` CDN endpoint should then have that path purged. (Probably we
should stop updating this file and change our code to use `imagesets6` instead,
but that migration would be a bit of a hassle.)


### `cattool emit-searchdata`

The `emit-searchdata` subcommand emits a JavaScript/JSON data file used as a
search index by the web client. It indexes not only the sky-based image sets,
but also several catalogs of well-known stars and galaxies (e.g., the Messier
and Bright Star catalogs).

The command takes one argument, which is the name of a directory containing
supporting catalog data files. These can be pulled off of the `catalog` blob
container of the `wwtfiles` storage account, or downloaded like so:

```sh
for c in bsc commonstars constellationlist ic messier ngc ssobjects ; do
  curl -fsSL "http://worldwidetelescope.org/wwtweb/catalog.aspx?Q=$c" -o $c.txt
done
```

After downloading, one should currently patch some of the files with the patch
`assets/repair-catalogs.patch` stored in this repo. The more sensible thing
would be to actually fix the server-side catalogs, but maintaining/regenerating
them is a bigger project than I want to take on right now (PKGW, July 2022).
Note that "fixing" is a bit ill-defined; the patch updates the BSC to add
RA/Decs to items in the HR catalog that are now known to be novae, etc., which
should maybe be removed instead. (Historical webclient behavior, though, was to
provide these sources in a way that wedges the viewer if you try to seek to
one.)

The `--pretty-json` option causes the data to be emitted to stdout as indented,
prettified JSON, which is most convenient for diffing and understanding the
detailed output. Without this option, the output is essentially minified
JavaScript, emitted using [pyjson5] and some manually-inserted shims.

[pyjson5]: https://github.com/Kijewski/pyjson5

The non-pretty output is currently put into production by uploading it into the
path `$web/data/searchdata_v2.min.js` path of the `wwtwebstatic` storage account
on Azure. A CDN purge will be needed to update the search data for the
production webclient. The upload can be done from the CLI with:

```sh
export AZURE_STORAGE_CONNECTION_STRING=secret-for-wwtwebstatic
az storage azcopy blob upload -c '$web' -s searchdata_v2.min.js -d data/
```

### `cattool ingest <WTML>`

This reads a WTML file and updates `places/`, and `imagesets/` with its data
contents. If you have a WTML defining new datasets to include, use this.

With the `--prepend-to=FILENAME` option, this command will update an existing
catalog template file to include the newly-ingested imagery at its beginning.
For instance, the command:

```sh
./cattool.py ingest jwst_fgs_preview.wtml --prepend-to=catfiles/jwst.yml
```

will take the new images and places defined in the file `jwst_fgs_preview.wtml`,
incorporate their definitions into the database, and add the new images at the
beginning of the `jwst` catalog.

With the `--emit` option, this command will create a wholly new catalog template
in `catfiles/` mirroring the input WTML's structure. You'll only want this
option if you're importing a substantial, new image collection.

### `cattool report`

Report the number of imagesets in the database.

### `cattool trace`

Trace down from `exploreroot6` and `imagesets6` to search for imagesets that are
not referenced from any WTML collections. This indicates the presence of a known
imageset that isn't accessible by the clients (with a small number of known
false positives).

### `cattool format-imagesets`

Read and rewrite the files in `imagesets/`, applying the system's organization
and normalization. E.g., if you edit an imageset's bandpass setting, it will
move into a new file.

### `cattool format-places`

Like `format-imagesets`, but for places.

### `cattool ground-truth`

For each of the catalogs currently being managed by this repo, download the
version that's currently being served by the production server and save it into
the current directory. You can combine this with a temporary Git repository or
other diffing solution in order to review the updates that you'll be uploading.

### `cattool prettify <XML>`

Rewrite an XML file in "prettified" format, assuming that elements have lots of
attributes. This is a low-level utility.

### `cattool replace-urls`

A specialized, untested utility intended to update imageset URLs in a compatible
way by using WWT's AltUrl support.

### `cattool add-alt-urls`

A specialized utility to add AltUrl attributes to many imagesets at once.


## See also

The [`wwt-hips-list-importer`][hips] repo contains a script for generating the
`hips.wtml` catalog.

[hips]: https://github.com/WorldWideTelescope/wwt-hips-list-importer
