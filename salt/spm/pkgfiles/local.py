"""
This module allows SPM to use the local filesystem to install files for SPM.

.. versionadded:: 2015.8.0
"""


import errno
import logging
import os
import os.path
import re

import salt.syspaths
import salt.utils.files
import salt.utils.stringutils

# Get logging started
log = logging.getLogger(__name__)
FILE_TYPES = ("c", "d", "g", "l", "r", "s", "m")
# c: config file
# d: documentation file
# g: ghost file (i.e. the file contents are not included in the package payload)
# l: license file
# r: readme file
# s: SLS file
# m: Salt module


def init(**kwargs):
    """
    Initialize the directories for the files
    """
    formula_path = __opts__["formula_path"]
    pillar_path = __opts__["pillar_path"]
    reactor_path = __opts__["reactor_path"]
    spm_module_path = __opts__["spm_module_path"]
    for dir_ in (formula_path, pillar_path, reactor_path, spm_module_path):
        if not os.path.exists(dir_):
            os.makedirs(dir_)
    return {
        "formula_path": formula_path,
        "pillar_path": pillar_path,
        "reactor_path": reactor_path,
        "spm_module_path": spm_module_path,
    }


def map_path(path, formula, parent_dir=None, conn=None):
    """
    Translate a path from the SPM into a filesystem path.
    """
    if conn is None:
        conn = init()

    _parent_dir = parent_dir or "{}/".format(
        formula.get("top_level_dir", formula.get("name", ""))
    )

    if not path.startswith(_parent_dir):
        return (None, None)

    trimmed_path = path.replace(_parent_dir, "", 1)
    file_path = trimmed_path
    base_path = conn["formula_path"]

    if trimmed_path == "FORMULA":
        base_path = None
        file_path = None

    elif trimmed_path.startswith("_pillar/"):
        base_path = conn["pillar_path"]
        file_path = re.sub(r"^_pillar/+", "", trimmed_path)

    elif trimmed_path.startswith("_"):
        file_path = trimmed_path[1:]
        node_type = str(__opts__.get("spm_node_type"))
        if node_type not in ("master", "minion"):
            log.error('Configuration value "spm_node_type" is not set')
            file_path = None
            base_path = None
        else:
            # Module files are distributed via extmods directory
            base_path = conn["spm_module_path"]

    elif trimmed_path == "pillar.example":
        # Pillars are automatically put in the pillar_path
        base_path = conn["pillar_path"]
        file_path = "{}.sls.orig".format(formula["name"])

    elif formula["name"].endswith("-conf"):
        # Configuration files go into /etc/salt/
        base_path = salt.syspaths.CONFIG_DIR

    elif formula["name"].endswith("-reactor"):
        # Reactor files go into /srv/reactor/
        base_path = conn["reactor_path"]

    return base_path, file_path


def check_existing(package, pkg_files, formula_def, conn=None):
    """
    Check the filesystem for existing files
    """
    _ = package  # Unused

    existing_files = []
    for member in pkg_files:
        if member.isdir():
            continue

        log.debug("Checking if %s:%s is already installed ...", package, member.name)
        (base_path, file_path) = map_path(member.name, formula_def, conn=conn)
        if not base_path or not file_path:
            log.warning("%s not in top level directory", member.name)
            continue
        new_path = os.path.sep.join((base_path, file_path))

        if os.path.exists(new_path):
            existing_files.append(new_path)
            if not __opts__["force"]:
                log.error("%s already exists, not installing", new_path)

    return existing_files


def install_file(package, formula_tar, member, formula_def, conn=None):
    """
    Install a single file to the file system
    """
    if member.name == package:
        return False

    (base_path, file_path) = map_path(member.name, formula_def, conn=conn)
    if not base_path or not file_path:
        log.warning("%s not in top level directory, not installing", member.name)
        return False
    new_path = os.path.sep.join((base_path, file_path))
    member.name = file_path
    log.debug("Installing package file %s to %s", member.name, new_path)
    formula_tar.extract(member, base_path)
    return base_path


def remove_file(path, conn=None):
    """
    Remove a single file from the file system
    """
    if conn is None:
        conn = init()

    log.debug("Removing package file %s", path)
    try:
        os.remove(path)
    except OSError as err:
        if errno.ENOENT != err.errno:
            raise err


def hash_file(path, hashobj, conn=None):
    """
    Get the hexdigest hash value of a file
    """
    if os.path.isdir(path):
        return ""

    try:
        with salt.utils.files.fopen(path, "rb") as f:
            hashobj.update(salt.utils.stringutils.to_bytes(f.read()))
            return hashobj.hexdigest()
    except OSError as err:
        if errno.ENOENT == err.errno:
            return ""
        else:
            raise err


def path_exists(path):
    """
    Check to see whether the file already exists
    """
    return os.path.exists(path)


def path_isdir(path):
    """
    Check to see whether the file already exists
    """
    return os.path.isdir(path)
