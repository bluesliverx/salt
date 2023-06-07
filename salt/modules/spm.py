"""
Salt Packager Manager (SPM) execution module.
"""
import errno
import logging
import os

import salt.config
import salt.syspaths
import salt.utils.data
import salt.utils.files
import salt.utils.yaml
from salt import spm
from salt.exceptions import SaltInvocationError

log = logging.getLogger(__name__)


class SPMModuleInterface(spm.SPMUserInterface):
    """
    Mocks the UI interface to retrieve status, etc.
    """

    def __init__(self):
        self.statuses = []
        self.errors = []

    def clear_output(self):
        self.statuses = []
        self.errors = []

    def status(self, msg):
        self.statuses.append(msg)

    def error(self, msg):
        self.errors.append(msg)

    def confirm(self, action):
        # Always allow actions
        pass


# The refresh/refresh_tag code is copied from salt core and modified for our purposes


def _refresh_tag():
    """
    Return the refresh_tag file location. This file is used to ensure that we don't
    refresh more than once (unless explicitly configured to do so).
    """
    return os.path.join(__opts__["cachedir"], "spm_refresh")


def _clear_refresh_tag():
    """
    Remove the refresh_tag file.
    """
    try:
        os.remove(_refresh_tag())
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            # Using __str__() here to get the fully-formatted error message
            # (error number, error message, path)
            log.warning("Encountered error removing refresh_tag: %s", exc.__str__())


def _get_spm_client_ui(verbose=False):
    # The config dir can be changed via command line parameter, but since we have no command line here
    # just use the default value and expand the user in it if needed.
    # The config file should match the value of SPMParser._config_filename_
    config_file_path = os.path.join(os.path.expanduser(salt.syspaths.CONFIG_DIR), "spm")
    config = salt.config.spm_config(config_file_path)

    # Store some values that are expected to be there but are not
    config["verbose"] = verbose
    config["assume_yes"] = True
    config["force"] = True
    # Always force a localfs cache, if this is a shared cache it breaks spm installs
    config["cache"] = "localfs"
    client = spm.SPMClient(SPMModuleInterface(), opts=config)
    return client, client.ui


def _list_repos(spm_client=None, spm_ui=None):
    """
    Utility method to list the available repository names, takes an optional spm_client and spm_ui to use.
    """
    if not spm_client:
        spm_client, spm_ui = _get_spm_client_ui()
    spm_client.run(["repo", "list"])
    return spm_ui.statuses or []


def _refresh_metadata(spm_client, spm_ui):
    _clear_refresh_tag()
    spm_client.run(["update_repo"])
    if len(spm_ui.errors) > 0:
        log.error(
            "Encountered an error while refreshing SPM repo metadata: %s",
            ", ".join(spm_ui.errors),
        )
        return False
    log.debug("Refreshed SPM repo metadata")
    spm_ui.clear_output()
    return True


def refresh_metadata():
    """
    Refresh SPM metdata for all configured repositories.

    CLI Example:

    .. code-block:: bash

        salt '*' spm.refresh_metadata
    """
    spm_client, spm_ui = _get_spm_client_ui()
    _refresh_metadata(spm_client, spm_ui)
    return True


def list_repos():
    """
    Lists all repos in the configured SPM repo dir.

    CLI Example:

    .. code-block:: bash

        salt '*' spm.list_repos
    """
    return _list_repos()


def get_repo(name):
    """
    Retrieves repo data for the specified repository name in the configured SPM repo dir.

    CLI Example:

    .. code-block:: bash

        salt '*' spm.get_repo foo
    """
    spm_client, spm_ui = _get_spm_client_ui()

    ret = [{}]

    def get_metadata(repo_name, repo_data):
        ret[0] = repo_data

    # The SPM client does not expose methods for retrieving the actual repo metadata
    # Once more fine-grained methods are implemented, replace this with actual SPM client calls
    spm_client._traverse_repos(get_metadata, repo_name=name)
    return ret[0]


def _get_repo_file(spm_client, repo_name):
    """
    Retrieves the actual file location for an SPM repo at the given location.
    """
    repos_dir = "{}.d".format(spm_client.opts["spm_repos_config"])
    return f"{os.path.join(repos_dir, repo_name)}.repo"


def mod_repo(name, **kwargs):
    """
    Creates or modifies an SPM repo file with the given options.

    CLI Example:

    .. code-block:: bash

        salt '*' spm.mod_repo foo url=https://foo/url
    """
    spm_client, spm_ui = _get_spm_client_ui()

    # Filter out __pub and saltenv-type options
    repo_data = {
        x: kwargs[x] for x in kwargs if not x.startswith("__") and x not in ("saltenv",)
    }

    if "url" not in repo_data:
        raise SaltInvocationError("A url was not specified for the repository")

    repofile = _get_repo_file(spm_client, name)
    if not os.path.exists(repofile):
        # Make sure the repo directory exists
        dir = os.path.dirname(repofile)
        if not os.path.exists(dir):
            raise SaltInvocationError(
                "The repo does not exist anspm ind needs to be created, but the configured spm_repos_config path does "
                "not exist: {}".format(dir)
            )

    # The SPM client doesn't provide an interface for updating repo files, so we work around it manually here
    contents = salt.utils.yaml.safe_dump({name: repo_data})
    with salt.utils.files.fopen(repofile, "w") as fileout:
        fileout.write(contents)

    return {repofile: repo_data}


def del_repo(name):
    """
    Deletes the SPM repo file for the specified repo

    CLI Example:

    .. code-block:: bash

        salt '*' spm.del_repo foo
    """
    spm_client, spm_ui = _get_spm_client_ui()

    repofile = _get_repo_file(spm_client, name)
    if not os.path.exists(repofile):
        raise SaltInvocationError(
            f"The repo file to be deleted does not exist: {repofile}"
        )
    os.remove(repofile)

    return f"Repo {name} file {repofile} has been removed"


def _list_repo_packages(
    pkg=None, exact_pkg=False, repo=None, latest=False, refresh=False
):
    spm_client, spm_ui = _get_spm_client_ui()

    if refresh:
        refresh_metadata()

    packages = []
    repo_metadata = spm_client._get_repo_metadata()
    for repo_name in repo_metadata:
        if not repo or repo == repo_name:
            url = (
                repo_metadata[repo_name]["info"]["url"]
                if "url" in repo_metadata[repo_name]["info"]
                else None
            )
            for repo_pkg in repo_metadata[repo_name]["packages"]:
                if not pkg or (
                    (exact_pkg and pkg == repo_pkg)
                    or (not exact_pkg and pkg in repo_pkg)
                ):
                    filename = repo_metadata[repo_name]["packages"][repo_pkg][
                        "filename"
                    ]
                    if url:
                        source = f"{url}/{filename}"
                    else:
                        source = filename
                    packages.append(
                        {
                            "name": repo_pkg,
                            "version": repo_metadata[repo_name]["packages"][repo_pkg][
                                "info"
                            ]["version"],
                            "release": repo_metadata[repo_name]["packages"][repo_pkg][
                                "info"
                            ]["release"],
                            "repo": repo_name,
                            "source": source,
                        }
                    )
    if not latest:
        return packages

    latest_packages = {}
    repo_vers = {}
    repo_rels = {}
    for package in packages:
        name = package["name"]
        # Check package version, replace if newer version
        # This logic is duplicated from the SPM install method
        if name not in repo_vers:
            latest_packages[name] = package
            repo_vers[name] = package["version"]
            repo_rels[name] = package["release"]
        elif repo_vers[name] == package["version"]:
            # Version is the same, check release
            if repo_rels[name] > package["release"]:
                latest_packages[name] = package
                repo_vers[name] = package["version"]
                repo_rels[name] = package["release"]
            elif repo_rels[name] == package["release"]:
                # Version and release are the same, give
                # preference to local (file://) repos
                if package["source"].startswith("file://"):
                    latest_packages[name] = package
                    repo_vers[name] = package["version"]
                    repo_rels[name] = package["release"]
        elif repo_vers[name] > package["version"]:
            latest_packages[name] = package
            repo_vers[name] = package["version"]
            repo_rels[name] = package["release"]
    return latest_packages.values()


def _list_installed_packages(
    pkg=None, exact_pkg=False, refresh=False, spm_client=None, spm_ui=None
):
    if not spm_client:
        spm_client, spm_ui = _get_spm_client_ui()

    if refresh:
        refresh_metadata()

    spm_client.run(["list", "packages"])
    packages = spm_ui.statuses
    spm_ui.clear_output()
    if pkg:
        return list(
            package
            for package in packages
            if (exact_pkg and pkg == package) or (not exact_pkg and pkg in package)
        )
    return packages


def list_repo_packages(pkg=None, repo=None, refresh=False, latest=False, **kwargs):
    """
    Lists packages available in repos, optionally limiting which repo is searched.

    CLI Example:

    .. code-block:: bash

        salt '*' spm.list_repo_packages
        salt '*' spm.list_repo_packages pkg=bar
        salt '*' spm.list_repo_packages pkg=bar latest=true
        salt '*' spm.list_repo_packages repo=foo
        salt '*' spm.list_repo_packages pkg=bar repo=foo latest=true refresh=true
    """
    return _list_repo_packages(
        pkg=pkg, repo=repo, exact_pkg=False, latest=latest, refresh=refresh
    )


def repo_info(pkg, refresh=False, **kwargs):
    """
    Retrieves the SPM package info for the latest available from all configured SPM repos.

    CLI Example:

    .. code-block:: bash

        salt '*' spm.repo_info foo
        salt '*' spm.repo_info foo refresh=true
    """
    packages = _list_repo_packages(
        pkg=pkg, exact_pkg=True, refresh=refresh, latest=True
    )
    if len(packages) == 0:
        return {}
    return packages[0]


def list_installed_packages(pkg=None, refresh=False):
    """
    Lists packages currently installed. Due to limitations with the SPM system, this only lists the
    package names and not any other information.

    CLI Example:

    .. code-block:: bash

        salt '*' spm.list_installed_packages
        salt '*' spm.list_installed_packages pkg=bar refresh=True
    """
    # The SPM subsystem doesn't allow us to return info for all installed SPMs
    # even though the code looks like it is setup to do so.
    # Instead we must currently retrieve each package individually.
    return _list_installed_packages(pkg=pkg, exact_pkg=False, refresh=refresh)


def _installed_info(pkg, spm_client=None, spm_ui=None):
    if not spm_client:
        spm_client, spm_ui = _get_spm_client_ui()
    # Currently we cannot get all package info in an easily parseable way from the SPM client methods.
    # Instead, call directly into package methods to get the package info.
    # Obviously, this is not very future proof and may break, so the SPM methods need to be improved
    # at some point.
    pkg_info = spm_client._pkgdb_fun("info", pkg, spm_client.db_conn)
    if pkg_info is None:
        return {}
    return pkg_info


def installed_info(pkg, **kwargs):
    """
    Retrieves the SPM package info for an installed package.

    CLI Example:

    .. code-block:: bash

        salt '*' spm.installed_info foo
    """
    return _installed_info(pkg)


def install(
    name=None, refresh=False, pkgs=None, reinstall=False, saltenv="base", **kwargs
):
    """
    Install the passed SPM package(s), add refresh=True to update the SPM repo
    metadata before any package is installed.
    name
        The name of the SPM package to be installed. Note that this parameter is
        ignored if "pkgs" is passed.
        .. code-block:: bash

            salt '*' pkg.install <package name>
    refresh
        Whether or not to update the SPM repo metadata before executing.
    Multiple SPM Package Installation Options:
    pkgs
        A list of SPM packages to install. Must be passed as a python list.

        CLI Examples:

        .. code-block:: bash

            salt '*' pkg.install pkgs='["foo", "bar"]'
    Returns a dict containing the new SPM package names and versions::
        {"<package>": {"old": "<old-version>",
                       "new": "<new-version>"}}
    """
    if salt.utils.data.is_true(refresh):
        refresh_metadata()

    if pkgs:
        if not isinstance(pkgs, list):
            raise SaltInvocationError(
                'Invalidly formatted "pkgs" parameter, it must be a list of package names'
            )
    else:
        if isinstance(pkgs, list) and len(pkgs) == 0:
            raise SaltInvocationError(
                'Invalidly formatted "pkgs" parameter, it must be a list of package names'
            )
        else:
            pkgs = [name]

    spm_client, spm_ui = _get_spm_client_ui()

    def get_version_info(pkg):
        info = _installed_info(pkg, spm_client=spm_client, spm_ui=spm_ui)
        if not info.get("version"):
            return ""
        return "{}-{}".format(info.get("version"), info.get("release"))

    to_install = []
    to_remove = []
    # Populate the above lists to determine which SPMs need to removed before installing.
    old_pkgs = _list_installed_packages(
        refresh=False, spm_client=spm_client, spm_ui=spm_ui
    )
    ret = {}
    for pkg in pkgs:
        if pkg in old_pkgs:
            ret[pkg] = {"old": get_version_info(pkg)}
            to_remove.append(pkg)
        else:
            ret[pkg] = {"old": ""}
        to_install.append(pkg)

    # Gather all files installed by the current SPMs so that we can manually clean them up later (if needed)
    files_to_remove = []
    for package in to_remove:
        spm_client.run(["list", "files", package])
        files_to_remove.extend(spm_ui.statuses)
        spm_ui.clear_output()

    # Remove SPMs that are installed and need upgraded
    args = ["remove"]
    args.extend(to_remove)
    spm_client.run(args)
    spm_ui.clear_output()

    # Due to bugs in the SPM subsystem, we must manually remove files that are not uninstalled correctly,
    # but first gather all the files before the SPM is removed
    for file_name in files_to_remove:
        # Ignore already removed files and all directories
        if not os.path.exists(file_name) or os.path.isdir(file_name):
            continue
        try:
            os.remove(file_name)
        except Exception:  # pylint: disable=broad-except
            log.error("Could not remove SPM file %s", file_name)

    # Install all SPMs
    args = ["install"]
    args.extend(to_install)
    spm_client.run(args)

    new_pkgs = _list_installed_packages(
        refresh=False, spm_client=spm_client, spm_ui=spm_ui
    )
    for pkg in pkgs:
        if pkg in new_pkgs:
            ret[pkg]["new"] = get_version_info(pkg)
        else:
            ret[pkg]["new"] = ""

    return ret
