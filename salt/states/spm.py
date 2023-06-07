"""
Handles Salt Package Manager (SPM) package management.
"""
import logging
import os

import salt.utils
from salt.exceptions import CommandExecutionError

log = logging.getLogger(__name__)


# The refresh/refresh_tag code is copied from salt core and modified for our purposes


def _refresh_tag():
    """
    Return the refresh_tag file location. This file is used to ensure that we don't
    refresh more than once (unless explicitly configured to do so).
    """
    return os.path.join(__opts__["cachedir"], "spm_refresh")


def _write_refresh_tag():
    """
    Write the refresh_tag file
    """
    refresh_tag_file = _refresh_tag()
    if not os.path.exists(refresh_tag_file):
        try:
            with salt.utils.files.fopen(refresh_tag_file, "w+"):
                pass
        except OSError as exc:
            log.warning("Encountered error writing refresh tag: %s", exc.__str__())


# Refresh none means refresh only once, false means do not refresh, and true means always refresh
def _check_refresh(refresh=None):
    """
    Check whether or not a refresh is necessary.
    Returns:
    - True if refresh evaluates as True
    - False if refresh is False
    - A boolean if refresh is not False and the refresh_tag file exists
    """
    return bool(
        salt.utils.data.is_true(refresh)
        or (os.path.isfile(_refresh_tag()) and refresh is not False)
    )


def mod_init(low):
    """
    Set a flag to tell the install functions to refresh the package database.
    This ensures that the package database is refreshed only once during
    a state run significantly improving the speed of SPM package management
    during a state run.
    .. seealso::
       :py:func:`salt.states.pkg.mod_init`
    """
    ret = True
    if low["fun"] == "installed" or low["fun"] == "latest":
        _write_refresh_tag()
        return ret
    return False


def latest(name, refresh=None, exact_versions=None, pkgs=None, fromrepo=None, **kwargs):
    """
    Ensure that the named SPM package is installed and the latest available
    SPM. If the SPM can be updated, this state function will update
    the package. Currently this is done by removing the current page and
    installing the new package due to the limitations of the SPM subsystem.
    Note that versions and repositories may not be specified for SPM installs
    since the SPM subsystem does not allow for fine-grained control of package
    installs.
    name
        The name of the SPM package to maintain at the latest available version.
        This parameter is ignored if "pkgs" is used.
    refresh
        This parameter controls whether or not the SPM repo metadata is
        updated prior to checking for the latest available version of the
        requested packages.
        If ``True``, the repo metadata will be refreshed (``spm update_repo``)
        before checking for the latest available version of the requested SPMs.
        If ``False``, the repo metadata will *not* be refreshed before checking.
        If unset, then Salt treats repo metadata refreshes differently
        depending on whether or not a ``spm`` state has been executed already
        during the current Salt run. Once a refresh has been performed in a
        ``spm`` state, for the remainder of that Salt run no other refreshes
        will be performed for ``spm`` states which do not explicitly set
        ``refresh`` to ``True``. This prevents needless additional refreshes
        from slowing down the Salt run.
    exact_versions
        If truthy, Specifies that the versions in the repo should be installed
        if they are different than the currently installed version.
    Multiple SPM Package Installation Options:
    pkgs
        A list of SPM packages to maintain at the latest available version.
    .. code-block:: yaml

        mypkgs:
          spm.latest:
            - pkgs:
              - foo
              - bar
              - baz
    fromrepo
        The repo to install all available SPMs from. Depending on repo configuration, versions
        may be pulled from other repositories due to limitations in the SPM subsystem.
    .. code-block:: yaml

        mypkgs:
          spm.latest:
            - fromrepo: foo
    """
    refresh = _check_refresh(refresh)

    # Immediately refresh metadata
    if refresh:
        __salt__["spm.refresh_metadata"]()

    no_spm_packages_ret = {
        "name": name,
        "changes": {},
        "result": True,
        "comment": "No SPM packages to install provided",
    }
    if pkgs:
        # Translate a possible dictionary into a list of packages
        desired_pkgs = list(
            {
                str(x): str(y) if y is not None else y
                for x, y in salt.utils.repack_dictlist(pkgs).items()
            }.keys()
        )
        if not desired_pkgs:
            # Badly-formatted SLS
            return {
                "name": name,
                "changes": {},
                "result": False,
                "comment": 'Invalidly formatted "pkgs" parameter. See ' "minion log.",
            }
    elif isinstance(pkgs, list) and len(pkgs) == 0:
        return no_spm_packages_ret
    elif fromrepo:
        # No need to refresh, if a refresh was necessary it would have been
        # performed at the beginning of this method
        desired_pkgs = list(
            package["name"]
            for package in __salt__["spm.list_repo_packages"](
                repo=fromrepo, refresh=False
            )
        )
        if len(desired_pkgs) == 0:
            return no_spm_packages_ret
    else:
        desired_pkgs = [name]

    kwargs["saltenv"] = __env__

    avail = {}
    try:
        # No need to refresh here
        packages = __salt__["spm.list_repo_packages"](
            latest=True, refresh=False, **kwargs
        )
        for package in packages:
            if package["name"] in desired_pkgs:
                avail[package["name"]] = package
    except CommandExecutionError as exc:
        return {
            "name": name,
            "changes": {},
            "result": False,
            "comment": "An error was encountered while checking the "
            "newest available version of SPM package(s): {}".format(exc),
        }

    cur = {}
    for pkg in desired_pkgs:
        try:
            cur[pkg] = __salt__["spm.installed_info"](pkg, **kwargs)
        except CommandExecutionError as exc:
            return {
                "name": name,
                "changes": {},
                "result": False,
                "comment": exc.strerror,
            }

    def get_version_info(package_info):
        return "{}-{}".format(package_info["version"], package_info["release"])

    targets = {}
    problems = []
    for pkg in desired_pkgs:
        if not avail.get(pkg):
            if not cur.get(pkg):
                # Package does not exist
                msg = f"No SPM information found for '{pkg}'."
                log.error(msg)
                problems.append(msg)
            # Else the package is up-to-date
        elif not cur.get(pkg):
            # Package is not installed
            targets[pkg] = get_version_info(avail[pkg])
        else:
            # Compare versions
            avail_version_info = get_version_info(avail[pkg])
            cur_version_info = get_version_info(cur[pkg])
            comparison = salt.utils.versions.version_cmp(
                avail_version_info, cur_version_info, ignore_epoch=True
            )
            if comparison == 1 or (exact_versions and comparison):
                targets[pkg] = avail_version_info

    if problems:
        return {
            "name": name,
            "changes": {},
            "result": False,
            "comment": " ".join(problems),
        }

    if targets:
        # Find up-to-date packages
        if not pkgs:
            # There couldn't have been any up-to-date packages if this state
            # only targeted a single package and is being allowed to proceed to
            # the install step.
            up_to_date = []
        else:
            up_to_date = [x for x in pkgs if x not in targets]

        if __opts__["test"]:
            comments = []
            comments.append(
                "The following SPM packages would be installed/upgraded: "
                + ", ".join(sorted(targets))
            )
            if up_to_date:
                up_to_date_count = len(up_to_date)
                if up_to_date_count <= 10:
                    comments.append(
                        "The following SPM packages are already up-to-date: "
                        + ", ".join([f"{x} ({cur[x]})" for x in sorted(up_to_date)])
                    )
                else:
                    comments.append(
                        "{} SPM packages are already up-to-date".format(
                            up_to_date_count
                        )
                    )

            return {
                "name": name,
                "changes": {},
                "result": None,
                "comment": "\n".join(comments),
            }

        # Build updated list of pkgs to exclude non-targeted ones
        targeted_pkgs = list(targets)

        try:
            # No need to refresh here
            changes = __salt__["spm.install"](
                name=None, refresh=False, pkgs=targeted_pkgs, **kwargs
            )
        except CommandExecutionError as exc:
            return {
                "name": name,
                "changes": {},
                "result": False,
                "comment": "An error was encountered while installing "
                "SPM package(s): {}".format(exc),
            }

        if changes:
            # Find failed and successful updates
            failed = [
                x
                for x in targets
                if not changes.get(x)
                or changes[x].get("new") != targets[x]
                and targets[x] != "latest"
            ]
            successful = [x for x in targets if x not in failed]

            comments = []
            if failed:
                msg = "The following SPM package(s) failed to update: " "{}".format(
                    ", ".join(sorted(failed))
                )
                comments.append(msg)
            if successful:
                msg = (
                    "The following SPM packages were successfully "
                    "installed/upgraded: "
                    "{}".format(", ".join(sorted(successful)))
                )
                comments.append(msg)
            if up_to_date:
                if len(up_to_date) <= 10:
                    msg = (
                        "The following SPM packages were already up-to-date: "
                        "{}".format(", ".join(sorted(up_to_date)))
                    )
                else:
                    msg = "{} SPM packages were already up-to-date ".format(
                        len(up_to_date)
                    )
                comments.append(msg)

            return {
                "name": name,
                "changes": changes,
                "result": False if failed else True,
                "comment": " ".join(comments),
            }
        else:
            if len(targets) > 10:
                comment = (
                    "{} targeted SPM packages failed to update. "
                    "See debug log for details.".format(len(targets))
                )
            elif len(targets) > 1:
                comment = (
                    "The following targeted SPM packages failed to update. "
                    "See debug log for details: ({}).".format(
                        ", ".join(sorted(targets))
                    )
                )
            else:
                comment = "SPM package {} failed to " "update.".format(
                    next(iter(list(targets.keys())))
                )
            if up_to_date:
                if len(up_to_date) <= 10:
                    comment += (
                        " The following SPM packages were already "
                        "up-to-date: "
                        "{}".format(", ".join(sorted(up_to_date)))
                    )
                else:
                    comment += "{} SPM packages were already " "up-to-date".format(
                        len(up_to_date)
                    )

            return {
                "name": name,
                "changes": changes,
                "result": False,
                "comment": comment,
            }
    else:
        if len(desired_pkgs) > 10:
            comment = f"All {len(desired_pkgs)} SPM packages are up-to-date."
        elif len(desired_pkgs) > 1:
            comment = "All SPM packages are up-to-date " "({}).".format(
                ", ".join(sorted(desired_pkgs))
            )
        else:
            comment = "SPM package {} is already " "up-to-date".format(desired_pkgs[0])

        return {"name": name, "changes": {}, "result": True, "comment": comment}
