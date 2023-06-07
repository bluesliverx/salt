"""
Handles Salt Package Manager (SPM) repository management.
"""
import logging

import salt.utils
from salt.exceptions import CommandExecutionError, SaltInvocationError
from salt.state import STATE_INTERNAL_KEYWORDS as _STATE_INTERNAL_KEYWORDS

log = logging.getLogger(__name__)


def managed(name, **kwargs):
    """
    This state manages SPM (Salt Packager Manager) repositories. This is only
    supported for minions running on Salt masters.

    name
        This value will be used in two ways: Firstly, it will be the repo ID,
        as seen in the top-level key in the repo file for a given
        repo. Secondly, it will be the name of the file as stored in
        /etc/salt/spm.repos.d (e.g. ``/etc/spm.repos.d/foo.conf``).
    url
        The URL to the SPM repository
    enabled : True
        Whether or not the repo is enabled. Can be specified as True/False or
        1/0.
    refresh : True (only once per salt run using ``spm`` states)
       If set to ``False`` this will skip refreshing the spm repo metadata.
    """
    ret = {"name": name, "changes": {}, "result": None, "comment": ""}

    if "spm.get_repo" not in __salt__:
        ret["result"] = False
        ret["comment"] = "SPM repo management not implemented on this platform"
        return ret

    for kwarg in _STATE_INTERNAL_KEYWORDS:
        kwargs.pop(kwarg, None)

    repo = name
    try:
        pre = __salt__["spm.get_repo"](repo)
    except CommandExecutionError as exc:
        ret["result"] = False
        ret["comment"] = f"Failed to examine SPM repo '{name}': {exc}"
        return ret

    if pre:
        for kwarg in kwargs:
            if kwarg not in pre:
                if kwarg == "enabled":
                    # "enabled" is assumed to be true if not explicitly set, so we don't need to update the repo
                    # if it's desired to be enabled and the "enabled" key is missing from the repo definition
                    if not salt.utils.data.is_true(kwargs[kwarg]):
                        break
                else:
                    break
            else:
                if any(isinstance(x, bool) for x in (kwargs[kwarg], pre[kwarg])):
                    # This check disambiguates 1/0 from True/False
                    if salt.utils.data.is_true(
                        kwargs[kwarg]
                    ) != salt.utils.data.is_true(pre[kwarg]):
                        break
                else:
                    if str(kwargs[kwarg]) != str(pre[kwarg]):
                        break
        else:
            # If we break anywhere in the for up above
            ret["result"] = True
            ret["comment"] = f"SPM repo '{name}' already configured"
            return ret

    if __opts__["test"]:
        ret["comment"] = (
            "SPM repo '{}' will be configured. This may cause spm "
            "states to behave differently than stated if this action is "
            "repeated without test=True, due to the differences in the "
            "configured repositories.".format(name)
        )
        return ret

    try:
        __salt__["spm.mod_repo"](repo, **kwargs)
    except Exception as exc:  # pylint: disable=broad-except
        # This is another way to pass information back from the mod_repo
        # function.
        ret["result"] = False
        ret["comment"] = f"Failed to configure SPM repo '{name}': {exc}"
        return ret

    try:
        post = __salt__["spm.get_repo"](repo)
        if pre:
            for kwarg in kwargs:
                if post.get(kwarg) != pre.get(kwarg):
                    change = {"new": post.get(kwarg, ""), "old": pre.get(kwarg, "")}
                    ret["changes"][kwarg] = change
        else:
            ret["changes"] = {"repo": repo}

        ret["result"] = True
        ret["comment"] = f"Configured SPM repo '{name}'"
    except Exception as exc:  # pylint: disable=broad-except
        ret["result"] = False
        ret["comment"] = "Failed to confirm config of SPM repo '{}': {}".format(
            name, exc
        )

    # Update SPM metadata, if present, since changes to the
    # repositories may change the packages that are available.
    if ret["changes"]:
        __salt__["spm.refresh_metadata"]()
    return ret


def absent(name, **kwargs):
    """
    This function deletes the specified SPM repo on the system, if it exists. It
    is essentially a wrapper around spm.del_repo.
    name
        The name of the SPM repo, which is the file name and repo name in SPM commands.
    """
    ret = {"name": name, "changes": {}, "result": None, "comment": ""}

    try:
        repo = __salt__["spm.get_repo"](name)
    except CommandExecutionError as exc:
        ret["result"] = False
        ret["comment"] = f"Failed to configure SPM repo '{name}': {exc}"
        return ret

    if not repo:
        ret["comment"] = f"SPM repo {name} is absent"
        ret["result"] = True
        return ret

    if __opts__["test"]:
        ret["comment"] = (
            "SPM repo '{}' will be removed. This may "
            "cause spm states to behave differently than stated "
            "if this action is repeated without test=True, due "
            "to the differences in the configured repositories.".format(name)
        )
        return ret

    try:
        __salt__["spm.del_repo"](name, **kwargs)
    except (CommandExecutionError, SaltInvocationError) as exc:
        ret["result"] = False
        ret["comment"] = exc.strerror
        return ret

    repos = __salt__["spm.list_repos"]()
    if name not in repos:
        ret["changes"]["repo"] = name
        ret["comment"] = f"Removed SPM repo {name}"
        ret["result"] = True
    else:
        ret["result"] = False
        ret["comment"] = f"Failed to remove SPM repo {name}"

    return ret
