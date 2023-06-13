import hashlib
import pathlib

import pytest

import salt.spm.pkgfiles.local as spm
import salt.syspaths
from tests.support.mock import MagicMock, patch


@pytest.fixture()
def configure_loader_modules():
    return {spm: {"__opts__": {"spm_node_type": "master"}}}


class MockTar:
    def __init__(self):
        self.name = str(pathlib.Path("apache", "_README"))
        self.path = str(pathlib.Path(salt.syspaths.CACHE_DIR, "master", "extmods"))


def test_install_file(tmp_path):
    """
    test spm.pkgfiles.local
    """
    assert (
        spm.install_file(
            "apache",
            formula_tar=MagicMock(),
            member=MockTar(),
            formula_def={"name": "apache"},
            conn={"formula_path": str(tmp_path / "test")},
        )
        == MockTar().path
    )


def test_remove_file_exists(tmp_path):
    conn = MagicMock()
    tmp_file = tmp_path / "file1"
    tmp_file.write_text("text")
    spm.remove_file(str(tmp_file), conn=conn)
    assert not tmp_file.exists()


def test_remove_file_does_not_exist(tmp_path):
    conn = MagicMock()
    tmp_file = tmp_path / "file1"
    spm.remove_file(str(tmp_file), conn=conn)


@patch("salt.spm.pkgfiles.local.os")
def test_remove_file_raises_exc(os_mock, tmp_path):
    os_mock.remove.side_effect = OSError("exc")
    conn = MagicMock()
    tmp_file = tmp_path / "file1"
    with pytest.raises(OSError, match="exc"):
        spm.remove_file(str(tmp_file), conn=conn)


def test_hash_file_exists(tmp_path):
    tmp_file = tmp_path / "file1"
    tmp_file.write_text("text")
    hashobj = hashlib.sha1()
    assert spm.hash_file(str(tmp_file), hashobj) != ""


def test_hash_file_does_not_exist(tmp_path):
    tmp_file = tmp_path / "file1"
    hashobj = hashlib.sha1()
    assert spm.hash_file(str(tmp_file), hashobj) == ""


def test_hash_file_raises_exc(tmp_path):
    tmp_file = tmp_path / "file1"
    tmp_file.touch()
    hashobj = MagicMock()
    hashobj.update.side_effect = IOError(0, "exc")
    with pytest.raises(IOError, match="exc"):
        spm.hash_file(str(tmp_file), hashobj)
