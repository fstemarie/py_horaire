import json
import os
import pickle

import fsspec
from prefect.blocks.system import Secret


def get_smb_host() -> str:
    path = "raktar.home"
    return path


def get_smb_user() -> str:
    user = "py_horaire"
    return user


def get_smb_passwd() -> str:
    passwd = "Ayub.kekz.55"
    return passwd


def get_fs(host, user, passwd) -> fsspec.filesystem:
    fs = fsspec.filesystem("smb", host=host,
                           username=user, password=passwd)
    return fs


def flow():
    host = get_smb_host()
    user = get_smb_user()
    passwd = get_smb_passwd()
    fs = get_fs(host, user, passwd)
    files = fs.ls("/jade/files/horaire")
    print(files)


if __name__ == "__main__":
    flow()
