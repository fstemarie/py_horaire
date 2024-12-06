import asyncio
import pickle

import fsspec
from imbox import Imbox
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

SCOPES = ['https://www.googleapis.com/auth/drive']


@task(name="Get IMAP host")
async def get_imap_host() -> str:
    url = await Variable.get("horaire_imap_host")
    return url


@task(name="Get IMAP username")
async def get_imap_user() -> str:
    username = await Variable.get("horaire_imap_user")
    return username


@task(name="Get IMAP password")
async def get_imap_pass() -> str:
    password = await Secret.load("horaire-imap-pass")
    return password


@task(name="Get IMAP messages")
def get_imap_messages(imbox):
    messages = imbox.messages(
        folder="all", raw="label:Horaire -label:Horaire/pickedup")
    return messages


@task(name="Get attachments")
def get_attachments(message):
    attachments = []
    for attachment in message.attachments:
        filename = attachment.get("filename")
        content = attachment.get("content")
        if filename[-5:].lower() != ".xlsx":
            continue
        attachments.append((filename, content))
    return attachments


@task(name="Save attachment")
async def save_attachment(fs, filename, content):
    with fs.open(f"/d/jade/files/horaire/1- New/{filename}", "wb") as f:
        f.write(content.getbuffer())


@task(name="Get SMB host")
async def get_smb_host() -> str:
    host = await Variable.get("horaire_smb_host")
    return host


@task(name="Get SMB username")
async def get_smb_user() -> str:
    username = await Variable.get("horaire_smb_user")
    return username


@task(name="Get SMB password")
async def get_smb_pass() -> str:
    password = await Secret.load("horaire-smb-pass")
    return password


@task(name="Get filesystem")
def get_filesystem(host, user, passwd):
    fs = fsspec.filesystem(
        "smb", host=host, username=user, password=passwd.get())
    return fs


async def ramasseux_flow():
    imap_host, imap_user, imap_passwd = await asyncio.gather(
        get_imap_host(),
        get_imap_user(),
        get_imap_pass(),
    )
    # smb_host, smb_user, smb_passwd = await asyncio.gather(
    #     get_smb_host(),
    #     get_smb_user(),
    #     get_smb_pass()
    # )
    with Imbox(hostname=imap_host,
               username=imap_user,
               password=imap_passwd.get()) as imbox:
        messages = get_imap_messages(imbox)
        attachments = []
        for uid, message in messages:
            attachments.extend(get_attachments(message))
            imbox.move(uid, "Horaire/pickedup")

        # fs = get_filesystem(smb_host, smb_user, smb_passwd)
        fs = fsspec.filesystem("file")
        for filename, content in attachments:
            await save_attachment(fs, filename, content)


if __name__ == "__main__":
    asyncio.run(ramasseux_flow())
