import asyncio
import os

import fsspec
from imbox import Imbox
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

ROOT_PATH = "./workspace"
DEST_PATH = os.path.join(ROOT_PATH, "1- New")
IMAP_QUERY = "label:horaire -label:horaire/pickedup"


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
        folder="all", raw=IMAP_QUERY)
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
    with fs.open(os.path.join(ROOT_PATH, filename), "wb") as f:
        f.write(content.getbuffer())


@flow(name="Ramasseux")
async def ramasseux():
    imap_host, imap_user, imap_passwd = await asyncio.gather(
        get_imap_host(),
        get_imap_user(),
        get_imap_pass(),
    )
    with Imbox(hostname=imap_host,
               username=imap_user,
               password=imap_passwd.get()) as imbox:
        messages = get_imap_messages(imbox)
        attachments = []
        for uid, message in messages:
            attachments.extend(get_attachments(message))
            imbox.move(uid, "Horaire/pickedup")

        fs = fsspec.filesystem("file")
        for filename, content in attachments:
            await save_attachment(fs, filename, content)


if __name__ == "__main__":
    asyncio.run(ramasseux())
