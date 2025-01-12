import asyncio
import os

import fsspec
from imbox import Imbox

ROOT_PATH = os.environ.get("HORAIRE_WORKSPACE")
DEST_PATH = os.path.join(ROOT_PATH, "1- New")
IMAP_QUERY = "label:horaire -label:horaire/pickedup"


async def get_imap_host() -> str:
    url = "imap.gmail.com"
    return url


async def get_imap_user() -> str:
    username = "fstemarie.bb"
    return username


async def get_imap_pass() -> str:
    password = os.environ.get("HORAIRE_IMAP_PASS")
    return password


def get_imap_messages(imbox):
    messages = imbox.messages(
        folder="all", raw=IMAP_QUERY)
    return messages


def get_attachments(message):
    attachments = []
    for attachment in message.attachments:
        filename = attachment.get("filename")
        content = attachment.get("content")
        if filename[-5:].lower() != ".xlsx":
            continue
        attachments.append((filename, content))
    return attachments


async def save_attachment(fs, filename, content):
    with fs.open(os.path.join(DEST_PATH, filename), "wb") as f:
        f.write(content.getbuffer())


async def ramasseux():
    imap_host, imap_user, imap_passwd = await asyncio.gather(
        get_imap_host(),
        get_imap_user(),
        get_imap_pass(),
    )
    with Imbox(hostname=imap_host,
               username=imap_user,
               password=imap_passwd) as imbox:
        messages = get_imap_messages(imbox)
        attachments = []
        for uid, message in messages:
            attachments.extend(get_attachments(message))
            imbox.move(uid, "horaire/pickedup")

        fs = fsspec.filesystem("file")
        for filename, content in attachments:
            await save_attachment(fs, filename, content)


if __name__ == "__main__":
    asyncio.run(ramasseux())
