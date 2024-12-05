#! python

# RFC2045 - Multipurpose Internet Mail Extensions (MIME) Part One
#     https://tools.ietf.org/html/rfc2045.html

# RFC2046 - Multipurpose Internet Mail Extensions (MIME) Part Two
#     https://tools.ietf.org/html/rfc2046.html

# RFC2047 - MIME (Multipurpose Internet Mail Extensions) Part Three
#     https://tools.ietf.org/html/rfc2047.html

# RFC2183 - Communicating Presentation Information in Internet Messages
#     https://tools.ietf.org/html/rfc2183.html

# RFC2231 - MIME Parameter Value and Encoded Word Extensions
#     https://tools.ietf.org/html/rfc2231.html

import asyncio
import re
from datetime import date, datetime, time, timedelta
from pprint import pprint
from uuid import uuid4
from zoneinfo import ZoneInfo

import caldav
import fsspec
import icalendar
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.variables import Variable
from slugify import slugify

SRC_PATH = "/jade/files/horaire/2- Prepared"
DEST_PATH = "/jade/files/horaire/3- Processed"
WEEK = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
PST = ZoneInfo("America/Vancouver")
EST = ZoneInfo("America/Montreal")
UTC = ZoneInfo("UTC")


@task(name="Get CalDAV URL")
async def get_caldav_url() -> str:
    url = await Variable.get("horaire_caldav_url")
    return url


@task(name="Get CalDAV Username")
async def get_caldav_user() -> str:
    username = Variable.get("horaire_caldav_user")
    return await username


@task(name="Get CalDAV Password")
async def get_caldav_passwd() -> str:
    passwd = await Secret.load("horaire-caldav-pass")
    return passwd


@task(name="Get CalDAV Client")
async def get_caldav_client(url, username, password) -> caldav.DAVClient:
    client = await caldav.DAVClient(url=url, username=username, password=password)
    return client


@task(name="Delete CalDAV old events")
async def del_caldav_old_events(cal: caldav.Calendar):
    dtend = datetime.now() - timedelta(days=14)
    old_events = cal.search(event=True, end=dtend)
    for x in old_events:
        await x.delete()


@task(name="Create CalDAV Events")
def create_events(client: caldav.DAVClient, schedule: list[dict]):
    try:
        cal_employees = client.principal().calendar(name="gs-employees")
    except caldav.error.NotFoundError:
        cal_employees = client.principal().make_calendar(name="gs-employees")
    for x in schedule:
        try:
            cal = client.principal().calendar(name=x["cal"])
        except caldav.error.NotFoundError:
            cal = client.principal().make_calendar(name=x["cal"])
        cal_employees.save_event(x["iCal"])
        cal.save_event(x["iCal"])


@task(name="Get SMB host")
async def get_smb_host() -> str:
    host = await Variable.get("horaire_smb_host")
    return host


@task(name="Get SMB username")
async def get_smb_user() -> str:
    username = await Variable.get("horaire_smb_user")
    return username


@task(name="Get SMB password")
async def get_smb_passwd() -> str:
    passwd = await Secret.load("horaire-smb-pass")
    return passwd


@task(name="Get filesystem")
def get_filesystem(host, user, passwd) -> fsspec.filesystem:
    fs = fsspec.filesystem(
        "smb", host=host, username=user, password=passwd.get())
    return fs


def extract_schedule(fs, xl_file) -> pd.DataFrame:
    with fs.open(xl_file["name"], "rb") as xl_fobj:
        df = pd.read_excel(io=xl_fobj, engine="openpyxl", header=None, names=[
                           "employee", *WEEK]).fillna('')
    return df


def cleanup_hours(hours: str) -> str:
    if hours.lower() == "off":
        return "off"
    elif hours.lower() == "vac":
        return "vac"

    new_hours = hours.replace("`r`n", " ").replace(" PST", "")
    new_hours = re.sub(r"NO LUNCH", "", new_hours, flags=re.IGNORECASE)
    new_hours = new_hours.replace(" - ", "|")
    new_hours = re.sub(r"LUNCH\s*:", "|", new_hours)
    new_hours = re.sub(r"\s*", "", new_hours)
    # new_hours = re.sub(r"|$", "", new_hours)
    return new_hours


def transform_to_date(date_: date, time_: str) -> datetime:
    if len(time_) < 6:
        tm = datetime.strptime(time_, "%I%p").time()
    else:
        tm = datetime.strptime(time_, "%I:%M%p").time()
    return datetime.combine(date_, tm).replace(tzinfo=PST)


def process_schedule(schedule_df: pd.DataFrame):
    schedule = []
    for day_of_week in WEEK:
        for employee, hours in zip(schedule_df["employee"], schedule_df[day_of_week]):
            if not hours:  # Empty row
                continue
            if not employee:  # Dates row
                day = hours
                continue
            hours = cleanup_hours(hours)
            if hours == "off":
                schedule.append(
                    dict(employee=employee,
                         cal="gs-" + slugify(employee),
                         summary=f"Off {employee}",
                         start=day,
                         finish=day + timedelta(days=1),
                         all_day=True))
            elif hours == "vac":
                schedule.append(
                    dict(employee=employee,
                         cal="gs-" + slugify(employee),
                         summary=f"Vacation {employee}",
                         all_day=True))
            else:
                hs = hours.split("|")
                start = transform_to_date(day, hs[0])
                finish = transform_to_date(day, hs[1])
                schedule.append(
                    dict(employee=employee,
                         cal="gs-" + slugify(employee),
                         summary=f"<> {employee}",
                         start=start.astimezone(UTC),
                         finish=finish.astimezone(UTC),
                         all_day=False))
                if len(hs) == 3:  # Lunch
                    start = transform_to_date(day, hs[2])
                    finish = start + timedelta(minutes=30)
                    schedule.append(
                        dict(employee=employee,
                             cal="gs-" + slugify(employee),
                             summary=f"-- {employee}",
                             start=start.astimezone(UTC),
                             finish=finish.astimezone(UTC),
                             all_day=False))
    return schedule


@task(name="Process Excel files")
async def process_xl_files(fs: fsspec.filesystem) -> list[dict]:
    xl_files = fs.ls(SRC_PATH)
    schedule = []
    for xl_file in xl_files:
        schedule_df = extract_schedule(fs, xl_file)
        schedule.extend(process_schedule(schedule_df))
    return schedule


@task(name="Extrapolate iCal")
async def extrapolate_iCal(schedule: list[dict]) -> list[dict]:
    for x in schedule:
        nev = icalendar.Event()
        nev.add("UID", uuid4().hex + "_falarie")
        nev.add("SUMMARY", x["summary"])
        nev.add("DESCRIPTION", "")
        nev.add("DTSTAMP", datetime.now().astimezone(UTC))
        if not x["all_day"]:
            nev.add("DTSTART", x["start"])
            nev.add("DTEND", x["finish"])
        else:
            nev.add("DTSTART", x["start"])
            nev.add("DTEND", x["finish"])
        nc = icalendar.Calendar()
        nc.add("PRODID", "-//falarie/py_horaire")
        nc.add("VERSION", "2.0")
        nc.add_component(nev)
        x["iCal"] = nc.to_ical().decode()
    return schedule


@flow(name="Horaire flow")
async def horaire_flow():
    caldav_url, caldav_user, caldav_passwd = await asyncio.gather(
        get_caldav_url(),
        get_caldav_user(),
        get_caldav_passwd()
    )
    smb_host, smb_user, smb_passwd = await asyncio.gather(
        get_smb_host(),
        get_smb_user(),
        get_smb_passwd()
    )

    fs = get_filesystem(smb_host, smb_user, smb_passwd)
    schedule = await process_xl_files(fs)
    schedule = await extrapolate_iCal(schedule)
    with caldav.DAVClient(url=caldav_url,
                          username=caldav_user,
                          password=caldav_passwd.get()) as client:
        create_events(client, schedule)
    pprint(schedule)
    # cal = client.principal().calendars()[0]


if __name__ == "__main__":
    asyncio.run(horaire_flow())
