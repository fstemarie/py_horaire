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
import os
import re
from datetime import date, datetime, time, timedelta
from pprint import pprint
from uuid import uuid4
from zoneinfo import ZoneInfo

import caldav
import fsspec
import icalendar
import pandas as pd
from ramasseux import *
from slugify import slugify

ROOT_PATH = os.environ.get("HORAIRE_WORKSPACE")
SRC_PATH = os.path.join(ROOT_PATH, "2- Prepared")
DEST_PATH = os.path.join(ROOT_PATH, "3- Processed")
ICS_PATH = os.path.join(ROOT_PATH, "ics")
WEEK = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
PST = ZoneInfo("America/Vancouver")
EST = ZoneInfo("America/Montreal")
UTC = ZoneInfo("UTC")


async def get_caldav_url() -> str:
    print("Getting CalDAV URL")
    url = "https://radicale.sole-altair.ts.net"
    return url


async def get_caldav_user() -> str:
    print("Getting CalDAV User")
    username = "py_horaire"
    return username


async def get_caldav_passwd() -> str:
    print("Getting CalDAV Password")
    passwd = os.environ.get("HORAIRE_CALDAV_PASS")
    return passwd


def extract_schedule(xl_fobj) -> dict:
    schedule = {}
    df = pd.read_excel(io=xl_fobj,
                       engine="openpyxl",
                       header=None,
                       names=["employee", *WEEK]).fillna('')

    schedule["employee"] = tuple(df["employee"])
    for day_of_week in WEEK:
        schedule[day_of_week] = tuple(df[day_of_week])
    return schedule


async def process_excel_files() -> list[dict]:
    schedules = []
    fs = fsspec.filesystem("file")
    xl_files = fs.ls(SRC_PATH)
    for xl_file in xl_files:
        with fs.open(xl_file, "rb") as xl_fobj:
            schedule = extract_schedule(xl_fobj)
            schedule["filename"] = os.path.basename(xl_file)
            schedules.append(schedule)
            fs.mv(xl_file, DEST_PATH)
    return schedules


async def build_event(event):
    ne = icalendar.Event()
    ne.add("UID", uuid4().hex + "_falarie")
    ne.add("SUMMARY", event["summary"])
    ne.add("DESCRIPTION", "")
    ne.add("DTSTAMP", datetime.now().astimezone(UTC))
    if event["all_day"]:
        ne.add("DTSTART", event["start"], parameters={"VALUE": "DATE"})
        ne.add("DTEND", event["finish"], parameters={"VALUE": "DATE"})
        ne.add("TRANSP", "TRANSPARENT")
    else:
        ne.add("DTSTART", event["start"])
        ne.add("DTEND", event["finish"])
    return ne


async def build_ical(events):
    print(f"Building iCal")
    nc = icalendar.Calendar()
    nc.add("PRODID", "-//falarie/py_horaire")
    nc.add("VERSION", "2.0")
    for event in events:
        ne = await build_event(event)
        nc.add_component(ne)
    return nc.to_ical().decode()


async def cleanup_hours(hours: str) -> str:
    if hours.lower() == "off":
        return "off"
    elif hours.lower() == "vac":
        return "vac"

    new_hours = hours
    new_hours = new_hours.replace("`r`n", " ").replace(" PST", "")
    new_hours = re.sub(r"NO LUNCH", "", new_hours, flags=re.IGNORECASE)
    new_hours = new_hours.replace(" - ", "|")
    new_hours = re.sub(r"LUNCH\s*:", "|", new_hours)
    new_hours = re.sub(r"\s*", "", new_hours)
    # new_hours = re.sub(r"|$", "", new_hours)
    return new_hours


def to_time(time_str: str) -> datetime:
    if len(time_str) < 6:
        tm = datetime.strptime(time_str, "%I%p").time()
    else:
        tm = datetime.strptime(time_str, "%I:%M%p").time()
    return tm.replace(tzinfo=PST)


async def process_schedule(schedule):
    events = []
    for day_of_week in WEEK:
        for employee, hours in zip(schedule["employee"], schedule[day_of_week]):
            if not hours:  # Empty row
                continue
            if not employee:  # Dates row
                day = hours
                day = day.date()
                continue
            hours = await cleanup_hours(hours)
            if hours == "off":
                events.append(
                    dict(employee=employee,
                         summary=f"Off {employee}",
                         start=day,
                         finish=day + timedelta(days=1),
                         all_day=True))
            elif hours == "vac":
                events.append(
                    dict(employee=employee,
                         summary=f"Vacation {employee}",
                         start=day,
                         finish=day + timedelta(days=1),
                         all_day=True))
            else:
                hs = hours.split("|")
                start = datetime.combine(day, to_time(hs[0]))
                finish = datetime.combine(day, to_time(hs[1]))
                if finish < start:
                    finish += timedelta(days=1)
                events.append(
                    dict(employee=employee,
                         summary=f"<> {employee}",
                         start=start.astimezone(UTC),
                         finish=finish.astimezone(UTC),
                         all_day=False))
                if len(hs) == 3:  # Lunch
                    start = datetime.combine(day, to_time(hs[2]))
                    finish = start + timedelta(minutes=30)
                    events.append(
                        dict(employee=employee,
                             summary=f"-- {employee}",
                             start=start.astimezone(UTC),
                             finish=finish.astimezone(UTC),
                             all_day=False))
    return events


async def prune_calendar(client: caldav.DAVClient):
    print("Pruning calendar")
    # end_date = datetime.now(tz=UTC) - timedelta(days=14)
    for cal_name in ["gs-collegues", "gs-ste-marie-francois"]:
        try:
            cal = client.principal().calendar(name=cal_name)
        except caldav.error.NotFoundError:
            cal = client.principal().make_calendar(name=cal_name)
        # events = cal.search(end=end_date, event=True)
        events = cal.events()
        for event in events:
            event.delete()


async def fill_calendar(client: caldav.DAVClient, events: list[dict]):
    print("Filling calendar")
    cal = client.principal().calendar(name="gs-collegues")
    cal_me = client.principal().calendar(name="gs-ste-marie-francois")
    for event in events:
        ical = await build_ical([event])
        if event["employee"] == 'Ste-Marie, François':
            cal_me.save_event(ical)
        else:
            cal.save_event(ical)


async def horaire():
    caldav_url, caldav_user, caldav_passwd = await asyncio.gather(
        get_caldav_url(),
        get_caldav_user(),
        get_caldav_passwd()
    )
    client = caldav.DAVClient(url=caldav_url,
                              username=caldav_user,
                              password=caldav_passwd)
    await prune_calendar(client)
    schedules = await process_excel_files()
    if not schedules:
        print("No Excel files to process")
        return
    print("Processing Excel files")
    for schedule in schedules:
        print("Processing schedule")
        events = await process_schedule(schedule)
        print("Building iCal")
        ics_str = await build_ical(events)
        ics_file = os.path.join(
            ICS_PATH, schedule["filename"].replace(".xlsx", ".ics"))
        with open(ics_file, "w+",) as f:
            f.write(ics_str)
        print("Filling calendar")
        await fill_calendar(client, events)
    client.close()

if __name__ == "__main__":
    print(f"ROOT_PATH = {ROOT_PATH}")
    asyncio.run(horaire())
