#!/usr/bin/env python3
"""Generate an RSS 2.0 feed from the MotherDuck status page API."""

import datetime
import html
import re
from email.utils import format_datetime
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring

import requests

BASE_URL = "https://status.motherduck.com"
MONTHS_BACK = 3
OUTPUT_FILE = "feed.xml"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch_json(path, params=None):
    resp = requests.get(f"{BASE_URL}/{path}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def month_boundaries_ms(year, month):
    """Return (since, until) in milliseconds (JS getTime() style) for the given month."""
    since = datetime.datetime(year, month, 1, tzinfo=datetime.timezone.utc)
    if month == 12:
        until = datetime.datetime(year + 1, 1, 1, tzinfo=datetime.timezone.utc)
    else:
        until = datetime.datetime(year, month + 1, 1, tzinfo=datetime.timezone.utc)
    return int(since.timestamp() * 1000), int(until.timestamp() * 1000)


def ms_to_rfc2822(ms):
    """Convert a millisecond epoch timestamp to RFC 2822 format for RSS pubDate."""
    if not ms:
        return format_datetime(datetime.datetime.now(datetime.timezone.utc))
    dt = datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)
    return format_datetime(dt)


def ms_to_human(ms):
    """Convert millisecond timestamp to a human-readable UTC string."""
    if not ms:
        return ""
    dt = datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)
    return dt.strftime("%b %d, %Y %H:%M UTC")


# ---------------------------------------------------------------------------
# API fetchers
# ---------------------------------------------------------------------------

def fetch_posts(months=MONTHS_BACK):
    """Fetch all posts for the last N months, handling pagination."""
    posts = []
    now = datetime.datetime.now(datetime.timezone.utc)

    for i in range(months):
        # Python floor division handles year rollover correctly
        total_months = now.month - 1 - i
        year = now.year + total_months // 12
        month = total_months % 12 + 1

        since_ms, until_ms = month_boundaries_ms(year, month)
        data = fetch_json("api/posts", {"since": since_ms, "until": until_ms})
        posts.extend(data.get("posts", []))

        token = data.get("continuationToken")
        while token:
            data = fetch_json(
                "api/posts",
                {"since": since_ms, "until": until_ms, "continuation_token": token},
            )
            posts.extend(data.get("posts", []))
            token = data.get("continuationToken")

    return posts


def build_lookup(items, id_key="id", name_key="name"):
    return {item[id_key]: item[name_key] for item in items if id_key in item}


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def strip_html(text):
    """Remove HTML tags, unescape entities, and normalise whitespace."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"<[^>]*$", "", text)  # strip partial/unclosed tag at string end
    text = html.unescape(text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


# ---------------------------------------------------------------------------
# Feed assembly
# ---------------------------------------------------------------------------

def build_item_title(post, severity_map):
    title = post.get("title", "Status Update")
    latest = post.get("latest_update") or {}
    severity_id = latest.get("severity_id", "")
    severity_name = severity_map.get(severity_id, "")
    if severity_name and severity_name.lower() not in ("all good", ""):
        return f"[{severity_name.title()}] {title}"
    return title


def build_item_description(post, status_map, severity_map, service_map):
    """Build a plain-text description for an RSS item."""
    lines = []

    # Status
    latest = post.get("latest_update") or {}
    status_name = status_map.get(latest.get("status_id", ""), "")
    if status_name:
        lines.append(f"Status: {status_name.title()}")

    # Affected services with per-service impact severity
    impacts = latest.get("impacts") or []
    if impacts:
        affected = []
        for imp in impacts:
            svc = service_map.get(imp.get("service_id", ""), "Unknown service")
            sev = severity_map.get(imp.get("severity_id", ""), "")
            affected.append(f"{svc} ({sev})" if sev else svc)
        lines.append(f"Affected: {', '.join(affected)}")

    lines.append("")  # blank line before updates

    # All updates in chronological order
    updates = sorted(post.get("updates") or [], key=lambda u: u.get("reported_at", 0))
    for upd in updates:
        ts = ms_to_human(upd.get("reported_at"))
        msg = strip_html(upd.get("message", ""))
        if ts:
            lines.append(f"[{ts}]")
        if msg:
            lines.append(msg)
        lines.append("")

    return "\n".join(lines).strip()


def generate_rss(layout_data, posts, post_enums, services):
    layout_settings = (
        layout_data.get("layout", {})
        .get("layout_settings", {})
        .get("statusPage", {})
    )
    global_headline = layout_settings.get("globalStatusHeadline", "MotherDuck Status")

    # Build ID → name lookup tables from post_enums
    all_enums = post_enums.get("post_enums", [])
    severity_map = build_lookup(
        [e for e in all_enums if e.get("post_enum_type") == "severity"]
    )
    status_map = build_lookup(
        [e for e in all_enums if e.get("post_enum_type") == "status"]
    )
    # impacts severity uses the same enum set as incident severity
    impact_severity_map = severity_map

    service_map = build_lookup(services.get("services", []))

    # Build RSS
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text = "MotherDuck Status"
    SubElement(channel, "link").text = "https://status.motherduck.com"
    SubElement(channel, "description").text = global_headline
    SubElement(channel, "language").text = "en"
    SubElement(channel, "lastBuildDate").text = format_datetime(
        datetime.datetime.now(datetime.timezone.utc)
    )
    SubElement(channel, "ttl").text = "15"

    for post in posts:
        post_id = post.get("id", "")
        item = SubElement(channel, "item")
        SubElement(item, "title").text = build_item_title(post, severity_map)
        SubElement(item, "link").text = (
            f"https://status.motherduck.com/posts/details/{post_id}"
        )
        SubElement(item, "guid").text = post_id
        SubElement(item, "description").text = build_item_description(
            post, status_map, impact_severity_map, service_map
        )
        pub_ms = post.get("first_update_at") or post.get("last_update_at")
        SubElement(item, "pubDate").text = ms_to_rfc2822(pub_ms)

    if not posts:
        item = SubElement(channel, "item")
        SubElement(item, "title").text = f"All Systems Operational — {global_headline}"
        SubElement(item, "link").text = "https://status.motherduck.com"
        today = datetime.date.today().isoformat()
        SubElement(item, "guid").text = f"motherduck-no-incidents-{today}"
        SubElement(item, "description").text = "No incidents reported in the past 3 months."
        SubElement(item, "pubDate").text = format_datetime(
            datetime.datetime.now(datetime.timezone.utc)
        )

    xml_bytes = tostring(rss, encoding="unicode")
    dom = minidom.parseString(f'<?xml version="1.0" encoding="UTF-8"?>{xml_bytes}')
    return dom.toprettyxml(indent="  ", encoding=None).replace(
        '<?xml version="1.0" ?>', '<?xml version="1.0" encoding="UTF-8"?>'
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("Fetching layout data...")
    layout_data = fetch_json("api/data")

    print("Fetching post enumerations...")
    post_enums = fetch_json("api/post_enums")

    print("Fetching services...")
    services = fetch_json("api/services")

    print(f"Fetching posts for the last {MONTHS_BACK} months...")
    posts = fetch_posts()
    print(f"Found {len(posts)} posts.")

    feed_xml = generate_rss(layout_data, posts, post_enums, services)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(feed_xml)

    print(f"Wrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
