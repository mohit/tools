#!/usr/bin/env python3
"""Build vibe playlists from the latest deduplicated Last.fm history."""

from __future__ import annotations

import csv
import json
import math
import os
import re
import unicodedata
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

import duckdb

ROOT = Path(
    os.environ.get("DATALAKE_ROOT", "/Volumes/MoErMedia/datalake.me")
).expanduser()
LASTFM_GLOB = str(ROOT / "curated/lastfm/scrobbles/**/*.parquet")
APPLE_TRACKS_ZIP = ROOT / "raw/apple-music/Apple Music Library Tracks.json.zip"
APPLE_ACTIVITY_PARQUET = ROOT / "raw/apple-music/apple_music_play_activity.parquet"
OUTPUT_DIR = ROOT / "artifacts/lastfm-vibe-playlists-2026-07-22"
APPLE_STOREFRONT = "us"

EXCLUDED_TRACKS = {
    ("frank ocean", "chanel twinsick remix mixed"),
    ("frank ocean", "lost adrianisntadub remix mixed"),
    ("atoms for peace", "essential mix sat 03 09 2013"),
    ("nils frahm", "ode mixed"),
    ("can", "she brings the rain from bottom ein grosser graublauer"),
    ("dj danger mouse", "justify my thug"),
    ("lou reed", "magic and loss the summation"),
    ("wolfgang amadeus mozart", "greensleeves piano"),
    ("wolfgang amadeus mozart", "o fortuna"),
    ("johann sebastian bach", "track 5"),
    ("frederic chopin", "no 16"),
    ("frederic chopin", "no 21"),
    ("frederic chopin", "introduction"),
    ("frederic chopin", "no 15"),
    ("frederic chopin", "no 4"),
}


@dataclass(frozen=True)
class PlaylistSpec:
    number: int
    title: str
    slug: str
    description: str
    target_hours: float
    artists: tuple[tuple[str, int], ...]
    pins: tuple[tuple[str, str], ...] = ()
    min_duration: int = 90
    max_duration: int = 1_200


PLAYLISTS = (
    PlaylistSpec(
        1,
        "Blue Hour, Low Voltage",
        "blue-hour-low-voltage",
        "Intimate voices, negative space, sub-bass, and rainy-window electronics. "
        "The James Blake / xx / Beacon axis, refreshed with your recent returns to "
        "Mount Kimbie, SOHN, Nicolas Jaar, and SBTRKT.",
        4.0,
        (
            ("Milosh", 3),
            ("The xx", 5),
            ("Beacon", 5),
            ("Active Child", 3),
            ("Bon Iver", 3),
            ("FKA twigs", 3),
            ("SOHN", 4),
            ("James Blake", 6),
            ("Mount Kimbie", 5),
            ("SBTRKT", 4),
            ("Burial", 4),
            ("Holy Other", 3),
            ("Koreless", 2),
            ("Apparat", 4),
            ("Nicolas Jaar", 5),
            ("Darkside", 4),
            ("The Weeknd", 3),
            ("Frank Ocean", 3),
            ("King Krule", 3),
            ("How to Destroy Angels", 3),
        ),
        (
            ("James Blake", "Loading"),
            ("James Blake", "Before"),
            ("Mount Kimbie", "Maybes"),
            ("Mount Kimbie", "You Took Your Time (feat. King Krule)"),
            ("SOHN", "Signal"),
            ("Nicolas Jaar", "Colomb"),
            ("Beacon", "Backbone"),
            ("Frank Ocean", "Pyramids"),
            ("Apparat", "Hailin From the Edge"),
            ("SBTRKT", "Remnant"),
        ),
    ),
    PlaylistSpec(
        2,
        "Sunlit Circuitry",
        "sunlit-circuitry",
        "Warm, tactile IDM and downtempo for focused afternoons: soft percussion, "
        "glowing synths, and just enough rhythmic detail to keep the room moving.",
        4.0,
        (
            ("Tycho", 4),
            ("Bibio", 3),
            ("ODESZA", 3),
            ("Bonobo", 4),
            ("Caribou", 4),
            ("Gold Panda", 5),
            ("Four Tet", 6),
            ("Baths", 4),
            ("Shigeto", 3),
            ("Nosaj Thing", 4),
            ("Boards of Canada", 6),
            ("Aphex Twin", 5),
            ("Telefon Tel Aviv", 3),
            ("Jon Hopkins", 4),
            ("Kiasmos", 3),
            ("Rival Consoles", 3),
            ("Nils Frahm", 3),
            ("Air", 3),
            ("Zero 7", 3),
            ("Teebs", 3),
            ("Slow Magic", 3),
            ("Shlohmo", 3),
        ),
        (
            ("Gold Panda", "The Dream"),
            ("Four Tet", "LA Trance"),
            ("Four Tet", "Teenage Birdsong"),
            ("Four Tet", "Baby"),
            ("Boards of Canada", "Peacock Tail"),
            ("Boards of Canada", "Kid for Today"),
            ("Caribou", "You Can Do It"),
            ("Nosaj Thing", "Eclipse / Blue (feat. Kazu Makino)"),
            ("Kiasmos", "Looped"),
            ("Rival Consoles", "Untravel"),
            ("Aphex Twin", "Flim"),
            ("Jon Hopkins", "Breathe This Air (feat. Purity Ring)"),
        ),
    ),
    PlaylistSpec(
        3,
        "Chrome Disco, Soft Focus",
        "chrome-disco-soft-focus",
        "A late-night dance floor with human edges: French touch, loose house, disco "
        "basslines, indie-dance, and the ecstatic pop-remix streak in your newest plays.",
        4.0,
        (
            ("Kindness", 3),
            ("Jungle", 6),
            ("Romare", 4),
            ("Tensnake", 4),
            ("Dua Lipa", 5),
            ("Jessie Ware", 3),
            ("Hot Chip", 3),
            ("LCD Soundsystem", 5),
            ("Simian Mobile Disco", 4),
            ("Daft Punk", 6),
            ("Justice", 3),
            ("The Chemical Brothers", 3),
            ("Disclosure", 3),
            ("Totally Enormous Extinct Dinosaurs", 3),
            ("John Talabot", 4),
            ("Maya Jane Coles", 3),
            ("Matthew Dear", 3),
            ("DJ Koze", 5),
            ("Daphni", 3),
            ("Jamie xx", 4),
            ("Trentemøller", 2),
            ("Röyksopp", 3),
            ("Metronomy", 3),
            ("Sylvester", 2),
            ("William Onyeabor", 2),
        ),
        (
            ("Jungle", "PROBLEMZ"),
            ("Jungle", "Someday, Somewhere"),
            ("Romare", "Sunshine"),
            ("Dua Lipa", "Break My Heart (Moodymann remix)"),
            ("Dua Lipa", "Don’t Start Now (Kaytranada Remix)"),
            ("LCD Soundsystem", "Daft Punk Is Playing at My House"),
            ("LCD Soundsystem", "I Can Change"),
            ("Simian Mobile Disco", "I Believe"),
            ("DJ Koze", "XTC"),
            ("DJ Koze", "Seeing Aliens"),
            ("Daphni", "Sizzling (feat. Paradise)"),
            ("Jamie xx", "Dream Night"),
            ("Sylvester", 'Over And Over (12" Version)'),
        ),
    ),
    PlaylistSpec(
        4,
        "Weather Systems in Bloom",
        "weather-systems-in-bloom",
        "Art-pop that changes the pressure in the room: Björk and Radiohead at the "
        "center, surrounded by dream-pop, left-field R&B, and beautifully odd voices.",
        4.0,
        (
            ("Björk", 7),
            ("Radiohead", 7),
            ("Blonde Redhead", 5),
            ("Little Dragon", 4),
            ("Grimes", 3),
            ("Beach House", 3),
            ("Cocteau Twins", 3),
            ("Stereolab", 3),
            ("Sylvan Esso", 3),
            ("Laurie Anderson", 3),
            ("M83", 3),
            ("Panda Bear", 3),
            ("Feist", 3),
            ("Frou Frou", 3),
            ("Lana Del Rey", 3),
            ("Thom Yorke", 4),
            ("Atoms for Peace", 3),
            ("Solange", 3),
            ("Moses Sumney", 3),
            ("Kate Bush", 3),
            ("Bat for Lashes", 3),
            ("Sigur Rós", 3),
            ("The Cure", 3),
            ("This Mortal Coil", 2),
        ),
        (
            ("The Cure", "And Nothing Is Forever - Cosmodelica Electrick Eden Remix"),
            ("This Mortal Coil", "Drugs"),
            ("Lana Del Rey", "Doin' Time"),
            ("Stereolab", "The Flower Called Nowhere (Instrumental)"),
            ("Solange", "Borderline (An Ode to Self Care) [feat. Q-Tip]"),
            ("Björk", "Come to Me"),
            ("Björk", "Hidden Place"),
            ("Radiohead", "Exit Music (For a Film) [Remastered]"),
            ("Radiohead", "Separator"),
        ),
    ),
    PlaylistSpec(
        5,
        "Cathedral of Riffs",
        "cathedral-of-riffs",
        "Long-form tension and release: prog architecture, industrial abrasion, doom "
        "weight, and the metal records that defined the earliest years of your history.",
        4.35,
        (
            ("Pink Floyd", 4),
            ("King Crimson", 4),
            ("Porcupine Tree", 5),
            ("Riverside", 4),
            ("Maybeshewill", 4),
            ("Anathema", 5),
            ("A Perfect Circle", 5),
            ("Tool", 5),
            ("Nine Inch Nails", 5),
            ("Black Light Burns", 3),
            ("Deftones", 3),
            ("Katatonia", 4),
            ("Opeth", 3),
            ("Alice in Chains", 4),
            ("Black Sabbath", 4),
            ("Led Zeppelin", 4),
            ("Blue Öyster Cult", 3),
            ("Metallica", 5),
            ("Megadeth", 3),
            ("Rage Against the Machine", 3),
            ("The Stooges", 2),
            ("Black Flag", 2),
        ),
        (
            ("The Stooges", "Loose"),
            ("Black Flag", "Rise Above"),
            ("Metallica", "Seek & Destroy"),
            ("Metallica", "Until It Sleeps (Remastered)"),
        ),
        120,
        1_800,
    ),
    PlaylistSpec(
        6,
        "Dust on the MPC",
        "dust-on-the-mpc",
        "Cracked drums, sample dust, future-soul, and nocturnal rap. It moves from "
        "Dilla and Madlib through the Flying Lotus / Brainfeeder orbit into Jai Paul, "
        "SBTRKT, and the hazier edge of R&B.",
        3.8,
        (
            ("J Dilla", 6),
            ("Pete Rock", 3),
            ("Madvillain", 4),
            ("Madlib", 4),
            ("Flying Lotus", 7),
            ("Nosaj Thing", 4),
            ("Ta-ku", 4),
            ("Shlohmo", 3),
            ("XXYYXX", 3),
            ("fLako", 3),
            ("Jel", 3),
            ("Wax Tailor", 3),
            ("Kutiman", 3),
            ("DJ Danger Mouse", 3),
            ("Jay-Z + DJ Danger Mouse", 3),
            ("The Notorious B.I.G.", 3),
            ("Kendrick Lamar", 3),
            ("Lauryn Hill", 3),
            ("M.I.A.", 3),
            ("Gorillaz", 3),
            ("Thundercat", 3),
            ("Little Simz", 3),
            ("Jai Paul", 5),
            ("SBTRKT", 4),
            ("Drake", 4),
            ("The Weeknd", 3),
            ("King Krule", 3),
            ("Solange", 3),
            ("SZA", 3),
        ),
        (
            ("Flying Lotus", "Zodiac Shit"),
            ("Flying Lotus", "Unexpected Delight (feat. Laura Darlington)"),
            ("Madvillain", "Meat Grinder"),
            ("J Dilla", "Reckless Driving"),
            ("Madlib", "Distant Land"),
            ("SBTRKT", "Waiting (feat. Teezo Touchdown)"),
        ),
        75,
        900,
    ),
    PlaylistSpec(
        7,
        "Old Souls, Open Road",
        "old-souls-open-road",
        "Songs that feel lived in: folk confessionals, smoky soul, downtown poets, "
        "and classic records built for a long drive with nowhere urgent to be.",
        4.0,
        (
            ("Bob Dylan", 5),
            ("Joni Mitchell", 5),
            ("Leonard Cohen", 5),
            ("Nina Simone", 5),
            ("Simon & Garfunkel", 4),
            ("The Beatles", 6),
            ("John Lennon", 3),
            ("The Band", 3),
            ("Buffalo Springfield", 3),
            ("Creedence Clearwater Revival", 3),
            ("Fleetwood Mac", 3),
            ("The Doors", 5),
            ("The Velvet Underground", 4),
            ("Lou Reed", 4),
            ("David Bowie", 3),
            ("Arthur Russell", 3),
            ("Nick Drake", 4),
            ("Iron & Wine", 4),
            ("Elliott Smith", 4),
            ("Jeff Buckley", 3),
            ("Feist", 3),
            ("Joan Baez", 3),
            ("Janis Joplin", 3),
        ),
        (
            ("Arthur Russell", "Close My Eyes"),
            ("Fleetwood Mac", "Sara"),
        ),
    ),
    PlaylistSpec(
        8,
        "Desert Radio After Midnight",
        "desert-radio-after-midnight",
        "Krautrock pulse, psychedelic haze, desert blues, Afro-disco, and oddball "
        "groove—the wide-open, crate-digging side of your listening history.",
        4.1,
        (
            ("Can", 6),
            ("Black Moth Super Rainbow", 6),
            ("Tinariwen", 5),
            ("William Onyeabor", 4),
            ("Phish", 4),
            ("Gomez", 5),
            ("Soul Coughing", 5),
            ("Talking Heads", 5),
            ("Brian Eno & David Byrne", 4),
            ("Kutiman", 4),
            ("Midival Punditz", 4),
            ("Ratatat", 4),
            ("Panda Bear", 4),
            ("The Flaming Lips", 3),
            ("MGMT", 3),
            ("Jefferson Airplane", 3),
            ("Mulatu Astatke", 3),
            ("Roy Ayers", 3),
            ("Thievery Corporation", 3),
            ("Nightmares on Wax", 3),
            ("Allah-Las", 2),
            ("Secret Circuit", 2),
            ("Da Lata", 2),
            ("Larry Heard", 2),
        ),
        (
            ("Allah-Las", "Raspberry Jam - Kyle Mullarky Dub"),
            ("Larry Heard", "Summertime Breeze"),
            ("Da Lata", "Jungle Kitten - Full Version"),
            ("Secret Circuit", "Jungle Dogs / Jungle Bones (Original Mix)"),
            ("Nightmares on Wax", "African Pirates"),
            ("Thievery Corporation", "Lebanese Blonde"),
            ("The Flaming Lips", "Moth in the Incubator"),
            ("Black Moth Super Rainbow", "Boatfriend"),
        ),
    ),
    PlaylistSpec(
        9,
        "Rooms Without Clocks",
        "rooms-without-clocks",
        "Ambient, modern classical, and film-score drift for reading, writing, or "
        "watching daylight move across the wall. Minimal motion, maximal atmosphere.",
        4.2,
        (
            ("Erik Satie", 7),
            ("Brian Eno", 7),
            ("Philip Glass", 6),
            ("Philip Glass Ensemble, The", 4),
            ("Clint Mansell", 5),
            ("Yann Tiersen", 5),
            ("Vangelis", 4),
            ("Nils Frahm", 4),
            ("Jon Hopkins", 4),
            ("Sigur Rós", 4),
            ("Boards of Canada", 4),
            ("Howard Shore", 4),
            ("Michael Andrews", 4),
            ("Wolfgang Amadeus Mozart", 3),
            ("Johann Sebastian Bach", 3),
            ("Frédéric Chopin", 3),
            ("Air", 3),
            ("M83", 3),
            ("John Cage", 3),
            ("Laurie Anderson", 3),
        ),
        (
            ("Brian Eno", "The Big Ship"),
            ("Boards of Canada", "Reach for the Dead"),
            ("Boards of Canada", "An Eagle in Your Mind"),
            ("Wolfgang Amadeus Mozart", "Requiem"),
            ("Wolfgang Amadeus Mozart", "Confutatis"),
            ("Johann Sebastian Bach", "Prelude in C major, BWV846"),
            ("Johann Sebastian Bach", "Toccata and Fugue in D Minor"),
            ("Frédéric Chopin", "Funeral March"),
            ("Frédéric Chopin", "Nocturne In G Major, Op. 37/2"),
        ),
        45,
        1_800,
    ),
)


def normalize(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.casefold().replace("&", " and ")
    text = text.replace("’", "'").replace("‘", "'").replace("–", "-").replace("—", "-")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def base_title(value: str | None) -> str:
    text = value or ""
    text = re.sub(
        r"\s*[\[(](?:[^)\]]*(?:remaster|deluxe|album version|radio edit|"
        r"edit version|original mix|bonus)[^)\]]*)[\])]",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\s*[-–—]\s*(?:\d{4}\s*)?(?:remaster(?:ed)?|original mix|"
        r"edit(?: version)?|album version)\b.*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return normalize(text)


def format_duration(seconds: int) -> str:
    hours, remainder = divmod(int(seconds), 3_600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def load_apple_library() -> tuple[
    dict[tuple[str, str], dict[str, Any]], dict[str, list[dict[str, Any]]]
]:
    exact: dict[tuple[str, str], dict[str, Any]] = {}
    by_title: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with zipfile.ZipFile(APPLE_TRACKS_ZIP) as archive:
        member = archive.namelist()[0]
        tracks = json.loads(archive.read(member))

    def quality(track: dict[str, Any]) -> tuple[int, int, int]:
        return (
            int(bool(track.get("Apple Music Track Identifier"))),
            int(track.get("Track Play Count") or 0),
            int(track.get("Track Duration") or 0),
        )

    for track in tracks:
        artist = normalize(track.get("Artist") or track.get("Album Artist"))
        title = normalize(track.get("Title"))
        if not artist or not title:
            continue
        key = (artist, title)
        if key not in exact or quality(track) > quality(exact[key]):
            exact[key] = track
        by_title[title].append(track)
        simplified = base_title(track.get("Title"))
        if simplified and simplified != title:
            by_title[simplified].append(track)
    return exact, by_title


def load_apple_activity_durations() -> tuple[
    dict[tuple[str, str], int], dict[str, int]
]:
    connection = duckdb.connect()
    cursor = connection.execute(
        f"""
        SELECT "Song Name" AS title,
               "Album Name" AS album,
               median(try_cast("Media Duration In Milliseconds" AS DOUBLE)) / 1000
                   AS duration_seconds
        FROM read_parquet('{APPLE_ACTIVITY_PARQUET}')
        WHERE try_cast("Media Duration In Milliseconds" AS DOUBLE)
              BETWEEN 45000 AND 1800000
        GROUP BY title, album
        """
    )
    exact: dict[tuple[str, str], int] = {}
    by_title_values: dict[str, list[int]] = defaultdict(list)
    for title, album, duration in cursor.fetchall():
        title_key = normalize(title)
        album_key = normalize(album)
        seconds = round(duration)
        if title_key:
            exact[(title_key, album_key)] = seconds
            by_title_values[title_key].append(seconds)
    connection.close()

    # A title-only fallback is safe when all observed catalog durations agree closely.
    by_title = {}
    for title, durations in by_title_values.items():
        if durations and max(durations) / min(durations) <= 1.1:
            by_title[title] = round(median(durations))
    return exact, by_title


def match_apple_track(
    artist: str,
    title: str,
    exact: dict[tuple[str, str], dict[str, Any]],
    by_title: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    artist_key = normalize(artist)
    title_key = normalize(title)
    if (artist_key, title_key) in exact:
        return exact[(artist_key, title_key)]
    candidates = by_title.get(title_key, []) + by_title.get(base_title(title), [])
    artist_tokens = set(artist_key.split())
    ranked = []
    for candidate in candidates:
        candidate_artist = normalize(
            candidate.get("Artist") or candidate.get("Album Artist")
        )
        overlap = len(artist_tokens & set(candidate_artist.split()))
        if overlap:
            ranked.append(
                (
                    overlap,
                    int(bool(candidate.get("Apple Music Track Identifier"))),
                    int(candidate.get("Track Play Count") or 0),
                    candidate,
                )
            )
    return max(ranked, key=lambda item: item[:3])[3] if ranked else None


def load_lastfm_stats() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    connection = duckdb.connect()
    connection.execute(
        f"""
        CREATE TEMP VIEW raw AS
        SELECT uts, played_at_utc, artist, track, album, mbid_track
        FROM read_parquet('{LASTFM_GLOB}', hive_partitioning=true)
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE scrobbles AS
        SELECT * EXCLUDE (row_number)
        FROM (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY uts, lower(artist), lower(track)
                       ORDER BY (mbid_track IS NOT NULL) DESC,
                                length(coalesce(album, '')) DESC
                   ) AS row_number
            FROM raw
        )
        WHERE row_number = 1
        """
    )
    connection.execute(
        """
        CREATE TEMP VIEW timed AS
        SELECT *,
               epoch(
                   lead(played_at_utc) OVER (ORDER BY played_at_utc) - played_at_utc
               ) AS next_gap
        FROM scrobbles
        """
    )
    cursor = connection.execute(
        """
        SELECT artist,
               track,
               arg_max(album, played_at_utc) AS album,
               arg_max(mbid_track, played_at_utc) AS mbid_track,
               count(*) AS plays,
               count(DISTINCT year(played_at_utc)) AS years_heard,
               min(played_at_utc) AS first_heard,
               max(played_at_utc) AS last_heard,
               median(next_gap) FILTER (WHERE next_gap BETWEEN 45 AND 1800) AS gap_seconds,
               min(next_gap) FILTER (WHERE next_gap BETWEEN 45 AND 1800) AS min_gap_seconds,
               count(*) FILTER (WHERE next_gap BETWEEN 45 AND 1800) AS gap_samples
        FROM timed
        GROUP BY artist, track
        """
    )
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    summary_cursor = connection.execute(
        """
        SELECT count(*) AS plays,
               count(DISTINCT artist) AS artists,
               count(DISTINCT (lower(artist), lower(track))) AS tracks,
               min(played_at_utc) AS first_play,
               max(played_at_utc) AS last_play
        FROM scrobbles
        """
    )
    summary_columns = [column[0] for column in summary_cursor.description]
    summary = dict(zip(summary_columns, summary_cursor.fetchone()))
    connection.close()
    return rows, summary


def enrich_stats(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    apple_exact, apple_by_title = load_apple_library()
    activity_exact, activity_by_title = load_apple_activity_durations()
    by_artist: dict[str, list[dict[str, Any]]] = defaultdict(list)
    provisional_durations: dict[str, list[int]] = defaultdict(list)
    global_durations: list[int] = []

    for row in rows:
        apple = match_apple_track(
            row["artist"], row["track"], apple_exact, apple_by_title
        )
        row["apple"] = apple
        apple_seconds = None
        if apple and apple.get("Track Duration"):
            apple_seconds = round(int(apple["Track Duration"]) / 1_000)
        activity_seconds = activity_exact.get(
            (normalize(row["track"]), normalize(row["album"]))
        ) or activity_by_title.get(normalize(row["track"]))
        gap_value = (
            row["min_gap_seconds"] if row["gap_samples"] <= 3 else row["gap_seconds"]
        )
        gap_seconds = round(gap_value) if gap_value else None
        duration = (
            apple_seconds
            if apple_seconds and 45 <= apple_seconds <= 1_800
            else activity_seconds or gap_seconds
        )
        row["duration_seconds"] = duration
        artist_key = normalize(row["artist"])
        if duration and 45 <= duration <= 1_800:
            provisional_durations[artist_key].append(duration)
            global_durations.append(duration)

    global_fallback = round(median(global_durations)) if global_durations else 240
    artist_fallback = {
        artist: round(median(durations))
        for artist, durations in provisional_durations.items()
        if durations
    }
    for row in rows:
        artist_key = normalize(row["artist"])
        row["duration_seconds"] = int(
            row["duration_seconds"]
            or artist_fallback.get(artist_key)
            or global_fallback
        )
        last_year = row["last_heard"].year
        recency = (
            1.6
            if last_year >= 2026
            else 1.0
            if last_year >= 2025
            else 0.5
            if last_year >= 2023
            else 0
        )
        favorite = (
            0.5
            if row["apple"] and row["apple"].get("Favorite Status - Track") is True
            else 0
        )
        row["score"] = (
            2.2 * math.log1p(row["plays"])
            + 0.22 * row["years_heard"]
            + recency
            + favorite
        )
        by_artist[artist_key].append(row)

    for tracks in by_artist.values():
        deduplicated: dict[str, dict[str, Any]] = {}
        for track in tracks:
            key = base_title(track["track"])
            if key not in deduplicated or track["score"] > deduplicated[key]["score"]:
                deduplicated[key] = track
        tracks[:] = sorted(
            deduplicated.values(),
            key=lambda item: (item["score"], item["plays"], item["last_heard"]),
            reverse=True,
        )
    return by_artist


def find_pin(
    by_artist: dict[str, list[dict[str, Any]]], artist: str, title: str
) -> dict[str, Any] | None:
    title_key = normalize(title)
    base_key = base_title(title)
    for track in by_artist.get(normalize(artist), []):
        if (
            normalize(track["track"]) == title_key
            or base_title(track["track"]) == base_key
        ):
            return track
    return None


def select_playlist(
    spec: PlaylistSpec,
    by_artist: dict[str, list[dict[str, Any]]],
    globally_used: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str]] = set()

    def is_rejected(track: dict[str, Any]) -> bool:
        raw_key = (normalize(track["artist"]), normalize(track["track"]))
        return raw_key in EXCLUDED_TRACKS or bool(
            re.search(
                r"(?:\[mixed\]|\(mixed\)|essential mix|\binterview\b)",
                track["track"],
                re.IGNORECASE,
            )
        )

    def add(track: dict[str, Any], *, force: bool = False) -> bool:
        key = (normalize(track["artist"]), base_title(track["track"]))
        if key in selected_keys or key in globally_used or is_rejected(track):
            return False
        duration = track["duration_seconds"]
        if not force and not (spec.min_duration <= duration <= spec.max_duration):
            return False
        if (
            normalize(track["artist"]) == "unknown"
            or normalize(track["track"]) == "unknown"
        ):
            return False
        selected.append(track)
        selected_keys.add(key)
        return True

    for artist, title in spec.pins:
        pin = find_pin(by_artist, artist, title)
        if pin:
            pin["is_pin"] = True
            add(pin, force=True)

    candidates: dict[str, list[dict[str, Any]]] = {}
    caps: dict[str, int] = {}
    for artist, cap in spec.artists:
        artist_key = normalize(artist)
        caps[artist_key] = cap
        candidates[artist_key] = [
            track
            for track in by_artist.get(artist_key, [])
            if spec.min_duration <= track["duration_seconds"] <= spec.max_duration
            and not is_rejected(track)
        ]

    target_seconds = round(spec.target_hours * 3_600)
    max_rounds = max(caps.values())
    target_reached = False
    for round_number in range(max_rounds):
        artist_wave = (
            spec.artists if round_number % 2 == 0 else tuple(reversed(spec.artists))
        )
        for artist, _ in artist_wave:
            artist_key = normalize(artist)
            if round_number >= caps[artist_key]:
                continue
            available = [
                track
                for track in candidates[artist_key]
                if (normalize(track["artist"]), base_title(track["track"]))
                not in selected_keys
                and (normalize(track["artist"]), base_title(track["track"]))
                not in globally_used
            ]
            if available:
                add(available[0])
            # Complete the first wave so every lane in the vibe gets represented.
            # On later waves, stop as soon as the target runtime is reached.
            if (
                round_number > 0
                and sum(track["duration_seconds"] for track in selected)
                >= target_seconds
            ):
                target_reached = True
                break
        if target_reached:
            break

    # If short because a pool contains many brief pieces or duplicate crossovers, extend
    # from the same artists without weakening the vibe definition.
    if sum(track["duration_seconds"] for track in selected) < target_seconds:
        extension = []
        for artist, _ in spec.artists:
            extension.extend(candidates[normalize(artist)])
        extension.sort(key=lambda item: item["score"], reverse=True)
        for track in extension:
            add(track)
            if sum(item["duration_seconds"] for item in selected) >= target_seconds:
                break

    # Sequence in waves: one track per artist before a second track by the same artist.
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for track in selected:
        grouped[normalize(track["artist"])].append(track)
    for tracks in grouped.values():
        tracks.sort(
            key=lambda item: (bool(item.get("is_pin")), item["score"], item["plays"]),
            reverse=True,
        )

    ordered: list[dict[str, Any]] = []
    max_group = max((len(tracks) for tracks in grouped.values()), default=0)
    for round_number in range(max_group):
        for artist, _ in spec.artists:
            tracks = grouped[normalize(artist)]
            if round_number < len(tracks):
                ordered.append(tracks[round_number])

    globally_used.update(
        (normalize(track["artist"]), base_title(track["track"])) for track in ordered
    )
    return ordered


def apple_url(track: dict[str, Any]) -> str:
    apple = track.get("apple")
    identifier = apple.get("Apple Music Track Identifier") if apple else None
    return (
        f"https://music.apple.com/{APPLE_STOREFRONT}/song/{identifier}"
        if identifier
        else ""
    )


def write_playlist_files(
    spec: PlaylistSpec, tracks: list[dict[str, Any]]
) -> dict[str, Any]:
    prefix = f"{spec.number:02d}-{spec.slug}"
    csv_path = OUTPUT_DIR / f"{prefix}.csv"
    txt_path = OUTPUT_DIR / f"{prefix}.txt"
    json_path = OUTPUT_DIR / f"{prefix}.json"

    fieldnames = (
        "position",
        "title",
        "artist",
        "album",
        "duration",
        "duration_seconds",
        "lastfm_scrobbles",
        "first_heard",
        "last_heard",
        "apple_genre",
        "apple_favorite",
        "apple_music_url",
        "musicbrainz_track_id",
    )
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for position, track in enumerate(tracks, start=1):
            apple = track.get("apple") or {}
            writer.writerow(
                {
                    "position": position,
                    "title": track["track"],
                    "artist": track["artist"],
                    "album": track["album"] or "",
                    "duration": format_duration(track["duration_seconds"]),
                    "duration_seconds": track["duration_seconds"],
                    "lastfm_scrobbles": track["plays"],
                    "first_heard": track["first_heard"].date().isoformat(),
                    "last_heard": track["last_heard"].date().isoformat(),
                    "apple_genre": apple.get("Genre") or "",
                    "apple_favorite": apple.get("Favorite Status - Track") is True,
                    "apple_music_url": apple_url(track),
                    "musicbrainz_track_id": track["mbid_track"] or "",
                }
            )

    txt_path.write_text(
        "\n".join(f"{track['artist']} — {track['track']}" for track in tracks) + "\n",
        encoding="utf-8",
    )

    payload = {
        "title": spec.title,
        "description": spec.description,
        "tracks": [
            {
                "position": position,
                "title": track["track"],
                "artist": track["artist"],
                "album": track["album"],
                "duration_seconds": track["duration_seconds"],
                "lastfm_scrobbles": track["plays"],
                "first_heard": track["first_heard"].date().isoformat(),
                "last_heard": track["last_heard"].date().isoformat(),
                "apple_music_url": apple_url(track) or None,
            }
            for position, track in enumerate(tracks, start=1)
        ],
    }
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    return {
        "spec": spec,
        "tracks": tracks,
        "duration_seconds": sum(track["duration_seconds"] for track in tracks),
        "apple_matches": sum(bool(apple_url(track)) for track in tracks),
        "csv": csv_path.name,
        "txt": txt_path.name,
        "json": json_path.name,
    }


def write_readme(results: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    lines = [
        "# Last.fm vibe playlists",
        "",
        (
            f"Built from **{summary['plays']:,} unique scrobbles** spanning "
            f"**{summary['first_play'].date().isoformat()} → "
            f"{summary['last_play'].date().isoformat()}**, after deduplicating repeated "
            "full and incremental snapshots."
        ),
        "",
        (
            "Every selected track appears in the Last.fm history. Selection balances repeat "
            "count, number of years heard, and a modest recent-listening boost. No track is "
            "repeated across playlists. Exact Apple Music library durations are used where "
            "available; otherwise duration is estimated from the median gap to the next "
            "scrobble. Apple Music links are included only when the local library export "
            "contains a catalog ID."
        ),
        "",
        "## The playlists",
        "",
    ]
    for result in results:
        spec = result["spec"]
        tracks = result["tracks"]
        recent = sum(track["last_heard"].year >= 2025 for track in tracks)
        lines.extend(
            [
                f"### {spec.number}. {spec.title}",
                "",
                spec.description,
                "",
                (
                    f"**{len(tracks)} tracks · {format_duration(result['duration_seconds'])} · "
                    f"{result['apple_matches']} Apple Music links · {recent} tracks revisited "
                    "in 2025–2026**"
                ),
                "",
                (
                    f"Files: [{result['csv']}]({result['csv']}) · "
                    f"[plain text]({result['txt']}) · [{result['json']}]({result['json']})"
                ),
                "",
                "<details>",
                "<summary>Track list</summary>",
                "",
            ]
        )
        for position, track in enumerate(tracks, start=1):
            link = apple_url(track)
            song = f"[{track['track']}]({link})" if link else track["track"]
            lines.append(
                f"{position}. {track['artist']} — {song} "
                f"({format_duration(track['duration_seconds'])})"
            )
        lines.extend(["", "</details>", ""])

    lines.extend(
        [
            "## Files and import",
            "",
            (
                "Each CSV includes ordering, album, runtime, Last.fm play count, first/last "
                "heard dates, Apple genre/favorite metadata, catalog URL, and MusicBrainz ID. "
                "The plain-text version is designed for quick copy/paste into a playlist "
                "transfer or catalog-search tool."
            ),
            "",
            "Generated by `scripts/generate_lastfm_vibe_playlists.py`.",
            "",
        ]
    )
    (OUTPUT_DIR / "README.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    if not ROOT.is_dir() or (
        str(ROOT).startswith("/Volumes/MoErMedia/")
        and not os.path.ismount("/Volumes/MoErMedia")
    ):
        raise RuntimeError(f"Datalake is not mounted: {ROOT}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    rows, summary = load_lastfm_stats()
    by_artist = enrich_stats(rows)
    globally_used: set[tuple[str, str]] = set()
    results = []
    for spec in PLAYLISTS:
        tracks = select_playlist(spec, by_artist, globally_used)
        results.append(write_playlist_files(spec, tracks))
    write_readme(results, summary)

    print(
        f"Built {len(results)} playlists from {summary['plays']:,} unique scrobbles "
        f"through {summary['last_play'].date().isoformat()}."
    )
    for result in results:
        print(
            f"{result['spec'].number:02d}. {result['spec'].title}: "
            f"{len(result['tracks'])} tracks, "
            f"{format_duration(result['duration_seconds'])}, "
            f"{result['apple_matches']} Apple Music links"
        )


if __name__ == "__main__":
    main()
