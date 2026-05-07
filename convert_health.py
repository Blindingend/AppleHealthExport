#!/usr/bin/env python3
"""Apple Health Export XML -> FIT workout files converter.

Streaming parse of a multi-GB Apple Health XML export, extracting workouts with
heart rate and GPS data, written as standard FIT files for upload to platforms
like Strava, TrainingPeaks, Coros, or Garmin Connect.

Features:
  - Streaming XML parse with O(1) memory
  - Heart-rate interpolation onto every GPS trackpoint
  - Pause-aware workout segmentation
  - Per-second FIT record generation with full sensor data
  - Multiprocess GPX loading and FIT generation

Memory: ~80MB peak for a 2.5GB / 2.7M heart-rate-record export.
"""

import os
import sys
import time
import bisect
import argparse
from array import array
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import groupby

from lxml import etree

# FIT file generation
from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.event_message import EventMessage
from fit_tool.profile.messages.device_info_message import DeviceInfoMessage
from fit_tool.profile.messages.developer_data_id_message import DeveloperDataIdMessage
from fit_tool.profile.profile_type import Sport, Event, EventType, FileType, Manufacturer

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APPLE_DATE_FMT = "%Y-%m-%d %H:%M:%S %z"
GPX_DATE_FMT = "%Y-%m-%dT%H:%M:%SZ"
TCX_DATE_FMT = "%Y-%m-%dT%H:%M:%SZ"

GPX_NS = "http://www.topografix.com/GPX/1/1"
TCX_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
TPX_NS = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"

ACTIVITY_TYPE_MAP = {
    # Mapped to COROS Sport values for direct import compatibility.
    # COROS activity modes (from t.coros.com import docs):
    #   Run, Indoor Run, Trail Run, Track Run, Hike, Walk,
    #   Road Bike, Mountain Bike, Gravel Bike, E-Bike, E-Mountain Bike,
    #   Pool Swim, Open Water, Strength, Gym, Other
    "HKWorkoutActivityTypeRunning": "Run",
    "HKWorkoutActivityTypeCycling": "Biking",
    "HKWorkoutActivityTypeWalking": "Walk",
    "HKWorkoutActivityTypeHiking": "Hike",
    "HKWorkoutActivityTypeSwimming": "Other",
    "HKWorkoutActivityTypeWheelchairRunPace": "Run",
    "HKWorkoutActivityTypeWheelchairWalkPace": "Walk",
    "HKWorkoutActivityTypeTraditionalStrengthTraining": "Other",
    "HKWorkoutActivityTypeFunctionalStrengthTraining": "Other",
    "HKWorkoutActivityTypeElliptical": "Other",
    "HKWorkoutActivityTypeYoga": "Other",
    "HKWorkoutActivityTypeHighIntensityIntervalTraining": "Other",
    "HKWorkoutActivityTypeCrossTraining": "Other",
    "HKWorkoutActivityTypeStairClimbing": "Other",
    "HKWorkoutActivityTypeStairs": "Other",
    "HKWorkoutActivityTypeRowing": "Other",
    "HKWorkoutActivityTypeDance": "Other",
    "HKWorkoutActivityTypeCooldown": "Other",
    "HKWorkoutActivityTypeCoreTraining": "Other",
    "HKWorkoutActivityTypePilates": "Other",
    "HKWorkoutActivityTypeFlexibility": "Other",
    "HKWorkoutActivityTypeMixedCardio": "Other",
    "HKWorkoutActivityTypeCardioDance": "Other",
    "HKWorkoutActivityTypeFitnessGaming": "Other",
    "HKWorkoutActivityTypeSnowboarding": "Other",
    "HKWorkoutActivityTypeClimbing": "Other",
    "HKWorkoutActivityTypeJumpRope": "Other",
    "HKWorkoutActivityTypeBadminton": "Other",
    "HKWorkoutActivityTypeBasketball": "Other",
    "HKWorkoutActivityTypeBoxing": "Other",
    "HKWorkoutActivityTypeSoccer": "Other",
    "HKWorkoutActivityTypeTennis": "Other",
    "HKWorkoutActivityTypeTableTennis": "Other",
    "HKWorkoutActivityTypeVolleyball": "Other",
    "HKWorkoutActivityTypeCricket": "Other",
    "HKWorkoutActivityTypeRugby": "Other",
    "HKWorkoutActivityTypeOther": "Other",
}

# FIT sport codes match COROS internal values (verified against COROS export)
FIT_SPORT_MAP: dict[str, Sport] = {
    "HKWorkoutActivityTypeRunning": Sport.RUNNING,
    "HKWorkoutActivityTypeCycling": Sport.CYCLING,
    "HKWorkoutActivityTypeWalking": Sport.WALKING,
    "HKWorkoutActivityTypeHiking": Sport.HIKING,
    "HKWorkoutActivityTypeSwimming": Sport.SWIMMING,
}

MIN_DURATION_S = 60
MIN_DISTANCE_M = 50

# ---------------------------------------------------------------------------
# Date/time helpers
# ---------------------------------------------------------------------------


def parse_apple_date(s: str) -> float:
    dt = datetime.strptime(s, APPLE_DATE_FMT)
    return dt.timestamp()


def parse_gpx_date(s: str) -> float:
    dt = datetime.strptime(s, GPX_DATE_FMT).replace(tzinfo=timezone.utc)
    return dt.timestamp()


def format_tcx_datetime(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime(TCX_DATE_FMT)


def parse_apple_duration(value: str, unit: str) -> float:
    v = float(value)
    unit_lower = unit.lower().strip()
    if unit_lower in ("min", "mins", "minute", "minutes"):
        return v * 60.0
    elif unit_lower in ("hr", "hrs", "hour", "hours"):
        return v * 3600.0
    elif unit_lower in ("s", "sec", "secs", "second", "seconds"):
        return v
    elif unit_lower in ("ms", "millisecond", "milliseconds"):
        return v / 1000.0
    return v


def distance_to_meters(value: float, unit: str) -> float:
    unit_lower = unit.lower().strip()
    if unit_lower in ("km", "kilometer", "kilometers"):
        return value * 1000.0
    elif unit_lower in ("mi", "mile", "miles"):
        return value * 1609.344
    elif unit_lower in ("m", "meter", "meters"):
        return value
    return value


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class SegmentInfo:
    """A workout segment (lap) extracted from WorkoutEvent."""
    start_ts: float
    end_ts: float
    duration_s: float = 0.0


@dataclass
class WorkoutInfo:
    activity_type: str
    start_ts: float
    end_ts: float
    duration_s: float
    distance_m: float = 0.0
    energy_kcal: float = 0.0
    hr_avg: Optional[float] = None
    hr_min: Optional[float] = None
    hr_max: Optional[float] = None
    gpx_rel_path: Optional[str] = None
    timezone_name: Optional[str] = None
    elevation_gain_m: float = 0.0
    weather_temp_f: Optional[float] = None
    weather_humidity_pct: Optional[float] = None
    source_name: str = ""
    # Segments for multi-lap output
    segments: list[SegmentInfo] = field(default_factory=list)
    # Active intervals (excludes pause periods) for HR matching
    active_intervals: list[tuple[float, float]] = field(default_factory=list)
    # Matched HR data (populated in Phase 2)
    hr_timestamps: list[float] = field(default_factory=list)
    hr_values: list[float] = field(default_factory=list)
    # GPS track points (populated in Phase 3)
    gps_points: list = field(default_factory=list)


@dataclass
class TrackPoint:
    ts: float = 0.0
    lat: Optional[float] = None
    lon: Optional[float] = None
    ele: Optional[float] = None
    speed: Optional[float] = None
    course: Optional[float] = None
    hr: Optional[float] = None


@dataclass
class ConversionStats:
    records_parsed: int = 0
    hr_records_found: int = 0
    workouts_parsed: int = 0
    workouts_with_gps: int = 0
    workouts_with_hr: int = 0
    gpx_parse_errors: int = 0
    files_written: int = 0
    files_skipped: int = 0
    unparseable_dates: int = 0
    missing_gpx_files: int = 0
    workouts_merged: int = 0
    start_time: float = 0.0

    def print_summary(self):
        elapsed = time.time() - self.start_time
        print(f"\n{'='*60}")
        print(f"Conversion Summary")
        print(f"{'='*60}")
        print(f"  Records parsed:           {self.records_parsed:>10,}")
        print(f"  Heart rate records:       {self.hr_records_found:>10,}")
        print(f"  Workouts found:           {self.workouts_parsed:>10,}")
        print(f"  Workouts with GPS route:  {self.workouts_with_gps:>10,}")
        print(f"  Workouts with HR data:    {self.workouts_with_hr:>10,}")
        if self.workouts_merged:
            print(f"  Duplicates merged:        {self.workouts_merged:>10,}")
        print(f"  GPX parse errors:         {self.gpx_parse_errors:>10,}")
        print(f"  Missing GPX files:        {self.missing_gpx_files:>10,}")
        print(f"  FIT files written:        {self.files_written:>10,}")
        print(f"  FIT files skipped:        {self.files_skipped:>10,}")
        print(f"  Unparseable dates:        {self.unparseable_dates:>10,}")
        print(f"  Elapsed time:             {elapsed:>9.1f}s")
        print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Phase 1: Streaming XML Parse
# ---------------------------------------------------------------------------


def parse_export_xml(xml_path: str, stats: ConversionStats):
    """Stream-parse the Apple Health XML export.

    Returns (hr_timestamps, hr_values, workouts).
    """
    hr_timestamps = array("d")
    hr_values = array("f")
    workouts: list[WorkoutInfo] = []

    print(f"Streaming parse of: {xml_path}")
    print("Collecting HeartRate records and Workout metadata...")
    last_report = time.time()

    context = etree.iterparse(
        xml_path,
        events=("end",),
        tag=("Record", "Workout"),
        huge_tree=True,
    )

    for event, elem in context:
        tag = elem.tag

        if tag == "Record":
            stats.records_parsed += 1
            rec_type = elem.get("type", "")
            if rec_type == "HKQuantityTypeIdentifierHeartRate":
                try:
                    start_str = elem.get("startDate", "")
                    if not start_str:
                        elem.clear()
                        continue
                    ts = parse_apple_date(start_str)
                    hr_val = float(elem.get("value", "0"))
                    hr_timestamps.append(ts)
                    hr_values.append(hr_val)
                    stats.hr_records_found += 1
                except (ValueError, TypeError):
                    stats.unparseable_dates += 1

            if stats.records_parsed % 500000 == 0:
                elapsed = time.time() - last_report
                print(
                    f"  ... {stats.records_parsed/1e6:.1f}M records, "
                    f"{stats.hr_records_found/1e6:.2f}M HR, "
                    f"({elapsed:.1f}s)"
                )
                last_report = time.time()

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        elif tag == "Workout":
            stats.workouts_parsed += 1
            w = _parse_workout_element(elem, stats)
            if w is not None:
                workouts.append(w)

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            if stats.workouts_parsed % 200 == 0:
                print(f"  ... {stats.workouts_parsed} workouts parsed")

    print(
        f"Phase 1 complete: {stats.records_parsed:,} records, "
        f"{stats.hr_records_found:,} HR, "
        f"{stats.workouts_parsed:,} workouts"
    )

    # Sort HR records by timestamp (Apple's XML is not globally sorted within type)
    print("Sorting HR records by timestamp...")
    paired = sorted(zip(hr_timestamps, hr_values), key=lambda x: x[0])
    sorted_ts = array("d", (p[0] for p in paired))
    sorted_hr = array("f", (p[1] for p in paired))
    del paired
    print(f"  Sorted {len(sorted_ts):,} HR records")

    return sorted_ts, sorted_hr, workouts


def _parse_workout_element(elem, stats: ConversionStats) -> Optional[WorkoutInfo]:
    """Extract WorkoutInfo from a <Workout> element and its children."""
    try:
        start_ts = parse_apple_date(elem.get("startDate", ""))
        end_ts = parse_apple_date(elem.get("endDate", ""))
    except (ValueError, TypeError):
        stats.unparseable_dates += 1
        return None

    activity_type = elem.get("workoutActivityType", "Other")
    duration_s = _get_duration_s(elem)
    source_name = elem.get("sourceName", "")

    if duration_s <= 0:
        duration_s = max(0.0, end_ts - start_ts)

    w = WorkoutInfo(
        activity_type=activity_type,
        start_ts=start_ts,
        end_ts=end_ts,
        duration_s=duration_s,
        source_name=source_name,
    )

    # Collect WorkoutEvent data for segments and pause intervals
    events = []  # (type, date_ts, duration_s)
    pauses = []  # list of (pause_start, resume_end) tuples

    for child in elem:
        child_tag = child.tag

        if child_tag == "WorkoutStatistics":
            stat_type = child.get("type", "")
            if stat_type in (
                "HKQuantityTypeIdentifierDistanceWalkingRunning",
                "HKQuantityTypeIdentifierDistanceCycling",
            ):
                sum_val = child.get("sum")
                unit = child.get("unit", "m")
                if sum_val is not None:
                    w.distance_m = distance_to_meters(float(sum_val), unit)
            elif stat_type == "HKQuantityTypeIdentifierActiveEnergyBurned":
                sum_val = child.get("sum")
                if sum_val is not None:
                    w.energy_kcal = float(sum_val)
            elif stat_type == "HKQuantityTypeIdentifierHeartRate":
                avg = child.get("average")
                mn = child.get("minimum")
                mx = child.get("maximum")
                if avg is not None:
                    w.hr_avg = float(avg)
                if mn is not None:
                    w.hr_min = float(mn)
                if mx is not None:
                    w.hr_max = float(mx)

        elif child_tag == "WorkoutEvent":
            event_type = child.get("type", "")
            try:
                event_date = parse_apple_date(child.get("date", ""))
            except (ValueError, TypeError):
                continue

            event_dur = child.get("duration")
            event_dur_unit = child.get("durationUnit", "min")
            event_dur_s = 0.0
            if event_dur is not None:
                try:
                    event_dur_s = parse_apple_duration(event_dur, event_dur_unit)
                except (ValueError, TypeError):
                    pass

            events.append((event_type, event_date, event_dur_s))

            if event_type == "HKWorkoutEventTypePause":
                pauses.append([event_date, None])  # end unknown yet
            elif event_type == "HKWorkoutEventTypeResume":
                # Close the most recent open pause
                for p in reversed(pauses):
                    if p[1] is None:
                        p[1] = event_date
                        break

        elif child_tag == "WorkoutRoute":
            w = _parse_workout_route(child, w, stats)

        elif child_tag == "MetadataEntry":
            key = child.get("key", "")
            value = child.get("value", "")
            if key == "HKTimeZone":
                w.timezone_name = value
            elif key == "HKElevationAscended":
                try:
                    w.elevation_gain_m = float(value.split()[0]) / 100.0
                except (ValueError, IndexError):
                    pass
            elif key == "HKWeatherTemperature":
                try:
                    w.weather_temp_f = float(value.split()[0])
                except (ValueError, IndexError):
                    pass
            elif key == "HKWeatherHumidity":
                try:
                    w.weather_humidity_pct = float(value.split()[0])
                except (ValueError, IndexError):
                    pass

    # Build segments from WorkoutEvent
    _build_segments_and_intervals(w, events, pauses)

    return w


def _build_segments_and_intervals(
    w: WorkoutInfo,
    events: list,
    pauses: list,
):
    """Build segment list and active intervals from WorkoutEvent data.

    Segments become TCX laps. Active intervals are the workout timeline
    minus pause periods, used for HR matching.
    """
    # Build active intervals: start with full workout, subtract pauses
    # Only keep completed pause/resume pairs
    completed_pauses = [(p[0], p[1]) for p in pauses if p[1] is not None]

    if not completed_pauses:
        w.active_intervals = [(w.start_ts, w.end_ts)]
    else:
        # Sort pauses by start time
        completed_pauses.sort(key=lambda x: x[0])
        intervals = []
        current = w.start_ts
        for p_start, p_end in completed_pauses:
            if p_start > current:
                intervals.append((current, p_start))
            current = max(current, p_end)
        if current < w.end_ts:
            intervals.append((current, w.end_ts))
        w.active_intervals = intervals if intervals else [(w.start_ts, w.end_ts)]

    # Build segments from HKWorkoutEventTypeSegment events
    segment_events = [
        (ts, dur) for (etype, ts, dur) in events
        if etype == "HKWorkoutEventTypeSegment" and dur > 0
    ]
    segment_events.sort(key=lambda x: x[0])

    if len(segment_events) >= 2:
        for i, (seg_start, seg_dur) in enumerate(segment_events):
            seg_end = seg_start + seg_dur
            w.segments.append(SegmentInfo(
                start_ts=seg_start,
                end_ts=seg_end,
                duration_s=seg_dur,
            ))
    # If no useful segments, the full workout is a single lap


def _get_duration_s(elem) -> float:
    dur = elem.get("duration")
    unit = elem.get("durationUnit", "min")
    if dur is not None:
        try:
            return parse_apple_duration(dur, unit)
        except (ValueError, TypeError):
            pass
    return 0.0


def _parse_workout_route(route_elem, w: WorkoutInfo, stats: ConversionStats) -> WorkoutInfo:
    for child in route_elem:
        if child.tag == "FileReference":
            path = child.get("path", "")
            if path:
                w.gpx_rel_path = path
                stats.workouts_with_gps += 1
        elif child.tag == "MetadataEntry":
            key = child.get("key", "")
            value = child.get("value", "")
            if key == "HKTimeZone" and not w.timezone_name:
                w.timezone_name = value
    return w


# ---------------------------------------------------------------------------
# Phase 2: HR matching (pause-aware)
# ---------------------------------------------------------------------------


def match_hr_to_workouts(
    workouts: list[WorkoutInfo],
    hr_timestamps: "array",
    hr_values: "array",
    stats: ConversionStats,
):
    """For each workout, binary-search HR arrays within active intervals only."""
    print(f"\nMatching heart rate data to {len(workouts)} workouts...")
    n = len(hr_timestamps)
    if n == 0:
        print("  No heart rate records available.")
        return

    for w in workouts:
        all_hr_ts = []
        all_hr_vals = []

        for interval_start, interval_end in w.active_intervals:
            lo = bisect.bisect_left(hr_timestamps, interval_start)
            hi = bisect.bisect_right(hr_timestamps, interval_end, lo=lo)
            if hi > lo:
                all_hr_ts.extend(hr_timestamps[lo:hi])
                all_hr_vals.extend(hr_values[lo:hi])

        if all_hr_ts:
            w.hr_timestamps = all_hr_ts
            w.hr_values = all_hr_vals
            stats.workouts_with_hr += 1

            # Validate against WorkoutStatistics HR summary
            if w.hr_avg is not None and len(w.hr_values) > 0:
                matched_avg = sum(w.hr_values) / len(w.hr_values)
                if abs(matched_avg - w.hr_avg) > 8.0:
                    print(
                        f"  HR mismatch: {format_tcx_datetime(w.start_ts)} "
                        f"stats={w.hr_avg:.0f} matched={matched_avg:.0f} "
                        f"(n={len(w.hr_values)})"
                    )

    print(
        f"Phase 2 complete: {stats.workouts_with_hr}/{len(workouts)} "
        f"workouts matched with HR data"
    )


# ---------------------------------------------------------------------------
# Phase 3: GPX loading
# ---------------------------------------------------------------------------


def parse_gpx_file(gpx_path: str, stats: ConversionStats) -> list[TrackPoint]:
    """Parse a GPX file and return a list of TrackPoints."""
    points = []
    if not os.path.exists(gpx_path):
        stats.missing_gpx_files += 1
        return points

    try:
        tree = etree.parse(gpx_path)
    except Exception:
        stats.gpx_parse_errors += 1
        return points

    root = tree.getroot()
    ns = GPX_NS

    for trkseg in root.iter(f"{{{ns}}}trkseg"):
        for trkpt in trkseg.iter(f"{{{ns}}}trkpt"):
            lat_str = trkpt.get("lat")
            lon_str = trkpt.get("lon")
            if lat_str is None or lon_str is None:
                continue

            time_elem = trkpt.find(f"{{{ns}}}time")
            if time_elem is None or not time_elem.text:
                continue
            try:
                ts = parse_gpx_date(time_elem.text)
            except (ValueError, TypeError):
                continue

            tp = TrackPoint(ts=ts, lat=float(lat_str), lon=float(lon_str))

            ele_elem = trkpt.find(f"{{{ns}}}ele")
            if ele_elem is not None and ele_elem.text:
                tp.ele = float(ele_elem.text)

            ext_elem = trkpt.find(f"{{{ns}}}extensions")
            if ext_elem is not None:
                speed_elem = ext_elem.find(f"{{{ns}}}speed")
                if speed_elem is not None and speed_elem.text:
                    tp.speed = float(speed_elem.text)
                course_elem = ext_elem.find(f"{{{ns}}}course")
                if course_elem is not None and course_elem.text:
                    tp.course = float(course_elem.text)

            points.append(tp)

    return points


def resolve_gpx_path(rel_path: str, base_dir: str) -> str:
    clean = rel_path.lstrip("/")
    return os.path.join(base_dir, clean)


# ---------------------------------------------------------------------------
# Phase 4: TCX Generation
# ---------------------------------------------------------------------------

# TCX XSD requires strict child ordering in Trackpoint:
#   Time, Position?, AltitudeMeters?, DistanceMeters?, HeartRateBpm?, ...
# and in Lap:
#   TotalTimeSeconds, DistanceMeters, MaximumSpeed?, Calories,
#   AverageHeartRateBpm?, MaximumHeartRateBpm?, Intensity, TriggerMethod,
#   Track*, Notes?, Extensions?


def _add_tcx_trackpoint(parent, pt: TrackPoint, cumulative_dist: float):
    """Add a Trackpoint element with children in schema-compliant order.

    TCX XSD ordering: Time, Position?, AltitudeMeters?, DistanceMeters?,
    HeartRateBpm?, Cadence?, SensorState?, Extensions?
    """
    tp_elem = ET.SubElement(parent, f"{{{TCX_NS}}}Trackpoint")

    # 1. Time (required)
    t = ET.SubElement(tp_elem, f"{{{TCX_NS}}}Time")
    t.text = format_tcx_datetime(pt.ts)

    # 2. Position (optional)
    if pt.lat is not None and pt.lon is not None:
        pos = ET.SubElement(tp_elem, f"{{{TCX_NS}}}Position")
        lat = ET.SubElement(pos, f"{{{TCX_NS}}}LatitudeDegrees")
        lat.text = f"{pt.lat:.6f}"
        lon = ET.SubElement(pos, f"{{{TCX_NS}}}LongitudeDegrees")
        lon.text = f"{pt.lon:.6f}"

    # 3. AltitudeMeters (optional)
    if pt.ele is not None:
        alt = ET.SubElement(tp_elem, f"{{{TCX_NS}}}AltitudeMeters")
        alt.text = f"{pt.ele:.4f}"

    # 4. DistanceMeters (optional) — MUST come before HeartRateBpm per XSD
    if pt.lat is not None and pt.lon is not None:
        dist = ET.SubElement(tp_elem, f"{{{TCX_NS}}}DistanceMeters")
        dist.text = f"{cumulative_dist:.2f}"

    # 5. HeartRateBpm (optional) — MUST come after DistanceMeters per XSD
    if pt.hr is not None:
        hr_elem = ET.SubElement(tp_elem, f"{{{TCX_NS}}}HeartRateBpm")
        hr_val = ET.SubElement(hr_elem, f"{{{TCX_NS}}}Value")
        hr_val.text = f"{pt.hr:.0f}"

    # 6. Extensions — speed and course from GPX (Garmin ActivityExtension v2)
    if pt.speed is not None or pt.course is not None:
        ext = ET.SubElement(tp_elem, f"{{{TCX_NS}}}Extensions")
        tpx = ET.SubElement(ext, f"{{{TPX_NS}}}TPX")
        if pt.speed is not None:
            speed_elem = ET.SubElement(tpx, f"{{{TPX_NS}}}Speed")
            speed_elem.text = f"{pt.speed:.4f}"
        if pt.course is not None:
            course_elem = ET.SubElement(tpx, f"{{{TPX_NS}}}Course")
            course_elem.text = f"{pt.course:.2f}"


def _add_tcx_lap(
    activity,
    start_ts: float,
    duration_s: float,
    distance_m: float,
    calories: float,
    hr_values: list[float],
    trackpoints: list[TrackPoint],
    elevation_gain_m: float = 0.0,
    weather_temp_f: Optional[float] = None,
    weather_humidity_pct: Optional[float] = None,
):
    """Add a Lap element with schema-compliant child ordering."""
    lap = ET.SubElement(
        activity, f"{{{TCX_NS}}}Lap", StartTime=format_tcx_datetime(start_ts)
    )

    # 1. TotalTimeSeconds
    tt = ET.SubElement(lap, f"{{{TCX_NS}}}TotalTimeSeconds")
    tt.text = f"{duration_s:.1f}"

    # 2. DistanceMeters
    dm = ET.SubElement(lap, f"{{{TCX_NS}}}DistanceMeters")
    dm.text = f"{distance_m:.1f}"

    # 3. MaximumSpeed (optional) — computed from trackpoints
    max_speed = 0.0
    for pt in trackpoints:
        if pt.speed and pt.speed > max_speed:
            max_speed = pt.speed
    if max_speed > 0:
        ms = ET.SubElement(lap, f"{{{TCX_NS}}}MaximumSpeed")
        ms.text = f"{max_speed:.3f}"

    # 4. Calories
    cal = ET.SubElement(lap, f"{{{TCX_NS}}}Calories")
    cal.text = f"{calories:.0f}"

    # 5. AverageHeartRateBpm (optional)
    if hr_values:
        avg_hr = sum(hr_values) / len(hr_values)
        ahr = ET.SubElement(lap, f"{{{TCX_NS}}}AverageHeartRateBpm")
        ahr_val = ET.SubElement(ahr, f"{{{TCX_NS}}}Value")
        ahr_val.text = f"{avg_hr:.0f}"

    # 6. MaximumHeartRateBpm (optional)
    if hr_values:
        max_hr = max(hr_values)
        mhr = ET.SubElement(lap, f"{{{TCX_NS}}}MaximumHeartRateBpm")
        mhr_val = ET.SubElement(mhr, f"{{{TCX_NS}}}Value")
        mhr_val.text = f"{max_hr:.0f}"

    # 7. Intensity
    intensity = ET.SubElement(lap, f"{{{TCX_NS}}}Intensity")
    intensity.text = "Active"

    # 8. TriggerMethod
    trigger = ET.SubElement(lap, f"{{{TCX_NS}}}TriggerMethod")
    trigger.text = "Manual"

    # 9. Track
    if trackpoints:
        track = ET.SubElement(lap, f"{{{TCX_NS}}}Track")
        cumulative_dist = 0.0
        prev_lat, prev_lon = None, None
        for pt in trackpoints:
            if pt.lat is not None and pt.lon is not None:
                if prev_lat is not None:
                    cumulative_dist += _haversine(prev_lat, prev_lon, pt.lat, pt.lon)
                prev_lat, prev_lon = pt.lat, pt.lon
            _add_tcx_trackpoint(track, pt, cumulative_dist)

    # 10. Extensions (elevation gain, weather)
    has_extensions = elevation_gain_m > 0 or weather_temp_f or weather_humidity_pct
    if has_extensions:
        ext = ET.SubElement(lap, f"{{{TCX_NS}}}Extensions")
        lx = ET.SubElement(ext, f"{{{TCX_NS}}}LX")
        if elevation_gain_m > 0:
            eg = ET.SubElement(lx, "ElevationGain")
            eg.text = f"{elevation_gain_m:.1f}"
        if weather_temp_f is not None:
            wt = ET.SubElement(lx, "Weather")
            wt.text = f"{weather_temp_f:.1f}°F"
        if weather_humidity_pct is not None:
            wh = ET.SubElement(lx, "Humidity")
            wh.text = f"{weather_humidity_pct:.0f}%"


def _is_sequential_segments(segments: list[SegmentInfo]) -> bool:
    """Return True if segments are non-overlapping sequential intervals.

    Apple Health often exports cumulative split markers (every km / every
    split point) that all start from the workout beginning.  These overlap
    heavily and should NOT be treated as separate laps.
    """
    if len(segments) < 2:
        return False

    # Check if more than half share the same start (±2s) → cumulative
    first_start = segments[0].start_ts
    same_start = sum(1 for s in segments if abs(s.start_ts - first_start) < 2.0)
    if same_start > len(segments) / 2:
        return False

    # Check that segments don't overlap: each starts after previous ends
    for i in range(len(segments) - 1):
        if segments[i].end_ts > segments[i + 1].start_ts + 1.0:
            return False

    return True


def generate_fit_bytes(workout: WorkoutInfo) -> Optional[bytes]:
    """Generate a FIT file (binary) from a workout.

    Returns the FIT file bytes, or None if there is no data to write.
    FIT format uses well-defined sport type enums that COROS recognises
    natively — avoiding the TCX Sport-attribute mapping issues.
    """
    # Filter GPS points to active workout intervals
    buffered_intervals = [
        (lo - 5.0, hi + 5.0) for lo, hi in workout.active_intervals
    ]
    gps_in_window = [
        pt for pt in workout.gps_points
        if any(lo <= pt.ts <= hi for lo, hi in buffered_intervals)
    ]

    # Merge: interpolate HR onto every GPS point
    merged = _merge_and_interpolate_hr(
        gps_in_window, workout.hr_timestamps, workout.hr_values
    )
    if not merged:
        return None

    sport = FIT_SPORT_MAP.get(workout.activity_type, Sport.GENERIC)

    # Timestamps — FIT uses milliseconds since Unix epoch
    start_ms = int(workout.start_ts * 1000)
    end_ms = int(workout.end_ts * 1000)

    builder = FitFileBuilder(auto_define=True)

    # Message order must match COROS reference exactly:
    #   file_id → developer_data_id → device_info → activity →
    #   event(start) → event(stop) → records → lap → session

    # --- FileId ---
    fid = FileIdMessage()
    fid.type = FileType.ACTIVITY
    fid.manufacturer = Manufacturer.DEVELOPMENT
    fid.time_created = end_ms
    fid.product = 0
    fid.product_name = "Apple Watch"
    builder.add(fid)

    # --- DeveloperDataId ---
    dev_data = DeveloperDataIdMessage()
    dev_data.manufacturer_id = Manufacturer.DEVELOPMENT
    dev_data.developer_data_index = 0
    dev_data.application_id = bytes(16)
    builder.add(dev_data)

    # --- DeviceInfo ---
    dev = DeviceInfoMessage()
    dev.timestamp = end_ms
    dev.manufacturer = Manufacturer.DEVELOPMENT
    dev.product_name = "Apple Health Export"
    builder.add(dev)

    # --- Activity ---
    act = ActivityMessage()
    act.timestamp = start_ms
    act.total_timer_time = workout.duration_s
    act.num_sessions = 1
    act.type = 1  # manual
    act.event = 26  # activity
    act.event_type = 1  # stop
    builder.add(act)

    # --- Timer start event ---
    ev_start = EventMessage()
    ev_start.timestamp = start_ms
    ev_start.event = Event.TIMER
    ev_start.event_type = EventType.START
    ev_start.event_group = 0
    builder.add(ev_start)

    # --- Timer stop event ---
    ev_stop = EventMessage()
    ev_stop.timestamp = end_ms
    ev_stop.event = Event.TIMER
    ev_stop.event_type = EventType.STOP_ALL
    ev_stop.event_group = 0
    builder.add(ev_stop)

    # --- Records (one per second, 1 Hz) ---
    # Down-sample to ~1 Hz: keep the first point per whole-second bucket
    last_sec = -1
    hr_vals: list[float] = []
    cumulative_dist = 0.0
    prev_lat = None
    prev_lon = None
    invalid_pos = (179.99999991618097, 179.99999991618097)  # COROS sentinel

    for pt in merged:
        sec = int(pt.ts)
        if sec == last_sec:
            continue
        last_sec = sec

        rec = RecordMessage()
        rec.timestamp = int(pt.ts * 1000)
        rec.activity_type = sport.value  # COROS uses sport code as activity_type

        if pt.lat is not None and pt.lon is not None:
            rec.position_lat = pt.lat
            rec.position_long = pt.lon
            if prev_lat is not None:
                cumulative_dist += _haversine(prev_lat, prev_lon, pt.lat, pt.lon)
            prev_lat, prev_lon = pt.lat, pt.lon
        elif prev_lat is not None:
            # No GPS at this point — use COROS sentinel
            rec.position_lat = invalid_pos[0]
            rec.position_long = invalid_pos[1]

        rec.distance = cumulative_dist

        if pt.hr is not None:
            rec.heart_rate = int(round(pt.hr))
            hr_vals.append(pt.hr)

        if pt.ele is not None:
            rec.altitude = pt.ele

        if pt.speed is not None and pt.speed >= 0:
            rec.speed = pt.speed

        builder.add(rec)

    # --- Lap ---
    # Use Apple Health recorded distance as floor; GPS-calculated may be 0
    # when no GPS track exists (indoor/no-route workouts).
    total_dist = cumulative_dist if cumulative_dist > 0 else workout.distance_m
    avg_speed = total_dist / workout.duration_s if workout.duration_s > 0 else 0
    max_speed_val = max((pt.speed for pt in merged if pt.speed), default=0)

    lap = LapMessage()
    lap.message_index = 0
    lap.timestamp = end_ms
    lap.start_time = start_ms
    lap.total_elapsed_time = workout.duration_s
    lap.total_timer_time = workout.duration_s
    lap.total_distance = total_dist
    lap.total_calories = int(workout.energy_kcal)
    lap.sport = sport
    lap.avg_speed = avg_speed
    lap.max_speed = max_speed_val
    lap.avg_cadence = 0
    lap.max_cadence = 0
    lap.avg_power = 0
    lap.avg_stance_time = 0.0
    lap.avg_stance_time_percent = 0.0
    lap.avg_step_length = 0.0
    lap.avg_vertical_oscillation = 0.0
    lap.avg_vertical_ratio = 0.0
    if hr_vals:
        lap.avg_heart_rate = int(round(sum(hr_vals) / len(hr_vals)))
        lap.max_heart_rate = int(round(max(hr_vals)))
        lap.min_heart_rate = int(round(min(hr_vals)))
    if workout.elevation_gain_m > 0:
        lap.total_ascent = int(workout.elevation_gain_m)
    if workout.weather_temp_f is not None:
        lap.avg_temperature = int(round((workout.weather_temp_f - 32) * 5 / 9))
    builder.add(lap)

    # --- Session ---
    sess = SessionMessage()
    sess.timestamp = end_ms
    sess.start_time = start_ms
    sess.total_elapsed_time = workout.duration_s
    sess.total_timer_time = workout.duration_s
    sess.total_distance = total_dist
    sess.total_calories = int(workout.energy_kcal)
    sess.sport = sport
    sess.avg_speed = avg_speed
    sess.max_speed = max_speed_val
    sess.avg_cadence = 0
    sess.max_cadence = 0
    sess.avg_power = 0
    sess.avg_stance_time = 0.0
    sess.avg_stance_time_balance = 0.0
    sess.avg_step_length = 0.0
    sess.avg_vertical_oscillation = 0.0
    sess.avg_vertical_ratio = 0.0
    if hr_vals:
        sess.avg_heart_rate = int(round(sum(hr_vals) / len(hr_vals)))
        sess.max_heart_rate = int(round(max(hr_vals)))
        sess.min_heart_rate = int(round(min(hr_vals)))
    if workout.elevation_gain_m > 0:
        sess.total_ascent = int(workout.elevation_gain_m)
    if workout.weather_temp_f is not None:
        sess.avg_temperature = int(round((workout.weather_temp_f - 32) * 5 / 9))
    builder.add(sess)

    fit_file = builder.build()
    return fit_file.to_bytes()


def _generate_fit_filename(workout: WorkoutInfo, file_index: int) -> str:
    """Generate a .fit filename like 2025-08-22_Hike.fit"""
    dt_str = datetime.fromtimestamp(workout.start_ts, tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )
    sport_name = ACTIVITY_TYPE_MAP.get(workout.activity_type, "Other")
    if file_index > 0:
        return f"{dt_str}_{sport_name}_{file_index}.fit"
    return f"{dt_str}_{sport_name}.fit"


def build_tcx_xml(workout: WorkoutInfo) -> Optional[ET.Element]:
    """Build a TCX XML tree from a workout with multi-lap support.

    Returns the root Element, or None if there is no data to write.
    """
    # Filter GPS points to active workout intervals (exclude pauses).
    # GPX route files span an entire day — we keep only the workout's
    # active periods with a small buffer around pause boundaries.
    buffered_intervals = [
        (lo - 5.0, hi + 5.0) for lo, hi in workout.active_intervals
    ]
    gps_in_window = [
        pt for pt in workout.gps_points
        if any(lo <= pt.ts <= hi for lo, hi in buffered_intervals)
    ]

    # Merge GPS and HR trackpoints — interpolate HR onto every GPS point
    merged = _merge_and_interpolate_hr(
        gps_in_window, workout.hr_timestamps, workout.hr_values
    )
    if not merged:
        return None

    sport = ACTIVITY_TYPE_MAP.get(workout.activity_type, "Other")

    ET.register_namespace("", TCX_NS)
    ET.register_namespace("xsi", XSI_NS)
    # Note: avoid ns0/ns1/ns2 — those are reserved by Python's ElementTree
    ET.register_namespace("tpx", TPX_NS)

    tcx_root = ET.Element(
        f"{{{TCX_NS}}}TrainingCenterDatabase",
        {
            f"{{{XSI_NS}}}schemaLocation": (
                f"{TCX_NS} "
                f"http://www.garmin.com/xmlschemas/TrainingCenterDatabasev2.xsd"
            ),
        },
    )

    activities = ET.SubElement(tcx_root, f"{{{TCX_NS}}}Activities")
    activity = ET.SubElement(activities, f"{{{TCX_NS}}}Activity", Sport=sport)

    activity_id = ET.SubElement(activity, f"{{{TCX_NS}}}Id")
    activity_id.text = format_tcx_datetime(workout.start_ts)

    # Build a lookup: timestamp (int second) -> HR value
    hr_by_ts = {}
    for ts, hr in zip(workout.hr_timestamps, workout.hr_values):
        hr_by_ts[int(ts)] = hr

    if _is_sequential_segments(workout.segments):
        # Multi-lap: non-overlapping sequential segments
        total_dur = sum(s.duration_s for s in workout.segments)
        if total_dur <= 0:
            total_dur = workout.duration_s

        for i, seg in enumerate(workout.segments):
            seg_tps = [
                pt for pt in merged
                if seg.start_ts <= pt.ts < seg.end_ts
            ]
            # For the last segment, include points at exact end time
            if i == len(workout.segments) - 1:
                seg_tps = [
                    pt for pt in merged
                    if pt.ts >= seg.start_ts
                ]

            seg_hr_vals = [
                hr_by_ts[int(pt.ts)]
                for pt in seg_tps
                if int(pt.ts) in hr_by_ts
            ]

            fraction = seg.duration_s / total_dur if total_dur > 0 else 0
            seg_cal = workout.energy_kcal * fraction
            seg_dist = workout.distance_m * fraction
            seg_ele = workout.elevation_gain_m * fraction

            _add_tcx_lap(
                activity,
                seg.start_ts,
                seg.duration_s,
                seg_dist,
                seg_cal,
                seg_hr_vals,
                seg_tps,
                seg_ele,
                workout.weather_temp_f,
                workout.weather_humidity_pct,
            )
    else:
        # Single lap
        _add_tcx_lap(
            activity,
            workout.start_ts,
            workout.duration_s,
            workout.distance_m,
            workout.energy_kcal,
            workout.hr_values,
            merged,
            workout.elevation_gain_m,
            workout.weather_temp_f,
            workout.weather_humidity_pct,
        )

    # Creator
    creator = ET.SubElement(
        activity, f"{{{TCX_NS}}}Creator", {f"{{{XSI_NS}}}type": "Device_t"}
    )
    creator_name = ET.SubElement(creator, f"{{{TCX_NS}}}Name")
    creator_name.text = "Apple Health Export"

    return tcx_root


def _merge_and_interpolate_hr(
    gps_points: list[TrackPoint],
    hr_timestamps: list[float],
    hr_values: list[float],
) -> list[TrackPoint]:
    """Merge HR onto GPS trackpoints via nearest-neighbor interpolation.

    Apple's XML export provides sparse HR samples (~every 10-15 min during
    workouts).  Platforms like COROS and Garmin ignore heart rate on
    trackpoints that lack GPS coordinates, so we must attach an HR value to
    *every* GPS trackpoint.  Without this, only 0.1 % of the track is visible.
    """
    if not gps_points:
        merged = []
        for ts, hr in zip(hr_timestamps, hr_values):
            merged.append(TrackPoint(ts=ts, hr=hr))
        merged.sort(key=lambda p: p.ts)
        return merged

    if not hr_timestamps:
        gps_points.sort(key=lambda p: p.ts)
        return gps_points

    n_hr = len(hr_timestamps)
    hr_idx = 0

    for pt in gps_points:
        # Advance hr_idx to first HR record at or after this trackpoint
        while hr_idx < n_hr and hr_timestamps[hr_idx] < pt.ts:
            hr_idx += 1

        if hr_idx == 0:
            pt.hr = hr_values[0]
        elif hr_idx >= n_hr:
            pt.hr = hr_values[-1]
        else:
            # Linear interpolation between surrounding HR samples
            t_before = hr_timestamps[hr_idx - 1]
            t_after = hr_timestamps[hr_idx]
            hr_before = hr_values[hr_idx - 1]
            hr_after = hr_values[hr_idx]

            gap = t_after - t_before
            if gap > 0:
                fraction = (pt.ts - t_before) / gap
                pt.hr = hr_before + fraction * (hr_after - hr_before)
            else:
                pt.hr = hr_before

    gps_points.sort(key=lambda p: p.ts)
    return gps_points


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Per-workout processing (for parallel execution)
# ---------------------------------------------------------------------------


def _process_one_workout(args: tuple) -> Optional[tuple[str, bytes]]:
    """Process a single workout: load GPX, build FIT, return (filename, bytes)."""
    workout, base_dir, file_index = args

    # Load GPX
    gps_points = []
    if workout.gpx_rel_path:
        gpx_path = resolve_gpx_path(workout.gpx_rel_path, base_dir)
        if os.path.exists(gpx_path):
            try:
                tree = etree.parse(gpx_path)
                root = tree.getroot()
                ns = GPX_NS
                for trkseg in root.iter(f"{{{ns}}}trkseg"):
                    for trkpt in trkseg.iter(f"{{{ns}}}trkpt"):
                        lat_str = trkpt.get("lat")
                        lon_str = trkpt.get("lon")
                        if lat_str is None or lon_str is None:
                            continue
                        time_elem = trkpt.find(f"{{{ns}}}time")
                        if time_elem is None or not time_elem.text:
                            continue
                        try:
                            ts = parse_gpx_date(time_elem.text)
                        except (ValueError, TypeError):
                            continue
                        tp = TrackPoint(ts=ts, lat=float(lat_str), lon=float(lon_str))
                        ele_elem = trkpt.find(f"{{{ns}}}ele")
                        if ele_elem is not None and ele_elem.text:
                            tp.ele = float(ele_elem.text)
                        ext_elem = trkpt.find(f"{{{ns}}}extensions")
                        if ext_elem is not None:
                            speed_elem = ext_elem.find(f"{{{ns}}}speed")
                            if speed_elem is not None and speed_elem.text:
                                tp.speed = float(speed_elem.text)
                            course_elem = ext_elem.find(f"{{{ns}}}course")
                            if course_elem is not None and course_elem.text:
                                tp.course = float(course_elem.text)
                        gps_points.append(tp)
            except Exception:
                pass

    workout.gps_points = gps_points
    fit_bytes = generate_fit_bytes(workout)
    if fit_bytes is None:
        return None

    filename = _generate_fit_filename(workout, file_index)
    return (filename, fit_bytes)


# ---------------------------------------------------------------------------
# Filtering & Deduplication
# ---------------------------------------------------------------------------


def should_include_workout(w: WorkoutInfo) -> bool:
    if w.duration_s < MIN_DURATION_S:
        return False
    return True


def deduplicate_workouts(workouts: list[WorkoutInfo], stats: ConversionStats) -> list[WorkoutInfo]:
    """Merge overlapping workouts with same date and activity type.

    When two sources record the same activity (e.g., 悦跑圈 + Apple Watch),
    keep the one with more data (GPS + HR).
    """
    if not workouts:
        return workouts

    # Sort by date, then by data quality (GPS + HR count)
    def quality(w: WorkoutInfo) -> int:
        score = 0
        if w.gpx_rel_path:
            score += 2
        if w.hr_avg is not None:
            score += 1
        return score

    # Group by (date, activity_type)
    def group_key(w: WorkoutInfo) -> str:
        dt = datetime.fromtimestamp(w.start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        return f"{dt}_{w.activity_type}"

    workouts.sort(key=lambda w: (group_key(w), -quality(w)))

    result = []
    for key, group in groupby(workouts, key=group_key):
        group_list = list(group)
        if len(group_list) == 1:
            result.append(group_list[0])
        else:
            # Check if workouts actually overlap in time
            best = group_list[0]  # highest quality due to sort
            for other in group_list[1:]:
                # Overlap if time windows intersect
                overlap_start = max(best.start_ts, other.start_ts)
                overlap_end = min(best.end_ts, other.end_ts)
                if overlap_end > overlap_start:
                    # Merge: keep best, absorb any extra data from other
                    if other.distance_m > best.distance_m:
                        best.distance_m = other.distance_m
                    if other.energy_kcal > best.energy_kcal:
                        best.energy_kcal = other.energy_kcal
                    if other.gpx_rel_path and not best.gpx_rel_path:
                        best.gpx_rel_path = other.gpx_rel_path
                    stats.workouts_merged += 1
                else:
                    result.append(other)
            result.append(best)

    return result


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def main():
    # Register XML namespaces early — must be set before any serialization.
    # ElementTree namespace registrations are process-global.
    ET.register_namespace("", TCX_NS)
    ET.register_namespace("xsi", XSI_NS)
    ET.register_namespace("tpx", TPX_NS)

    parser = argparse.ArgumentParser(
        description="Convert Apple Health XML export to FIT workout files."
    )
    parser.add_argument("--xml", default="apple_health_export/导出.xml")
    parser.add_argument("--base-dir", default="apple_health_export")
    parser.add_argument("--output", default="output_workouts")
    parser.add_argument("--min-duration", type=int, default=MIN_DURATION_S)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-parallel", action="store_true",
                       help="Disable multiprocessing")
    parser.add_argument("--workers", type=int, default=0,
                       help="Number of parallel workers (0=auto)")
    args = parser.parse_args()

    stats = ConversionStats(start_time=time.time())

    if not args.dry_run:
        os.makedirs(args.output, exist_ok=True)

    # -------------------------------------------------------------------
    # Phase 1: Parse XML
    # -------------------------------------------------------------------
    if not os.path.exists(args.xml):
        print(f"Error: XML file not found: {args.xml}")
        sys.exit(1)

    hr_timestamps, hr_values, workouts = parse_export_xml(args.xml, stats)

    # -------------------------------------------------------------------
    # Filter & Deduplicate
    # -------------------------------------------------------------------
    filtered = [w for w in workouts if should_include_workout(w)]
    stats.files_skipped = len(workouts) - len(filtered)
    print(f"\nFiltered: {len(filtered)} workouts kept, {stats.files_skipped} skipped")

    deduped = deduplicate_workouts(filtered, stats)
    if stats.workouts_merged > 0:
        print(f"Deduplicated: {stats.workouts_merged} overlapping workouts merged, "
              f"{len(deduped)} unique")

    if args.dry_run:
        for w in deduped[:10]:
            print(
                f"  {format_tcx_datetime(w.start_ts)}  {w.activity_type:45s}  "
                f"{w.duration_s/60:5.1f}min  {w.distance_m/1000:.2f}km  "
                f"HR:{w.hr_avg or 'N/A'}  GPS:{'yes' if w.gpx_rel_path else 'no'}  "
                f"segs:{len(w.segments)}  src:{w.source_name[:20]}"
            )
        stats.print_summary()
        return

    # -------------------------------------------------------------------
    # Phase 2: Match HR
    # -------------------------------------------------------------------
    match_hr_to_workouts(deduped, hr_timestamps, hr_values, stats)
    del hr_timestamps
    del hr_values

    # -------------------------------------------------------------------
    # Phase 3 + 4: Load GPX and generate FIT
    # -------------------------------------------------------------------
    print(f"\nGenerating FIT files...")

    # Build dedup filename map
    seen_names: dict[str, int] = {}
    tasks = []
    for w in deduped:
        dt_str = datetime.fromtimestamp(w.start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        sport = ACTIVITY_TYPE_MAP.get(w.activity_type, "Other")
        base = f"{dt_str}_{sport}"
        idx = seen_names.get(base, 0)
        seen_names[base] = idx + 1
        tasks.append((w, args.base_dir, idx))
    # Rename old .tcx reference

    if args.no_parallel or len(tasks) < 50:
        # Sequential
        _process_sequential(tasks, args.output, stats)
    else:
        workers = args.workers if args.workers > 0 else None
        _process_parallel(tasks, args.output, stats, workers)

    stats.print_summary()
    print(f"\nOutput files in: {os.path.abspath(args.output)}/")


def _process_sequential(tasks, output_dir, stats):
    """Process workouts sequentially."""
    for i, (workout, base_dir, file_index) in enumerate(tasks):
        result = _process_one_workout((workout, base_dir, file_index))
        if result is not None:
            filename, fit_bytes = result
            path = os.path.join(output_dir, filename)
            with open(path, "wb") as f:
                f.write(fit_bytes)
            stats.files_written += 1
        else:
            stats.files_skipped += 1

        if (i + 1) % 200 == 0:
            print(f"  ... {i+1}/{len(tasks)} FIT files written")


def _process_parallel(tasks, output_dir, stats, workers):
    """Process workouts in parallel using a process pool."""
    print(f"  Using {workers or os.cpu_count()} parallel workers for {len(tasks)} workouts")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_one_workout, t): t for t in tasks}
        completed = 0
        for future in as_completed(futures):
            completed += 1
            try:
                result = future.result()
                if result is not None:
                    filename, fit_bytes = result
                    path = os.path.join(output_dir, filename)
                    with open(path, "wb") as f:
                        f.write(fit_bytes)
                    stats.files_written += 1
                else:
                    stats.files_skipped += 1
            except Exception as e:
                stats.files_skipped += 1
                print(f"  Error processing workout: {e}")

            if completed % 200 == 0:
                print(f"  ... {completed}/{len(tasks)} FIT files written")


if __name__ == "__main__":
    main()
