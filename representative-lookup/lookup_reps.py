#!/usr/bin/env python3
"""
Look up all state + federal legislators for a US zip code.

Usage:
    python lookup_reps.py 94110
    python lookup_reps.py 95032 --pretty

Requires:
    pip install requests

Environment:
    Set OPENSTATES_API_KEY or pass --api-key
    Get a free key at https://openstates.org/accounts/signup/
"""

import argparse
import json
import os
import sys

import requests


def zip_to_coords(zip_code: str) -> dict:
    """Convert a US zip code to lat, lng, state, and place name."""
    res = requests.get(f"https://api.zippopotam.us/us/{zip_code}")
    res.raise_for_status()
    place = res.json()["places"][0]
    return {
        "lat": float(place["latitude"]),
        "lng": float(place["longitude"]),
        "state": place["state abbreviation"],
        "place_name": place["place name"],
    }


def get_all_reps(zip_code: str, api_key: str) -> dict:
    """
    Given a US zip code, return all state + federal representatives
    using Zippopotam (geocoding) and OpenStates (legislator lookup).
    """
    # Step 1: Geocode
    loc = zip_to_coords(zip_code)
    lat, lng = loc["lat"], loc["lng"]

    # Step 2: OpenStates lookup
    res = requests.get(
        "https://v3.openstates.org/people.geo",
        params={
            "lat": lat,
            "lng": lng,
            "apikey": api_key,
            "include": ["offices", "links"],
        },
    )
    res.raise_for_status()
    people = res.json().get("results", [])

    # Step 3: Parse and split into state vs federal
    state_legislators = []
    federal_legislators = []

    for person in people:
        role = person.get("current_role") or {}
        chamber_code = role.get("org_classification", "")

        offices = []
        for office in person.get("offices", []):
            offices.append({
                "type": office.get("classification"),
                "phone": office.get("voice"),
                "address": office.get("address"),
                "email": office.get("email"),
            })

        entry = {
            "name": person.get("name"),
            "party": person.get("party"),
            "title": role.get("title"),
            "district": role.get("district"),
            "image": person.get("image"),
            "offices": offices,
            "links": person.get("links", []),
        }

        if chamber_code in ("upper", "lower"):
            entry["chamber"] = (
                "State Senate" if chamber_code == "upper" else "State Assembly/House"
            )
            state_legislators.append(entry)
        else:
            entry["chamber"] = role.get("title", "Federal")
            federal_legislators.append(entry)

    return {
        "zip": zip_code,
        "location": loc,
        "state_legislators": state_legislators,
        "federal_legislators": federal_legislators,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Look up state & federal legislators by US zip code."
    )
    parser.add_argument("zipcode", help="5-digit US zip code (e.g. 94110)")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("OPENSTATES_API_KEY"),
        help="OpenStates API key (or set OPENSTATES_API_KEY env var)",
    )
    parser.add_argument(
        "--pretty", action="store_true", help="Pretty-print the JSON output"
    )
    args = parser.parse_args()

    if not args.api_key:
        print(
            "Error: No API key provided.\n"
            "  Set OPENSTATES_API_KEY env var or pass --api-key YOUR_KEY\n"
            "  Get a free key at https://openstates.org/accounts/signup/",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        result = get_all_reps(args.zipcode, args.api_key)
    except requests.HTTPError as e:
        print(f"Error: API request failed — {e}", file=sys.stderr)
        sys.exit(1)

    indent = 2 if args.pretty else None
    print(json.dumps(result, indent=indent))


if __name__ == "__main__":
    main()
