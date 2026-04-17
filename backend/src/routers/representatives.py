"""Representative lookup by US zip code via OpenStates + Zippopotam."""

import logging

import httpx
from fastapi import APIRouter, HTTPException, Query

from src.core.settings import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/representatives", tags=["representatives"])


async def _zip_to_coords(zip_code: str) -> dict:
    """Convert a US zip code to lat, lng, state, and place name."""
    async with httpx.AsyncClient() as client:
        res = await client.get(f"https://api.zippopotam.us/us/{zip_code}")
        if res.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Invalid zip code: {zip_code}")
        res.raise_for_status()
    place = res.json()["places"][0]
    return {
        "lat": float(place["latitude"]),
        "lng": float(place["longitude"]),
        "state": place["state abbreviation"],
        "place_name": place["place name"],
    }


async def _get_legislators(lat: float, lng: float, api_key: str) -> list[dict]:
    """Look up legislators by coordinates via OpenStates v3."""
    async with httpx.AsyncClient() as client:
        res = await client.get(
            "https://v3.openstates.org/people.geo",
            params=[
                ("lat", lat),
                ("lng", lng),
                ("apikey", api_key),
                ("include", "offices"),
                ("include", "links"),
            ],
        )
        res.raise_for_status()
    return res.json().get("results", [])


def _parse_person(person: dict) -> dict:
    """Extract a clean representative entry from an OpenStates person."""
    role = person.get("current_role") or {}
    chamber_code = role.get("org_classification", "")

    offices = [
        {
            "type": office.get("classification"),
            "phone": office.get("voice"),
            "address": office.get("address"),
            "email": office.get("email"),
        }
        for office in person.get("offices", [])
    ]

    # OpenStates puts email in a top-level field, not in offices.
    # Some entries are contact page URLs, not real emails — only keep actual emails.
    top_email = person.get("email") or ""
    email = top_email if "@" in top_email else None

    # Best phone: first office phone found
    phone = None
    for office in person.get("offices", []):
        if office.get("voice"):
            phone = office["voice"]
            break

    entry = {
        "name": person.get("name"),
        "party": person.get("party"),
        "title": role.get("title"),
        "district": role.get("district"),
        "image": person.get("image"),
        "email": email,
        "phone": phone,
        "offices": offices,
        "links": person.get("links", []),
    }

    if chamber_code in ("upper", "lower"):
        entry["chamber"] = (
            "State Senate" if chamber_code == "upper" else "State Assembly/House"
        )
    else:
        entry["chamber"] = role.get("title", "Federal")

    return entry


@router.get("/lookup", summary="Look up representatives by US zip code")
async def lookup_representatives(
    zipcode: str = Query(..., min_length=5, max_length=5, pattern=r"^\d{5}$"),
):
    api_key = settings.openstates_api_key
    if not api_key:
        raise HTTPException(status_code=503, detail="OpenStates API key not configured")

    try:
        loc = await _zip_to_coords(zipcode)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Geocoding failed for zip %s", zipcode)
        raise HTTPException(status_code=502, detail="Geocoding service unavailable")

    try:
        people = await _get_legislators(loc["lat"], loc["lng"], api_key)
    except Exception:
        logger.exception("OpenStates lookup failed")
        raise HTTPException(status_code=502, detail="OpenStates API unavailable")

    state_legislators = []
    federal_legislators = []
    for person in people:
        entry = _parse_person(person)
        role = person.get("current_role") or {}
        if role.get("org_classification") in ("upper", "lower"):
            state_legislators.append(entry)
        else:
            federal_legislators.append(entry)

    return {
        "zip": zipcode,
        "location": loc,
        "state_legislators": state_legislators,
        "federal_legislators": federal_legislators,
    }
