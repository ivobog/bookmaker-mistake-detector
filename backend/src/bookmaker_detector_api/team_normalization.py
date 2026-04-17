from __future__ import annotations

import re

NBA_TEAM_ALIASES = {
    "ATL": ("ATL", "Atlanta", "Atlanta Hawks", "Hawks"),
    "BOS": ("BOS", "Boston", "Boston Celtics", "Celtics"),
    "BKN": ("BKN", "BK", "Brooklyn", "Brooklyn Nets", "Nets"),
    "CHA": ("CHA", "Charlotte", "Charlotte Hornets", "Hornets"),
    "CHI": ("CHI", "Chicago", "Chicago Bulls", "Bulls"),
    "CLE": ("CLE", "Cleveland", "Cleveland Cavaliers", "Cavaliers"),
    "DAL": ("DAL", "Dallas", "Dallas Mavericks", "Mavericks"),
    "DEN": ("DEN", "Denver", "Denver Nuggets", "Nuggets"),
    "DET": ("DET", "Detroit", "Detroit Pistons", "Pistons"),
    "GSW": ("GSW", "GS", "Golden State", "Golden State Warriors", "Warriors"),
    "HOU": ("HOU", "Houston", "Houston Rockets", "Rockets"),
    "IND": ("IND", "Indiana", "Indiana Pacers", "Pacers"),
    "LAC": ("LAC", "LA Clippers", "Los Angeles Clippers", "Clippers"),
    "LAL": ("LAL", "LA Lakers", "Los Angeles Lakers", "Lakers"),
    "MEM": ("MEM", "Memphis", "Memphis Grizzlies", "Grizzlies"),
    "MIA": ("MIA", "Miami", "Miami Heat", "Heat"),
    "MIL": ("MIL", "Milwaukee", "Milwaukee Bucks", "Bucks"),
    "MIN": ("MIN", "Minnesota", "Minnesota Timberwolves", "Timberwolves"),
    "NOP": ("NOP", "NO", "New Orleans", "New Orleans Pelicans", "Pelicans"),
    "NYK": ("NYK", "New York", "New York Knicks", "Knicks"),
    "OKC": ("OKC", "Oklahoma City", "Oklahoma City Thunder", "Thunder"),
    "ORL": ("ORL", "Orlando", "Orlando Magic", "Magic"),
    "PHI": ("PHI", "Philadelphia", "Philadelphia 76ers", "76ers", "Sixers"),
    "PHX": ("PHX", "Phoenix", "Phoenix Suns", "Suns"),
    "POR": ("POR", "Portland", "Portland Trail Blazers", "Trail Blazers"),
    "SAC": ("SAC", "Sacramento", "Sacramento Kings", "Kings"),
    "SAS": ("SAS", "SA", "San Antonio", "San Antonio Spurs", "Spurs"),
    "TOR": ("TOR", "Toronto", "Toronto Raptors", "Raptors"),
    "UTA": ("UTA", "Utah", "Utah Jazz", "Jazz"),
    "WAS": ("WAS", "Washington", "Washington Wizards", "Wizards"),
}

NBA_TEAM_NAME_LOOKUP = {
    re.sub(r"[^A-Z0-9]+", " ", alias.upper()).strip(): team_code
    for team_code, aliases in NBA_TEAM_ALIASES.items()
    for alias in aliases
}


def normalize_team_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return re.sub(r"\s{2,}", " ", normalized)


def slugify_team_name(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return re.sub(r"-{2,}", "-", slug)


def normalize_team_code_or_name(value: str | None) -> str | None:
    resolved_code, _ = resolve_team_code_or_name(value)
    return resolved_code


def resolve_team_code_or_name(value: str | None) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    stripped_value = value.strip()
    if not stripped_value:
        return None, None
    normalized_value = re.sub(r"[^A-Z0-9]+", " ", stripped_value.upper()).strip()
    resolved_code = NBA_TEAM_NAME_LOOKUP.get(normalized_value)
    if resolved_code is None:
        return None, None

    normalized_code = re.sub(r"[^A-Z0-9]+", "", stripped_value.upper())
    if normalized_code == resolved_code:
        return resolved_code, "direct_code"
    return resolved_code, "alias_name"


def team_identity_keys(*values: str | None, team_code: str | None = None) -> set[str]:
    keys: set[str] = set()
    if team_code is not None and team_code.strip():
        keys.add(f"code:{team_code.strip().upper()}")
        for alias in NBA_TEAM_ALIASES.get(team_code.strip().upper(), ()):
            keys.update(_identity_keys_for_value(alias))

    for value in values:
        keys.update(_identity_keys_for_value(value))

    return keys


def _identity_keys_for_value(value: str | None) -> set[str]:
    if value is None:
        return set()
    stripped_value = value.strip()
    if not stripped_value:
        return set()

    keys = {
        f"name:{normalize_team_name(stripped_value)}",
        f"slug:{slugify_team_name(stripped_value)}",
    }
    normalized_team_code = normalize_team_code_or_name(stripped_value)
    if normalized_team_code is not None:
        keys.add(f"code:{normalized_team_code}")
    return keys
