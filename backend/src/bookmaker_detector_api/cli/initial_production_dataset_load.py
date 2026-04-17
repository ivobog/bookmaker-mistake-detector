from __future__ import annotations

import argparse
import json

from bookmaker_detector_api.services.initial_dataset_load import (
    DEFAULT_COVERS_NBA_TEAMS_INDEX_URL,
    parse_csv_values,
    run_initial_production_dataset_load,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the initial production historical dataset load."
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_COVERS_NBA_TEAMS_INDEX_URL,
        help="Covers NBA teams index URL used for dynamic team page discovery.",
    )
    parser.add_argument(
        "--team-codes",
        default=None,
        help=(
            "Optional comma-separated team scope for partial runs. Defaults to all teams "
            "discovered from the Covers index and matched to reference data."
        ),
    )
    parser.add_argument(
        "--season-labels",
        default=None,
        help=(
            "Optional comma-separated season scope. Defaults to the last four completed "
            "seasons in the reference table."
        ),
    )
    parser.add_argument(
        "--requested-by",
        default="initial-production-dataset-load",
        help="Requester tag recorded on created job runs.",
    )
    parser.add_argument(
        "--run-label",
        default="initial-production-dataset-load",
        help="Run label recorded on created job runs.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop immediately if one target fails.",
    )
    parser.add_argument(
        "--skip-payload-persistence",
        action="store_true",
        help="Do not persist raw payload snapshots during the load.",
    )
    parser.add_argument(
        "--browser-fallback",
        action="store_true",
        help="Request browser fallback when static HTML cannot provide the season block.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_initial_production_dataset_load(
        base_url=args.base_url,
        team_codes=parse_csv_values(args.team_codes),
        season_labels=parse_csv_values(args.season_labels),
        requested_by=args.requested_by,
        run_label=args.run_label,
        continue_on_error=not args.stop_on_error,
        persist_payload=not args.skip_payload_persistence,
        browser_fallback=args.browser_fallback,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
