from __future__ import annotations

import argparse
import json

from bookmaker_detector_api.services.initial_dataset_load import (
    parse_csv_values,
    run_initial_production_dataset_load,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the initial production historical dataset load."
    )
    parser.add_argument(
        "--source-url-template",
        required=True,
        help=(
            "Provider URL template. Supported placeholders include "
            "{team_code}, {team_code_lower}, {team_name}, {team_slug}, "
            "{season_label}, {season_start_year}, {season_end_year}, "
            "{season_start_date}, and {season_end_date}."
        ),
    )
    parser.add_argument(
        "--team-codes",
        default=None,
        help="Optional comma-separated team scope. Defaults to all teams in the reference table.",
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
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_initial_production_dataset_load(
        source_url_template=args.source_url_template,
        team_codes=parse_csv_values(args.team_codes),
        season_labels=parse_csv_values(args.season_labels),
        requested_by=args.requested_by,
        run_label=args.run_label,
        continue_on_error=not args.stop_on_error,
        persist_payload=not args.skip_payload_persistence,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
