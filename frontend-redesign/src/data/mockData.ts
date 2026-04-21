import type { AppData } from "../types";

export function buildMockAppData(): AppData {
  return {
    mode: "mock",
    sourceLabel: "Prototype dataset",
    generatedAt: "Updated just now",
    headline: "A guided operations desk for training, activating, scoring, and reviewing signals.",
    lead:
      "This redesign reframes the product around operator intent instead of internal artifacts, with one visible workflow strip and a plain-language next action on every screen.",
    defaults: {
      featureKey: "baseline_team_features_v1",
      targetTask: "spread_error_regression",
      trainRatio: 0.7,
      validationRatio: 0.15,
      seasonLabel: "2025-2026",
      sourceName: "demo_daily_lines_v1"
    },
    nextActionLabel: "Open Training Lab",
    nextActionRoute: "training-lab",
    stats: [
      { label: "Active model", value: "Tree Stump v12", tone: "green" },
      { label: "Signals waiting", value: "18", tone: "amber" },
      { label: "Current task", value: "Spread Error", tone: "blue" },
      { label: "Workflow health", value: "4 of 5 ready", tone: "slate" }
    ],
    tasks: [
      {
        key: "spread_error_regression",
        label: "Spread Error",
        description: "Predict spread miss magnitude and convert it into signal strength for side markets.",
        metricName: "MAE",
        defaultPolicy: "validation_regression_candidate_v1",
        semantics: "Spread edge scoring"
      },
      {
        key: "total_error_regression",
        label: "Total Error",
        description: "Track total-market miss patterns and select models for totals-focused review.",
        metricName: "MAE",
        defaultPolicy: "validation_regression_candidate_v1",
        semantics: "Totals edge scoring"
      },
      {
        key: "point_margin_regression",
        label: "Point Margin",
        description: "Estimate game margin directly for stronger scenario previews and validation support.",
        metricName: "RMSE",
        defaultPolicy: "validation_regression_candidate_v1",
        semantics: "Margin prediction"
      }
    ],
    workflow: [
      {
        id: "features",
        label: "Features ready",
        description: "Baseline snapshots are materialized for the working task and current slate scope.",
        status: "ready",
        updatedAt: "11 minutes ago",
        ctaLabel: "Refresh features",
        route: "training-lab"
      },
      {
        id: "training",
        label: "Models trained",
        description: "Standard production run finished with two usable candidates and one fallback result.",
        status: "ready",
        updatedAt: "43 minutes ago",
        ctaLabel: "Train new run",
        route: "training-lab"
      },
      {
        id: "activation",
        label: "Model activated",
        description: "Tree Stump v12 is live, but a fresher candidate is available for review.",
        status: "stale",
        updatedAt: "Yesterday",
        ctaLabel: "Activate model",
        route: "model-decision"
      },
      {
        id: "slate",
        label: "Slate scored",
        description: "Today's slate has a preview batch but no saved scoring run yet.",
        status: "review",
        updatedAt: "8 minutes ago",
        ctaLabel: "Run today's slate",
        route: "slate-runner"
      },
      {
        id: "signals",
        label: "Signals reviewed",
        description: "New analyst-facing signals are waiting for evidence review and disposition.",
        status: "review",
        updatedAt: "5 minutes ago",
        ctaLabel: "Open Signals Desk",
        route: "signals-desk"
      }
    ],
    home: {
      focusCards: [
        {
          eyebrow: "Next recommended step",
          title: "Review the fresh candidate before today's slate lock.",
          description: "The workflow is healthy enough to move from training into a release decision.",
          ctaLabel: "Go to Model Decision",
          route: "model-decision",
          tone: "blue"
        },
        {
          eyebrow: "Operator status",
          title: "Signals are waiting for analyst review.",
          description: "The last slate preview produced 18 candidate signals, 6 of which are evidence-heavy.",
          ctaLabel: "Open Signals Desk",
          route: "signals-desk",
          tone: "amber"
        },
        {
          eyebrow: "Training posture",
          title: "Presets are ready for a deep validation rerun.",
          description: "Expert settings stay collapsed until you choose to inspect them.",
          ctaLabel: "Open Training Lab",
          route: "training-lab",
          tone: "green"
        }
      ],
      recentActivity: [
        {
          title: "Standard production run completed",
          detail: "2 candidates passed validation thresholds for spread_error_regression.",
          timestamp: "43 minutes ago",
          tone: "green"
        },
        {
          title: "Slate preview generated",
          detail: "18 candidate signals were created for today's board but not yet saved.",
          timestamp: "8 minutes ago",
          tone: "blue"
        },
        {
          title: "Active model is aging",
          detail: "Current release is still valid, but yesterday's activation is older than the best fresh candidate.",
          timestamp: "Yesterday",
          tone: "amber"
        }
      ],
      missionNotes: [
        "Always show what is active, stale, waiting, and next.",
        "Keep primary actions plain-language and outcome-oriented.",
        "Treat artifact detail as drill-down depth, not top-level navigation."
      ]
    },
    trainingLab: {
      presets: [
        {
          name: "Fast sanity check",
          summary: "Cheap validation pass for early confirmation that features and task wiring look healthy.",
          fields: ["Short recent window", "Default policy", "Minimal comparables"],
          outcome: "Best for first-time setup or quick debugging."
        },
        {
          name: "Standard production run",
          summary: "Balanced daily workflow preset for regular candidate generation.",
          fields: ["Default train split", "Recommended validation split", "Stable evidence threshold"],
          outcome: "Best for normal operator cadence."
        },
        {
          name: "Deep validation run",
          summary: "Evidence-heavy pass that favors analyst confidence over speed.",
          fields: ["Larger sample guardrails", "Stricter comparables", "Longer validation history"],
          outcome: "Best before activating a new model."
        }
      ],
      candidates: [
        {
          name: "Tree Stump v13",
          modelFamily: "tree_stump",
          status: "Recommended candidate",
          metricLabel: "Validation MAE",
          metricValue: "1.87",
          whyItMatters: "Beat the active model while keeping decision rules simple enough for fast analyst explanation.",
          tags: ["Fresh run", "Explainable", "Production-ready"],
          runId: 413,
          targetTaskKey: "spread_error_regression",
          evidence: [
            "Latest validation slice improved on the active model without increasing workflow complexity.",
            "Decision path remains explainable enough for analyst-facing signal review.",
            "Feature freshness and validation timing still line up with today's operator flow."
          ],
          provenance: [
            { label: "Training batch", value: "Standard production run" },
            { label: "Selection policy", value: "validation_regression_candidate_v1" },
            { label: "Task", value: "spread_error_regression" }
          ],
          nextActions: ["Compare against the active release", "Preview today's slate", "Record release note before activation"]
        },
        {
          name: "Linear Feature v24",
          modelFamily: "linear_feature",
          status: "Competitive fallback",
          metricLabel: "Validation MAE",
          metricValue: "1.93",
          whyItMatters: "Slightly weaker overall, but still useful as a reference model for edge cases.",
          tags: ["Stable", "Reference baseline"],
          runId: 412,
          targetTaskKey: "spread_error_regression",
          evidence: [
            "Stayed competitive enough to remain a useful benchmark when tree-based behavior is surprising.",
            "More stable across edge cases, but no longer the top candidate for release.",
            "Useful as an analyst comparison point when signal rationale looks unusual."
          ],
          provenance: [
            { label: "Training batch", value: "Standard production run" },
            { label: "Role", value: "Reference baseline" },
            { label: "Task", value: "spread_error_regression" }
          ],
          nextActions: ["Keep as a comparison model", "Review only when the primary candidate drifts"]
        },
        {
          name: "Tree Stump v12",
          modelFamily: "tree_stump",
          status: "Currently active",
          metricLabel: "Validation MAE",
          metricValue: "1.95",
          whyItMatters: "Still dependable, but no longer the strongest candidate available.",
          tags: ["Active", "Needs release review"],
          runId: 401,
          selectionId: 91,
          targetTaskKey: "spread_error_regression",
          evidence: [
            "Current release still supports daily scoring without obvious operational risk.",
            "Validation score is now behind the freshest candidate.",
            "Analyst familiarity makes it safe to keep live until the newer candidate is reviewed."
          ],
          provenance: [
            { label: "Activation time", value: "Yesterday" },
            { label: "Release state", value: "Active" },
            { label: "Task", value: "spread_error_regression" }
          ],
          nextActions: ["Keep live if slate timing is tight", "Replace after release review if the preview stays healthy"]
        }
      ],
      validationNotes: [
        {
          title: "Selection policy stays in context, not in your face.",
          detail: "The system default is visible, but it is no longer the first thing an operator has to reason about.",
          tone: "blue"
        },
        {
          title: "Analyst drill-down remains available.",
          detail: "Backtest folds, evaluations, and registry detail should open as secondary evidence panels from here.",
          tone: "slate"
        },
        {
          title: "Freshness must be legible.",
          detail: "If candidate evidence is older than the latest feature snapshot or slate context, mark it stale before activation.",
          tone: "amber"
        }
      ],
      parameterGroups: [
        {
          title: "Essentials",
          description: "Always visible inputs for the operator path.",
          items: ["Target task", "Season or scope", "Preset", "Primary train and validation ratios"]
        },
        {
          title: "Expert settings",
          description: "Collapsed by default for advanced tuning.",
          items: ["Comparable sample threshold", "Policy override", "Exact split tuning", "Evidence constraints"]
        },
        {
          title: "System defaults",
          description: "Mostly read-only context to reduce clutter.",
          items: ["Default feature key", "Recommended policy", "Active model family", "Fallback behavior"]
        }
      ]
    },
    decision: {
      activeModel: {
        name: "Tree Stump v12",
        modelFamily: "tree_stump",
        status: "Active now",
        metricLabel: "Validation MAE",
        metricValue: "1.95",
        whyItMatters: "Reliable enough for daily scoring, but it is not the best current option anymore.",
        tags: ["Active release", "Aging"],
        runId: 401,
        selectionId: 91,
        targetTaskKey: "spread_error_regression",
        evidence: [
          "Still supports a stable daily workflow.",
          "Validation score no longer leads the pack.",
          "Low-risk to keep temporarily, but not the best available release."
        ],
        provenance: [
          { label: "Release state", value: "Active" },
          { label: "Activated", value: "Yesterday" },
          { label: "Task", value: "spread_error_regression" }
        ],
        nextActions: ["Keep active if release timing is constrained", "Replace after preview confidence check"]
      },
      recommendedModel: {
        name: "Tree Stump v13",
        modelFamily: "tree_stump",
        status: "Recommended for activation",
        metricLabel: "Validation MAE",
        metricValue: "1.87",
        whyItMatters: "Improves validation quality while preserving the same explainability story analysts already trust.",
        tags: ["Fresh evidence", "Safer upgrade"],
        runId: 413,
        targetTaskKey: "spread_error_regression",
        evidence: [
          "Best validation score among the current candidates.",
          "Preserves the explainability model analysts already know.",
          "Fresh enough to support a same-day release decision."
        ],
        provenance: [
          { label: "Training batch", value: "Standard production run" },
          { label: "Release posture", value: "Recommended" },
          { label: "Task", value: "spread_error_regression" }
        ],
        nextActions: ["Activate if preview stays healthy", "Add release note", "Move into Slate Runner"]
      },
      checklist: [
        { label: "Candidate beats active model on primary metric", state: "done" },
        { label: "Validation evidence is fresh for the current task", state: "done" },
        { label: "Slate preview reviewed before activation", state: "attention" },
        { label: "Analyst note recorded for release history", state: "pending" }
      ],
      history: [
        {
          title: "Activated Tree Stump v12",
          detail: "Release replaced Linear Feature v24 after a standard production cycle.",
          timestamp: "Yesterday"
        },
        {
          title: "Kept current model",
          detail: "Previous candidate failed to show consistent improvement across evidence slices.",
          timestamp: "3 days ago"
        }
      ]
    },
    slateRunner: {
      presets: [
        {
          name: "Fast preview",
          summary: "Quick score pass for seeing whether the slate is worth deeper analyst attention.",
          fields: ["Selected task", "Today's slate", "Preview only"],
          outcome: "Best for morning operator triage."
        },
        {
          name: "Standard analyst review",
          summary: "Balanced scoring run with enough evidence for same-day analyst screening.",
          fields: ["Saved scoring run", "Comparable evidence", "Standard thresholds"],
          outcome: "Best for routine scoring."
        },
        {
          name: "Evidence-heavy review",
          summary: "Slowest but richest run, aimed at high-confidence review sessions.",
          fields: ["More context panels", "Denser comparables", "Maximum traceability"],
          outcome: "Best before publishing or escalation."
        }
      ],
      scenarios: [
        {
          title: "Today's full slate",
          detail: "12 games ready to score with the standard analyst review preset.",
          status: "Recommended",
          preset: "Standard analyst review"
        },
        {
          title: "Single game preview",
          detail: "Swiss matchup preview for operator spot-checking ahead of full save.",
          status: "Available",
          preset: "Fast preview"
        },
        {
          title: "Historical comparison batch",
          detail: "Backfill-style scenario for checking how the active release behaves on prior boards.",
          status: "Advanced",
          preset: "Evidence-heavy review"
        }
      ],
      queueSummary: {
        slateLabel: "Tuesday slate",
        openSignals: "18 candidate signals",
        activeModel: "Tree Stump v12",
        note: "Preview exists, but the run has not been saved for downstream review yet."
      }
    },
    signalsDesk: {
      filters: ["Candidate", "Needs review", "Evidence-heavy", "High confidence", "Saved today"],
      signals: [
        {
          id: "SIG-204",
          game: "BOS vs NYK",
          market: "Spread",
          signalStrength: "High",
          evidenceRating: "Strong",
          status: "Needs review",
          recommendation: "Lean Boston -4.5",
          summary: "Edge looks durable across comparable cases and the active model agrees with the latest validation slice.",
          tags: ["Candidate", "Evidence-heavy", "Spread Error"],
          opportunityId: 204,
          scoringRunId: 601,
          targetTaskKey: "spread_error_regression",
          evidence: [
            "Comparable cases stayed aligned after the latest training run.",
            "Active model and fresh candidate both point in the same direction.",
            "Signal crossed the stronger evidence threshold in the preview batch."
          ],
          provenance: [
            { label: "Scoring run", value: "Slate preview batch" },
            { label: "Task", value: "spread_error_regression" },
            { label: "Evidence", value: "Strong" }
          ],
          nextActions: ["Inspect comparables", "Decide candidate vs watchlist", "Save analyst note if escalated"]
        },
        {
          id: "SIG-205",
          game: "LAL vs DEN",
          market: "Total",
          signalStrength: "Medium",
          evidenceRating: "Moderate",
          status: "In review",
          recommendation: "Watch Over 228.5",
          summary: "Signal is directionally useful, but comparables are mixed enough to keep it in analyst review.",
          tags: ["Totals", "Needs review"],
          opportunityId: 205,
          scoringRunId: 602,
          targetTaskKey: "total_error_regression",
          evidence: [
            "Totals direction is consistent, but comparable quality is uneven.",
            "Review-needed status comes from mixed evidence rather than hard failure.",
            "Best handled as an analyst queue item instead of an auto-escalation."
          ],
          provenance: [
            { label: "Scoring run", value: "Slate preview batch" },
            { label: "Task", value: "total_error_regression" },
            { label: "Evidence", value: "Moderate" }
          ],
          nextActions: ["Review market context", "Inspect if a stronger comparable set exists"]
        },
        {
          id: "SIG-206",
          game: "DAL vs PHX",
          market: "Spread",
          signalStrength: "High",
          evidenceRating: "Strong",
          status: "Candidate",
          recommendation: "Lean Phoenix +3.0",
          summary: "Model edge and evidence both cleared the stronger review threshold in the latest slate preview.",
          tags: ["Candidate", "High confidence"],
          opportunityId: 206,
          scoringRunId: 603,
          targetTaskKey: "spread_error_regression",
          evidence: [
            "Signal cleared the higher-confidence threshold.",
            "Evidence stayed consistent across the strongest comparable segment.",
            "No immediate model-quality concern was raised by the signal trace."
          ],
          provenance: [
            { label: "Scoring run", value: "Slate preview batch" },
            { label: "Task", value: "spread_error_regression" },
            { label: "Evidence", value: "Strong" }
          ],
          nextActions: ["Escalate for analyst disposition", "Save signal if queue remains stable"]
        },
        {
          id: "SIG-207",
          game: "MIL vs CLE",
          market: "Total",
          signalStrength: "Low",
          evidenceRating: "Thin",
          status: "Watchlist",
          recommendation: "No immediate action",
          summary: "Interesting score shape, but evidence is not strong enough for direct analyst escalation.",
          tags: ["Watchlist", "Thin evidence"],
          opportunityId: 207,
          scoringRunId: 604,
          targetTaskKey: "total_error_regression",
          evidence: [
            "Interesting direction, but the evidence layer is thin.",
            "Does not justify direct action without fresh support.",
            "Should remain visible so analysts can revisit after another run."
          ],
          provenance: [
            { label: "Scoring run", value: "Slate preview batch" },
            { label: "Task", value: "total_error_regression" },
            { label: "Evidence", value: "Thin" }
          ],
          nextActions: ["Leave on watchlist", "Recheck after a saved slate run"]
        }
      ]
    },
    history: {
      entries: [
        {
          title: "Training run archive updated",
          detail: "Latest candidate batch stored under the standard production preset.",
          timestamp: "43 minutes ago",
          tone: "blue"
        },
        {
          title: "Selection decision recorded",
          detail: "Most recent activation note is available for release-history drill-down.",
          timestamp: "Yesterday",
          tone: "green"
        }
      ]
    },
    settings: {
      groups: [
        {
          title: "Workflow defaults",
          description: "Defaults that should inform the UX without overwhelming the operator flow.",
          items: ["Default target task", "Default feature key", "Default training preset", "Default scoring preset"]
        },
        {
          title: "Display preferences",
          description: "Preferences that affect density and visibility rather than system behavior.",
          items: ["Compact tables in History", "Expanded evidence panels", "Workflow strip on every page", "Signal card density"]
        }
      ]
    },
    help: {
      glossary: [
        {
          term: "Training Lab",
          definition: "The place to create, compare, and validate candidate models for a target task."
        },
        {
          term: "Model Decision",
          definition: "The release-decision workspace where a candidate becomes the active model or is rejected."
        },
        {
          term: "Signals Desk",
          definition: "The analyst workspace for reviewing scored output, evidence, and recommended actions."
        }
      ]
    }
  };
}
