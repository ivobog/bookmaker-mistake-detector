import { Client } from "pg";

type Queryable = Record<string, unknown>;

export function dbConfig() {
  return {
    host: process.env.E2E_PGHOST ?? "127.0.0.1",
    port: Number(process.env.E2E_PGPORT ?? "5433"),
    database: process.env.E2E_PGDATABASE ?? "bookmaker_detector",
    user: process.env.E2E_PGUSER ?? "bookmaker",
    password: process.env.E2E_PGPASSWORD ?? "bookmaker"
  };
}

export async function withDb<T>(fn: (client: Client) => Promise<T>): Promise<T> {
  const client = new Client(dbConfig());
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

export async function scalar<T = number>(sql: string, params: unknown[] = []): Promise<T | null> {
  return withDb(async (client) => {
    const result = await client.query(sql, params);
    if (!result.rows.length) {
      return null;
    }
    return (Object.values(result.rows[0])[0] as T) ?? null;
  });
}

export async function row<T = Queryable>(sql: string, params: unknown[] = []): Promise<T | null> {
  return withDb(async (client) => {
    const result = await client.query(sql, params);
    return (result.rows[0] as T) ?? null;
  });
}

export async function rows<T = Queryable>(sql: string, params: unknown[] = []): Promise<T[]> {
  return withDb(async (client) => {
    const result = await client.query(sql, params);
    return result.rows as T[];
  });
}

export async function getLatestTrainingRun(
  targetTask: string,
): Promise<Record<string, unknown> | null> {
  return row(
    `
      SELECT
        id::int AS id,
        target_task,
        NULLIF(scope_team_code, '') AS team_code,
        NULLIF(scope_season_label, '') AS season_label,
        status,
        train_ratio,
        validation_ratio,
        artifact_json AS artifact,
        metrics_json AS metrics,
        created_at,
        completed_at
      FROM model_training_run
      WHERE target_task = $1
      ORDER BY completed_at DESC NULLS LAST, id DESC
      LIMIT 1
    `,
    [targetTask],
  );
}

export async function getTrainingRunById(runId: number): Promise<Record<string, unknown> | null> {
  return row(
    `
      SELECT
        id::int AS id,
        target_task,
        NULLIF(scope_team_code, '') AS team_code,
        NULLIF(scope_season_label, '') AS season_label,
        status,
        train_ratio,
        validation_ratio,
        artifact_json AS artifact,
        metrics_json AS metrics,
        created_at,
        completed_at
      FROM model_training_run
      WHERE id = $1
    `,
    [runId],
  );
}

export async function getEvaluationForRun(runId: number): Promise<Record<string, unknown> | null> {
  return row(
    `
      SELECT
        id::int AS id,
        model_training_run_id::int AS model_training_run_id,
        target_task,
        model_family,
        selected_feature,
        fallback_strategy,
        primary_metric_name,
        validation_metric_value,
        test_metric_value,
        validation_prediction_count,
        test_prediction_count,
        snapshot_json AS snapshot,
        created_at
      FROM model_evaluation_snapshot
      WHERE model_training_run_id = $1
    `,
    [runId],
  );
}

export async function getLatestActiveSelection(
  targetTask: string,
): Promise<Record<string, unknown> | null> {
  return row(
    `
      SELECT
        id::int AS id,
        target_task,
        model_family,
        selection_policy_name,
        model_evaluation_snapshot_id::int AS model_evaluation_snapshot_id,
        model_training_run_id::int AS model_training_run_id,
        rationale_json AS rationale,
        is_active,
        created_at
      FROM model_selection_snapshot
      WHERE target_task = $1 AND is_active = TRUE
      ORDER BY created_at DESC, id DESC
      LIMIT 1
    `,
    [targetTask],
  );
}

export async function getLatestBacktestRun(
  targetTask: string,
): Promise<Record<string, unknown> | null> {
  return row(
    `
      SELECT
        id::int AS id,
        target_task,
        selection_policy_name,
        strategy_name,
        minimum_train_games,
        test_window_games,
        fold_count,
        payload_json AS payload,
        created_at,
        completed_at
      FROM model_backtest_run
      WHERE target_task = $1
      ORDER BY completed_at DESC NULLS LAST, id DESC
      LIMIT 1
    `,
    [targetTask],
  );
}

export async function getLatestBatch(
  targetTask: string,
  scopeSource: "operator" | "team_scoped",
  teamCode?: string | null,
  seasonLabel?: string | null,
): Promise<Record<string, unknown> | null> {
  if (scopeSource === "operator") {
    return row(
      `
        SELECT
          materialization_batch_id,
          materialized_at,
          materialization_scope_source,
          materialization_scope_key
        FROM model_opportunity
        WHERE target_task = $1
          AND materialization_scope_source = 'operator'
        ORDER BY materialized_at DESC, id DESC
        LIMIT 1
      `,
      [targetTask],
    );
  }

  return row(
    `
      SELECT
        materialization_batch_id,
        materialized_at,
        materialization_scope_source,
        materialization_scope_team_code,
        materialization_scope_season_label,
        materialization_scope_key
      FROM model_opportunity
      WHERE target_task = $1
        AND materialization_scope_source IN ('team_scoped', 'game_scoped')
        AND materialization_scope_team_code = $2
        AND ($3::text IS NULL OR materialization_scope_season_label = $3)
      ORDER BY materialized_at DESC, id DESC
      LIMIT 1
    `,
    [targetTask, teamCode ?? null, seasonLabel ?? null],
  );
}

export async function getOpportunitiesForBatch(
  batchId: string,
): Promise<Array<Record<string, unknown>>> {
  return rows(
    `
      SELECT
        id,
        prediction_value,
        materialization_batch_id,
        materialized_at,
        materialization_scope_source,
        materialization_scope_key,
        payload_json AS payload
      FROM model_opportunity
      WHERE materialization_batch_id = $1
      ORDER BY id ASC
    `,
    [batchId],
  );
}

export async function getLatestEvaluationByFamily(
  targetTask: string,
  modelFamily: string,
): Promise<Record<string, unknown> | null> {
  return row(
    `
      SELECT
        id::int AS id,
        model_training_run_id::int AS model_training_run_id,
        model_family,
        selected_feature,
        snapshot_json AS snapshot,
        validation_metric_value,
        test_metric_value
      FROM model_evaluation_snapshot
      WHERE target_task = $1 AND model_family = $2
      ORDER BY created_at DESC, id DESC
      LIMIT 1
    `,
    [targetTask, modelFamily],
  );
}

export async function assertDbInvariants(targetTask: string): Promise<void> {
  const duplicateActiveSelection = await rows<{ target_task: string; active_count: string }>(
    `
      SELECT target_task, COUNT(*)::text AS active_count
      FROM model_selection_snapshot
      WHERE target_task = $1 AND is_active = TRUE
      GROUP BY target_task
      HAVING COUNT(*) > 1
    `,
    [targetTask],
  );
  if (duplicateActiveSelection.length > 0) {
    throw new Error(
      `Found duplicate active selections for ${targetTask}: ${JSON.stringify(duplicateActiveSelection)}`,
    );
  }

  const orphanEvaluations = await scalar<string>(
    `
      SELECT COUNT(*)::text
      FROM model_evaluation_snapshot mes
      LEFT JOIN model_training_run mtr ON mtr.id = mes.model_training_run_id
      LEFT JOIN model_registry mr ON mr.id = mes.model_registry_id
      LEFT JOIN feature_version fv ON fv.id = mes.feature_version_id
      WHERE mes.target_task = $1
        AND (mtr.id IS NULL OR mr.id IS NULL OR fv.id IS NULL)
    `,
    [targetTask],
  );
  if (Number(orphanEvaluations ?? "0") > 0) {
    throw new Error(`Found orphan model_evaluation_snapshot rows for ${targetTask}.`);
  }

  const orphanSelections = await scalar<string>(
    `
      SELECT COUNT(*)::text
      FROM model_selection_snapshot mss
      LEFT JOIN model_evaluation_snapshot mes ON mes.id = mss.model_evaluation_snapshot_id
      LEFT JOIN model_training_run mtr ON mtr.id = mss.model_training_run_id
      LEFT JOIN model_registry mr ON mr.id = mss.model_registry_id
      LEFT JOIN feature_version fv ON fv.id = mss.feature_version_id
      WHERE mss.target_task = $1
        AND (mes.id IS NULL OR mtr.id IS NULL OR mr.id IS NULL OR fv.id IS NULL)
    `,
    [targetTask],
  );
  if (Number(orphanSelections ?? "0") > 0) {
    throw new Error(`Found orphan model_selection_snapshot rows for ${targetTask}.`);
  }

  const inconsistentBatchScope = await scalar<string>(
    `
      SELECT COUNT(*)::text
      FROM (
        SELECT materialization_batch_id
        FROM model_opportunity
        WHERE target_task = $1
        GROUP BY materialization_batch_id
        HAVING COUNT(
          DISTINCT CONCAT_WS(
            '|',
            COALESCE(materialization_scope_source, ''),
            COALESCE(materialization_scope_team_code, ''),
            COALESCE(materialization_scope_season_label, ''),
            COALESCE(materialization_scope_key, '')
          )
        ) > 1
      ) inconsistent_batches
    `,
    [targetTask],
  );
  if (Number(inconsistentBatchScope ?? "0") > 0) {
    throw new Error(`Found model_opportunity batches with mixed scope metadata for ${targetTask}.`);
  }

  const invalidOperatorScope = await scalar<string>(
    `
      SELECT COUNT(*)::text
      FROM model_opportunity
      WHERE target_task = $1
        AND materialization_scope_source = 'operator'
        AND materialization_scope_key <> 'operator-wide'
    `,
    [targetTask],
  );
  if (Number(invalidOperatorScope ?? "0") > 0) {
    throw new Error(`Found operator-scoped opportunities without operator-wide scope key.`);
  }
}
