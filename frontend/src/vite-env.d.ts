/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_ENABLE_WORKFLOW_DESK?: string;
  readonly VITE_DEFAULT_TARGET_TASK?: string;
  readonly VITE_DEFAULT_TRAIN_RATIO?: string;
  readonly VITE_DEFAULT_VALIDATION_RATIO?: string;
  readonly VITE_DEFAULT_MINIMUM_TRAIN_GAMES?: string;
  readonly VITE_DEFAULT_TEST_WINDOW_GAMES?: string;
  readonly VITE_DEFAULT_SEASON_LABEL?: string;
  readonly VITE_DEFAULT_HOME_TEAM_CODE?: string;
  readonly VITE_DEFAULT_AWAY_TEAM_CODE?: string;
  readonly VITE_DEFAULT_GAME_DATE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
