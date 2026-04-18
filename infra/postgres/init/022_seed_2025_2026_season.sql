INSERT INTO season (label, start_date, end_date, is_completed)
VALUES ('2025-2026', DATE '2025-10-21', DATE '2026-06-19', FALSE)
ON CONFLICT (label) DO UPDATE
SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    is_completed = EXCLUDED.is_completed;
