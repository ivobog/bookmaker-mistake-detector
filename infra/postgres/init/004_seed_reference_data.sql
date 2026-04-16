INSERT INTO provider (name, provider_type, base_url, notes)
VALUES ('covers', 'historical_team_page', 'https://www.covers.com', 'Phase 1 fixture-backed provider')
ON CONFLICT (name) DO NOTHING;

INSERT INTO season (label, start_date, end_date, is_completed)
VALUES
    ('2021-2022', DATE '2021-10-19', DATE '2022-06-16', TRUE),
    ('2022-2023', DATE '2022-10-18', DATE '2023-06-12', TRUE),
    ('2023-2024', DATE '2023-10-24', DATE '2024-06-17', TRUE),
    ('2024-2025', DATE '2024-10-22', DATE '2025-06-22', TRUE)
ON CONFLICT (label) DO NOTHING;

INSERT INTO team (code, name, conference, division)
VALUES
    ('ATL', 'Atlanta Hawks', 'East', 'Southeast'),
    ('BOS', 'Boston Celtics', 'East', 'Atlantic'),
    ('BKN', 'Brooklyn Nets', 'East', 'Atlantic'),
    ('CHA', 'Charlotte Hornets', 'East', 'Southeast'),
    ('CHI', 'Chicago Bulls', 'East', 'Central'),
    ('CLE', 'Cleveland Cavaliers', 'East', 'Central'),
    ('DAL', 'Dallas Mavericks', 'West', 'Southwest'),
    ('DEN', 'Denver Nuggets', 'West', 'Northwest'),
    ('DET', 'Detroit Pistons', 'East', 'Central'),
    ('GSW', 'Golden State Warriors', 'West', 'Pacific'),
    ('HOU', 'Houston Rockets', 'West', 'Southwest'),
    ('IND', 'Indiana Pacers', 'East', 'Central'),
    ('LAC', 'Los Angeles Clippers', 'West', 'Pacific'),
    ('LAL', 'Los Angeles Lakers', 'West', 'Pacific'),
    ('MEM', 'Memphis Grizzlies', 'West', 'Southwest'),
    ('MIA', 'Miami Heat', 'East', 'Southeast'),
    ('MIL', 'Milwaukee Bucks', 'East', 'Central'),
    ('MIN', 'Minnesota Timberwolves', 'West', 'Northwest'),
    ('NOP', 'New Orleans Pelicans', 'West', 'Southwest'),
    ('NYK', 'New York Knicks', 'East', 'Atlantic'),
    ('OKC', 'Oklahoma City Thunder', 'West', 'Northwest'),
    ('ORL', 'Orlando Magic', 'East', 'Southeast'),
    ('PHI', 'Philadelphia 76ers', 'East', 'Atlantic'),
    ('PHX', 'Phoenix Suns', 'West', 'Pacific'),
    ('POR', 'Portland Trail Blazers', 'West', 'Northwest'),
    ('SAC', 'Sacramento Kings', 'West', 'Pacific'),
    ('SAS', 'San Antonio Spurs', 'West', 'Southwest'),
    ('TOR', 'Toronto Raptors', 'East', 'Atlantic'),
    ('UTA', 'Utah Jazz', 'West', 'Northwest'),
    ('WAS', 'Washington Wizards', 'East', 'Southeast')
ON CONFLICT (code) DO NOTHING;

