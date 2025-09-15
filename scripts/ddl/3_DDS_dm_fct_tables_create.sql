-- DDL для слоя DDS

-- Дата
CREATE TABLE dm_date (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    week INTEGER NOT NULL
);
COMMENT ON TABLE dm_date IS 'Измерение Дата';

-- Турнир
CREATE TABLE dm_tour (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    category VARCHAR(100),
    location VARCHAR(100)
);
COMMENT ON TABLE dm_tour IS 'Измерение Турнир';

-- 3. Измерение Игрок
CREATE TABLE dm_player (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(100) NOT NULL UNIQUE,
    firstname VARCHAR(100) NOT NULL,
    lastname VARCHAR(100) NOT NULL,
    country CHAR(3), -- стандартные коды стран
    birth_date DATE,
    height_cm INTEGER CHECK (height_cm > 0),
    weight_kg INTEGER CHECK (weight_kg > 0),
    plays VARCHAR(20) CHECK (plays IN ('left', 'right'))
);
COMMENT ON TABLE dm_player IS 'Измерение Игрок';

-- Рейтинг
CREATE TABLE dm_player_rank (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES dm_player(id) ON DELETE CASCADE,
    rank INTEGER,
    points INTEGER NOT NULL DEFAULT 0,
    active_from DATE NOT NULL,
    active_to DATE, -- NULL означает текущую версию
    CONSTRAINT valid_dates CHECK (active_to IS NULL OR active_to > active_from)
);
COMMENT ON TABLE dm_player_rank IS 'Рейтинг игроков (SCD2)';

-- ФАКТ: Результаты матча (до/после)
CREATE TABLE fct_match_result (
    id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL UNIQUE,
    player1_id INTEGER NOT NULL REFERENCES dm_player(id),
    player2_id INTEGER NOT NULL REFERENCES dm_player(id),
    tour_id INTEGER NOT NULL REFERENCES dm_tour(id),
    date_id INTEGER NOT NULL REFERENCES dm_date(id),
    odd1 NUMERIC(5, 2),
    odd2 NUMERIC(5, 2),
    winner_id INTEGER NOT NULL REFERENCES dm_player(id),
    result_score VARCHAR(20) NOT NULL,
    p1_last_5_wins INTEGER,
    p1_tb_in_last_5_wins INTEGER,
    p1_h2h_wins INTEGER,
    p2_last_5_wins INTEGER,
    p2_tb_in_last_5_wins INTEGER,
    p2_h2h_wins INTEGER
);
COMMENT ON TABLE fct_match_result IS 'Факты матча: результат и контекст';

-- ФАКТ: Статистика игроков в матче
CREATE TABLE fct_match_performance (
    id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES fct_match_result(id) ON DELETE CASCADE,
    player_id INTEGER NOT NULL REFERENCES dm_player(id),
    is_player_one BOOLEAN NOT NULL, -- игрок
    aces INTEGER,
    double_faults INTEGER,
    first_serve_percentage INTEGER,
    first_serve_in INTEGER,
    first_serve_points_won INTEGER,
    first_serve_points_total INTEGER,
    second_serve_points_won INTEGER,
    second_serve_points_total INTEGER,
    break_points_saved INTEGER,
    break_points_total INTEGER,
    first_return_points_won INTEGER,
    first_return_points_total INTEGER,
    second_return_points_won INTEGER,
    second_return_points_total INTEGER,
    break_points_converted INTEGER,
    break_points_converted_total INTEGER,
    service_points_won INTEGER,
    service_points_total INTEGER,
    return_points_won INTEGER,
    return_points_total INTEGER,
    total_points_won INTEGER,
    total_points_total INTEGER,
    last_10_balls_won INTEGER,
    match_points_saved INTEGER,
    service_games_won INTEGER,
    service_games_total INTEGER,
    return_games_won INTEGER,
    return_games_total INTEGER,
    total_games_won INTEGER,
    total_games_total INTEGER
);
COMMENT ON TABLE fct_match_performance IS 'Детальная статистика игрока в матче';

-- fct_match_result
CREATE INDEX ON fct_match_result (player1_id);
CREATE INDEX ON fct_match_result (player2_id);
CREATE INDEX ON fct_match_result (tour_id);
CREATE INDEX ON fct_match_result (date_id);
CREATE INDEX ON fct_match_result (winner_id);

-- Для fct_match_performance
CREATE INDEX ON fct_match_performance (match_id);
CREATE INDEX ON fct_match_performance (player_id);
-- Составной индекс для анализа игрока в матче
CREATE INDEX ON fct_match_performance (match_id, player_id);

-- dm_player_rank
CREATE INDEX ON dm_player_rank (player_id);
CREATE INDEX ON dm_player_rank (active_from);
CREATE INDEX ON dm_player_rank (active_to);
-- Составной индекс для быстрого поиска актуального рейтинга
CREATE INDEX ON dm_player_rank (player_id, active_to);