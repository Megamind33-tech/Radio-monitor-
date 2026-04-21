-- Zambian Airplay Monitor — Supabase / Postgres schema
-- Run once in Supabase SQL Editor (New query → paste → Run).

-- ---------------------------------------------------------------------------
-- Core tables
-- ---------------------------------------------------------------------------

create table if not exists stations (
    station_id text primary key,
    name text not null,
    stream_url text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists detections (
    id bigserial primary key,
    station_id text not null references stations (station_id) on delete cascade,
    raw_title text not null,
    parsed_artist text,
    parsed_title text,
    captured_at timestamptz not null default now()
);

create index if not exists idx_detections_station_captured
    on detections (station_id, captured_at desc);

create index if not exists idx_detections_captured_at
    on detections (captured_at desc);

create table if not exists station_events (
    id bigserial primary key,
    station_id text not null references stations (station_id) on delete cascade,
    event_type text not null,
    detail text,
    event_at timestamptz not null default now()
);

create index if not exists idx_station_events_station_time
    on station_events (station_id, event_at desc);

create index if not exists idx_station_events_event_at
    on station_events (event_at desc);

-- Keep updated_at fresh when monitor upserts station rows
create or replace function set_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at := now();
    return new;
end;
$$;

drop trigger if exists trg_stations_updated_at on stations;
create trigger trg_stations_updated_at
    before update on stations
    for each row
    execute function set_updated_at();

-- ---------------------------------------------------------------------------
-- Views (shaped for dashboards and ad-hoc queries)
-- ---------------------------------------------------------------------------

-- Latest detection per station (current "now playing" from metadata)
create or replace view v_now_playing as
select distinct on (d.station_id)
    s.station_id,
    s.name as station_name,
    d.raw_title,
    d.parsed_artist,
    d.parsed_title,
    d.captured_at
from stations s
inner join detections d on d.station_id = s.station_id
order by d.station_id, d.captured_at desc;

-- Detection counts per station for the current UTC calendar day
create or replace view v_station_activity_today as
select
    s.station_id,
    s.name as station_name,
    count(d.id)::bigint as detections_today
from stations s
left join detections d
    on d.station_id = s.station_id
    and d.captured_at >= (date_trunc('day', timezone('utc', now()))) at time zone 'utc'
    and d.captured_at < (date_trunc('day', timezone('utc', now())) + interval '1 day') at time zone 'utc'
group by s.station_id, s.name;

-- Per-station artist spin counts for the last 7 days (aggregate in SQL for leaderboards)
create or replace view v_top_artists_7d as
select
    d.station_id,
    s.name as station_name,
    d.parsed_artist,
    count(*)::bigint as spins
from detections d
inner join stations s on s.station_id = d.station_id
where d.captured_at >= now() - interval '7 days'
  and d.parsed_artist is not null
  and btrim(d.parsed_artist) <> ''
group by d.station_id, s.name, d.parsed_artist;
