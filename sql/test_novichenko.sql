create or replace language plpython3u;
create extension if not exists pg_rollup;

/*
 * function/table definitions are simplified forms of https://github.com/mikeizbicki/novichenko
 */

CREATE OR REPLACE FUNCTION url_remove_scheme(url TEXT)
RETURNS TEXT language plpgsql IMMUTABLE STRICT
AS $$
BEGIN
    RETURN COALESCE(SUBSTRING(url, '[^:/]*//(.*)'),url);
END
$$;

CREATE OR REPLACE FUNCTION url_host(url TEXT)
RETURNS TEXT language plpgsql IMMUTABLE STRICT
AS $$
DECLARE
    url_without_scheme TEXT = url_remove_scheme(url);
BEGIN
    RETURN SUBSTRING(url_without_scheme, '([^/?:]*):?[^/?]*[/?]?');
END
$$;

CREATE OR REPLACE FUNCTION url_host_key(url TEXT)
RETURNS TEXT language plpgsql IMMUTABLE STRICT
AS $$
DECLARE
    url_lower TEXT = lower(url);
BEGIN
    RETURN (host_key((url_host(url_lower))));
END
$$;

CREATE OR REPLACE FUNCTION host_key(host TEXT)
RETURNS TEXT language plpgsql IMMUTABLE STRICT
AS $$
BEGIN
    RETURN ((string_to_array(host,'.')),',')||')';
END
$$;

CREATE OR REPLACE FUNCTION url_hostpath_key(url TEXT)
RETURNS TEXT language plpgsql IMMUTABLE STRICT
AS $$
DECLARE
    url_lower TEXT = lower(url);
BEGIN
    RETURN (host_key((url_host(url_lower))) || ((url_lower)));
END
$$;

CREATE OR REPLACE FUNCTION url_hostpathquery_key(url TEXT)
RETURNS TEXT language plpgsql IMMUTABLE STRICT
AS $$
DECLARE
    url_lower TEXT = lower(url);
    query TEXT = ((url_lower));
BEGIN
    RETURN (
        host_key((url_host(url_lower))) ||
        ((url_lower)) ||
        CASE WHEN length(query)>0
            THEN '?' || query
            ELSE ''
        END
    );
END
$$;


CREATE TABLE metahtml (
    id BIGSERIAL PRIMARY KEY,
    accessed_at TIMESTAMPTZ NOT NULL,
    url TEXT NOT NULL, -- FIXME: add this constraint? CHECK (uri_normalize(uri(url)) = uri(url)),
    jsonb JSONB NOT NULL
);

insert into metahtml (accessed_at, url, jsonb) values
    ('2020-01-01 00:00:00', 'https://google.com', '{}'),
    ('2020-01-01 00:00:00', 'https://google.com/search', '{}'),
    ('2020-01-01 00:00:00', 'https://google.com/robots.txt', '{}'),
    ('2020-01-01 00:00:00', 'https://amazon.com', '{}'),
    ('2020-01-02 00:00:00', 'https://amazon.com', '{}'),
    ('2020-01-03 00:00:00', 'https://google.com', '{}'),
    ('2020-01-04 00:00:00', 'https://google.com', '{}'),
    ('2020-01-05 00:00:00', 'https://google.com', '{}');

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup1',
    wheres => $$
        url_host_key(url) AS host_key,
        date_trunc('day', accessed_at) AS access_day
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup2',
    wheres => $$
        url_host_key(url) AS host_key
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup3',
    wheres => $$
        date_trunc('day', accessed_at) AS access_day
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup4',
    wheres => $$
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup5',
    wheres => $$
        date_trunc('day', accessed_at) AS access_day,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

insert into metahtml (accessed_at, url, jsonb) values
    ('2020-01-01 00:00:00', 'https://google.com', '{}'),
    ('2020-01-01 00:00:00', 'https://google.com/search', '{}'),
    ('2020-01-01 00:00:00', 'https://google.com/robots.txt', '{}'),
    ('2020-01-01 00:00:00', 'https://amazon.com', '{}'),
    ('2020-01-02 00:00:00', 'https://amazon.com', '{}'),
    ('2020-01-03 00:00:00', 'https://google.com', '{}'),
    ('2020-01-04 00:00:00', 'https://google.com', '{}'),
    ('2020-01-05 00:00:00', 'https://google.com', '{}');

select assert_rollup('metahtml_rollup1');
select assert_rollup('metahtml_rollup2');
select assert_rollup('metahtml_rollup3');
select assert_rollup('metahtml_rollup4');
select assert_rollup('metahtml_rollup5');
