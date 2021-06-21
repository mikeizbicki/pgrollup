SET client_min_messages TO WARNING;

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

CREATE MATERIALIZED VIEW metahtml_rollup1 AS (
    SELECT
        hll_count(url),
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        url_host_key(url) AS host_key,
        date_trunc('day', accessed_at) AS access_day
    FROM metahtml
    GROUP BY host_key, access_day
);

CREATE MATERIALIZED VIEW metahtml_rollup2 AS (
    SELECT
        hll_count(url),
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        url_host_key(url) AS host_key
    FROM metahtml
    GROUP BY host_key
);

CREATE MATERIALIZED VIEW metahtml_rollup3 AS (
    SELECT
        hll_count(url),
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        date_trunc('day', accessed_at) AS access_day
    FROM metahtml
    GROUP BY access_day
);

CREATE MATERIALIZED VIEW metahtml_rollup4 AS (
    SELECT
        hll_count(url),
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    FROM metahtml
    GROUP BY timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup5 AS (
    SELECT
        hll_count(url),
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        date_trunc('day', accessed_at) AS access_day
    FROM metahtml
    GROUP BY timestamp_published,access_day
);

CREATE MATERIALIZED VIEW metahtml_rollup6 AS (
    SELECT
        hll_count(url),
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        jsonb_array_elements(jsonb->'links'->'best'->'value')->>'href' AS links
    FROM metahtml
    GROUP BY links
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

--------------------------------------------------------------------------------
-- this test is just to verify that group by columns get parsed correctly

CREATE VIEW metahtml_rollup_langmonth AS (
    SELECT
        jsonb->'language'->'best'->>'value' AS language,  
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY language,timestamp_published
);
/*
SELECT ((metahtml.jsonb -> 'language'::text) -> 'best'::text) ->> 'value'::text AS language,
    date_trunc('month'::text, ((((metahtml.jsonb -> 'timestamp.published'::text) -> 'best'::text) -> 'value'::text) ->> 'lo'::text)::timestamp with time zone) AS timestamp_published,
    hll_count(metahtml.url) AS url,
    hll_count(url_hostpathquery_key(metahtml.url)) AS hostpathquery,
    hll_count(url_hostpath_key(metahtml.url)) AS hostpath
   FROM metahtml
  GROUP BY (((metahtml.jsonb -> 'language'::text) -> 'best'::text) ->> 'value'::text), (date_trunc('month'::text, ((((metahtml.jsonb -> 'timestamp.published'::text) -> 'best'::text) -> 'value'::text) ->> 'lo'::text)::timestamp with time zone));
*/

select * from metahtml_rollup_langmonth;
