-- auto vaccum이 되었던 시간과 개수
select relname, autovacuum_count, last_autovacuum::timestamp with time zone at time zone 'Asia/Seoul' from pg_stat_all_tables where last_autovacuum is not null order by last_autovacuum desc limit 20;


-- 현재 실행중인 query
SELECT pid,query,now()-query_start as diff,query_start,state_change FROM pg_stat_activity where state='active' order by query_start desc;

-- 실행중인 pid kill
SELECT pg_terminate_backend( 
 --pid
);

--5분 지난 idle pid kill
SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '' -- db name
      AND pid <> pg_backend_pid()
      AND state = 'idle'
      AND state_change < current_timestamp - INTERVAL '5' MINUTE;

-- auto vaccum이 실행되고 있는지 확인
select count(*) from pg_stat_activity where query like 'autovacuum:%';

-- 한국 시간으로 timezone 설정
SET TIME ZONE +9;


-- 전체 함수 리스트
SELECT  p.proname
FROM    pg_catalog.pg_namespace n
JOIN    pg_catalog.pg_proc p
ON      p.pronamespace = n.oid
WHERE   n.nspname = 'public';


-- 전체 정의된 Table/View 리스트
select table_name, table_type from information_schema.tables where table_schema='public';


-- 함수의 파라메터와 리턴 타입
CREATE OR REPLACE FUNCTION format_types(oid[])
    RETURNS text[] AS $$
    SELECT ARRAY(SELECT format_type(unnest($1), null))
$$ LANGUAGE sql IMMUTABLE;

select prorettype, format_type(prorettype, null) as ret, format_types(proargtypes) as arg from pg_proc
 where proname='function_name' -- function name
 ;


-- Table/View의 column 정보 가져오기
SELECT
  a.attname as name,
  pg_catalog.format_type(a.atttypid, null) as type
  FROM
  pg_catalog.pg_attribute a
  WHERE
    a.attnum > 0
  AND NOT a.attisdropped
  AND a.attrelid = (
    SELECT c.oid
    FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'table_name' -- Table or view name
   AND pg_catalog.pg_table_is_visible(c.oid)
  );



-- plv8 extension 켜기
create extension plv8;

-- 함수 생성
create function encode_uri(text) returns text language plv8 strict immutable as $$
  return encodeURI($1);
$$;

-- 테스트 쿼리
select encode_uri('/search#/?a=1&abc=한글');

                                    
-- information_schema.columns  테이블을 조회하면 컬럼 정보가 조회 됩니다.
-- PostgreSql 자체가 소문자로 저장되기 때문에 조건절의 테이블명은 반드시 소문자로 해야합니다. 
select 	table_name
	, 	column_name
	, 	data_type
	, 	character_maximum_length
	,	is_nullable
from     	information_schema.columns 
where 	table_name = '테이블명'
order by     ordinal_position
