--
-- GLOBAL TEMP
-- Test global temp relations
--

-- Test ON COMMIT DELETE ROWS

CREATE GLOBAL TEMP TABLE temptest(col int) ON COMMIT DELETE ROWS;

BEGIN;
INSERT INTO temptest VALUES (1);
INSERT INTO temptest VALUES (2);

SELECT * FROM temptest;
COMMIT;

SELECT * FROM temptest;

DROP TABLE temptest;

BEGIN;
CREATE GLOBAL TEMP TABLE temptest(col) ON COMMIT DELETE ROWS AS SELECT 1;

SELECT * FROM temptest;
COMMIT;

SELECT * FROM temptest;

DROP TABLE temptest;

-- Test foreign keys
BEGIN;
CREATE GLOBAL TEMP TABLE temptest1(col int PRIMARY KEY);
CREATE GLOBAL TEMP TABLE temptest2(col int REFERENCES temptest1)
  ON COMMIT DELETE ROWS;
INSERT INTO temptest1 VALUES (1);
INSERT INTO temptest2 VALUES (1);
COMMIT;
SELECT * FROM temptest1;
SELECT * FROM temptest2;

BEGIN;
CREATE GLOBAL TEMP TABLE temptest3(col int PRIMARY KEY) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMP TABLE temptest4(col int REFERENCES temptest3);
COMMIT;

-- For partitioned temp tables, ON COMMIT actions ignore storage-less
-- partitioned tables.
begin;
CREATE GLOBAL TEMP TABLE temp_parted_oncommit (a int)
  partition by list (a) on commit delete rows;
CREATE GLOBAL TEMP TABLE temp_parted_oncommit_1
  partition of temp_parted_oncommit
  for values in (1) on commit delete rows;
insert into temp_parted_oncommit values (1);
commit;
-- partitions are emptied by the previous commit
select * from temp_parted_oncommit;
drop table temp_parted_oncommit;

-- Using ON COMMIT DELETE on a partitioned table does not remove
-- all rows if partitions preserve their data.
begin;
CREATE GLOBAL TEMP TABLE temp_parted_oncommit_test (a int)
  partition by list (a) on commit delete rows;
CREATE GLOBAL TEMP TABLE temp_parted_oncommit_test1
  partition of temp_parted_oncommit_test
  for values in (1) on commit preserve rows;
insert into temp_parted_oncommit_test values (1);
commit;
-- Data from the remaining partition is still here as its rows are
-- preserved.
select * from temp_parted_oncommit_test;
-- two relations remain in this case.
select relname from pg_class where relname like 'temp_parted_oncommit_test%';
drop table temp_parted_oncommit_test;

-- Check dependencies between ON COMMIT actions with inheritance trees.
-- Data on the parent is removed, and the child goes away.
begin;
CREATE GLOBAL TEMP TABLE temp_inh_oncommit_test (a int) on commit delete rows;
CREATE GLOBAL TEMP TABLE temp_inh_oncommit_test1 ()
  inherits(temp_inh_oncommit_test) on commit preserve rows;
insert into temp_inh_oncommit_test1 values (1);
insert into temp_inh_oncommit_test values (1);
commit;
select * from temp_inh_oncommit_test;
-- two relations remain
select relname from pg_class where relname like 'temp_inh_oncommit_test%';
drop table temp_inh_oncommit_test;

-- Tests with two-phase commit
-- Transactions creating objects in a temporary namespace cannot be used
-- with two-phase commit.

-- These cases generate errors about temporary namespace.
-- Function creation
begin;
create function pg_temp.twophase_func() returns void as
  $$ select '2pc_func'::text $$ language sql;
prepare transaction 'twophase_func';
-- Function drop
create function pg_temp.twophase_func() returns void as
  $$ select '2pc_func'::text $$ language sql;
begin;
drop function pg_temp.twophase_func();
prepare transaction 'twophase_func';
-- Operator creation
begin;
create operator pg_temp.@@ (leftarg = int4, rightarg = int4, procedure = int4mi);
prepare transaction 'twophase_operator';

-- These generate errors about temporary tables.
begin;
create type pg_temp.twophase_type as (a int);
prepare transaction 'twophase_type';
begin;
create view pg_temp.twophase_view as select 1;
prepare transaction 'twophase_view';
begin;
create sequence pg_temp.twophase_seq;
prepare transaction 'twophase_sequence';

-- Temporary tables cannot be used with two-phase commit.
CREATE GLOBAL TEMP TABLE twophase_tab (a int);
begin;
select a from twophase_tab;
prepare transaction 'twophase_tab';
begin;
insert into twophase_tab values (1);
prepare transaction 'twophase_tab';
begin;
lock twophase_tab in access exclusive mode;
prepare transaction 'twophase_tab';
begin;
drop table twophase_tab;
prepare transaction 'twophase_tab';

-- Corner case: current_schema may create a temporary schema if namespace
-- creation is pending, so check after that.  First reset the connection
-- to remove the temporary namespace.
\c -
SET search_path TO 'pg_temp';
BEGIN;
SELECT current_schema() ~ 'pg_temp' AS is_temp_schema;
PREPARE TRANSACTION 'twophase_search';
