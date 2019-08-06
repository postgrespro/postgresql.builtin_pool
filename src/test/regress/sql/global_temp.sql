--
-- GLOBAL TEMP
-- Test global temp relations
--

-- Test ON COMMIT DELETE ROWS

CREATE GLOBAL TEMP TABLE global_temptest(col int) ON COMMIT DELETE ROWS;

BEGIN;
INSERT INTO global_temptest VALUES (1);
INSERT INTO global_temptest VALUES (2);

SELECT * FROM global_temptest;
COMMIT;

SELECT * FROM global_temptest;

DROP TABLE global_temptest;

BEGIN;
CREATE GLOBAL TEMP TABLE global_temptest(col) ON COMMIT DELETE ROWS AS SELECT 1;

SELECT * FROM global_temptest;
COMMIT;

SELECT * FROM global_temptest;

DROP TABLE global_temptest;

-- Test foreign keys
BEGIN;
CREATE GLOBAL TEMP TABLE global_temptest1(col int PRIMARY KEY);
CREATE GLOBAL TEMP TABLE global_temptest2(col int REFERENCES global_temptest1)
  ON COMMIT DELETE ROWS;
INSERT INTO global_temptest1 VALUES (1);
INSERT INTO global_temptest2 VALUES (1);
COMMIT;
SELECT * FROM global_temptest1;
SELECT * FROM global_temptest2;

BEGIN;
CREATE GLOBAL TEMP TABLE global_temptest3(col int PRIMARY KEY) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMP TABLE global_temptest4(col int REFERENCES global_temptest3);
COMMIT;

-- For partitioned temp tables, ON COMMIT actions ignore storage-less
-- partitioned tables.
BEGIN;
CREATE GLOBAL TEMP TABLE temp_parted_oncommit (a int)
  PARTITION BY LIST (a) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMP TABLE temp_parted_oncommit_1
  PARTITION OF temp_parted_oncommit
  FOR VALUES IN (1) ON COMMIT DELETE ROWS;
INSERT INTO temp_parted_oncommit VALUES (1);
COMMIT;
-- partitions are emptied by the previous commit
SELECT * FROM temp_parted_oncommit;
DROP TABLE temp_parted_oncommit;

-- Using ON COMMIT DELETE on a partitioned table does not remove
-- all rows if partitions preserve their data.
BEGIN;
CREATE GLOBAL TEMP TABLE global_temp_parted_oncommit_test (a int)
  PARTITION BY LIST (a) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMP TABLE global_temp_parted_oncommit_test1
  PARTITION OF global_temp_parted_oncommit_test
  FOR VALUES IN (1) ON COMMIT PRESERVE ROWS;
INSERT INTO global_temp_parted_oncommit_test VALUES (1);
COMMIT;
-- Data from the remaining partition is still here as its rows are
-- preserved.
SELECT * FROM global_temp_parted_oncommit_test;
-- two relations remain in this case.
SELECT relname FROM pg_class WHERE relname LIKE 'global_temp_parted_oncommit_test%';
DROP TABLE global_temp_parted_oncommit_test;

-- Check dependencies between ON COMMIT actions with inheritance trees.
-- Data on the parent is removed, and the child goes away.
BEGIN;
CREATE GLOBAL TEMP TABLE global_temp_inh_oncommit_test (a int) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMP TABLE global_temp_inh_oncommit_test1 ()
  INHERITS(global_temp_inh_oncommit_test) ON COMMIT PRESERVE ROWS;
INSERT INTO global_temp_inh_oncommit_test1 VALUES (1);
INSERT INTO global_temp_inh_oncommit_test VALUES (1);
COMMIT;
SELECT * FROM global_temp_inh_oncommit_test;
-- two relations remain
SELECT relname FROM pg_class WHERE relname LIKE 'global_temp_inh_oncommit_test%';
DROP TABLE global_temp_inh_oncommit_test1;
DROP TABLE global_temp_inh_oncommit_test;

-- Global temp table cannot inherit from temporary relation
BEGIN;
CREATE TEMP TABLE global_temp_table (a int) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMP TABLE global_temp_table1 ()
  INHERITS(global_temp_table) ON COMMIT PRESERVE ROWS;
ROLLBACK;

-- Temp table can inherit from global temporary relation
BEGIN;
CREATE GLOBAL TEMP TABLE global_temp_table (a int) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE temp_table1 ()
  INHERITS(global_temp_table) ON COMMIT PRESERVE ROWS;
CREATE TEMP TABLE temp_table2 ()
  INHERITS(global_temp_table) ON COMMIT DELETE ROWS;
INSERT INTO temp_table2 VALUES (2);
INSERT INTO temp_table1 VALUES (1);
INSERT INTO global_temp_table VALUES (0);
SELECT * FROM global_temp_table;
COMMIT;
SELECT * FROM global_temp_table;
DROP TABLE temp_table2;
DROP TABLE temp_table1;
DROP TABLE global_temp_table;

-- Global temp table can inherit from normal relation
BEGIN;
CREATE TABLE normal_table (a int);
CREATE GLOBAL TEMP TABLE temp_table1 ()
  INHERITS(normal_table) ON COMMIT PRESERVE ROWS;
CREATE GLOBAL TEMP TABLE temp_table2 ()
  INHERITS(normal_table) ON COMMIT DELETE ROWS;
INSERT INTO temp_table2 VALUES (2);
INSERT INTO temp_table1 VALUES (1);
INSERT INTO normal_table VALUES (0);
SELECT * FROM normal_table;
COMMIT;
SELECT * FROM normal_table;
DROP TABLE temp_table2;
DROP TABLE temp_table1;
DROP TABLE normal_table;

-- Check SERIAL and BIGSERIAL pseudo-types
CREATE GLOBAL TEMP TABLE global_temp_table ( aid BIGSERIAL, bid SERIAL );
CREATE SEQUENCE test_sequence;
INSERT INTO global_temp_table DEFAULT VALUES;
INSERT INTO global_temp_table DEFAULT VALUES;
INSERT INTO global_temp_table DEFAULT VALUES;
SELECT * FROM global_temp_table;
SELECT NEXTVAL( 'test_sequence' );
\c
SELECT * FROM global_temp_table;
SELECT NEXTVAL( 'test_sequence' );
INSERT INTO global_temp_table DEFAULT VALUES;
INSERT INTO global_temp_table DEFAULT VALUES;
INSERT INTO global_temp_table DEFAULT VALUES;
SELECT * FROM global_temp_table;
SELECT NEXTVAL( 'test_sequence' );
DROP TABLE global_temp_table;
DROP SEQUENCE test_sequence;
