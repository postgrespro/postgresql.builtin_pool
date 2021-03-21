-- predictability
SET synchronous_commit = on;

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');

CREATE TABLE stats_test(data text);

-- function to wait for counters to advance
CREATE FUNCTION wait_for_decode_stats(check_reset bool) RETURNS void AS $$
DECLARE
  start_time timestamptz := clock_timestamp();
  updated bool;
BEGIN
  -- we don't want to wait forever; loop will exit after 30 seconds
  FOR i IN 1 .. 300 LOOP

    -- check to see if all updates have been reset/updated
    SELECT CASE WHEN check_reset THEN (spill_txns = 0)
                ELSE (spill_txns > 0)
           END
    INTO updated
    FROM pg_stat_replication_slots WHERE slot_name='regression_slot';

    exit WHEN updated;

    -- wait a little
    perform pg_sleep_for('100 milliseconds');

    -- reset stats snapshot so we can test again
    perform pg_stat_clear_snapshot();

  END LOOP;

  -- report time waited in postmaster log (where it won't change test output)
  RAISE LOG 'wait_for_decode_stats delayed % seconds',
    extract(epoch from clock_timestamp() - start_time);
END
$$ LANGUAGE plpgsql;

-- spilling the xact
BEGIN;
INSERT INTO stats_test SELECT 'serialize-topbig--1:'||g.i FROM generate_series(1, 5000) g(i);
COMMIT;
SELECT count(*) FROM pg_logical_slot_peek_changes('regression_slot', NULL, NULL, 'skip-empty-xacts', '1');

-- Check stats, wait for the stats collector to update. We can't test the
-- exact stats count as that can vary if any background transaction (say by
-- autovacuum) happens in parallel to the main transaction.
SELECT wait_for_decode_stats(false);
SELECT slot_name, spill_txns > 0 AS spill_txns, spill_count > 0 AS spill_count FROM pg_stat_replication_slots;

-- reset the slot stats, and wait for stats collector to reset
SELECT pg_stat_reset_replication_slot('regression_slot');
SELECT wait_for_decode_stats(true);
SELECT slot_name, spill_txns, spill_count FROM pg_stat_replication_slots;

-- decode and check stats again.
SELECT count(*) FROM pg_logical_slot_peek_changes('regression_slot', NULL, NULL, 'skip-empty-xacts', '1');
SELECT wait_for_decode_stats(false);
SELECT slot_name, spill_txns > 0 AS spill_txns, spill_count > 0 AS spill_count FROM pg_stat_replication_slots;

-- Ensure stats can be repeatedly accessed using the same stats snapshot. See
-- https://postgr.es/m/20210317230447.c7uc4g3vbs4wi32i%40alap3.anarazel.de
BEGIN;
SELECT slot_name FROM pg_stat_replication_slots;
SELECT slot_name FROM pg_stat_replication_slots;
COMMIT;

DROP FUNCTION wait_for_decode_stats(bool);
DROP TABLE stats_test;
SELECT pg_drop_replication_slot('regression_slot');
