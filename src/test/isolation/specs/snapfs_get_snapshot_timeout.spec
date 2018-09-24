# The recent just created snapshot cannot be older than 1 minute :)

teardown {
	select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

session "s1"
step "s1_mk_sn" {
	select 1 as "make_snapshot" from pg_make_snapshot();
	select ( now() - ( select pg_get_snapshot_timestamp( ( select recent_snapshot from pg_control_snapshot() ) ) ) ) between '0 sec'::interval and '1 min'::interval as "timestamp_check"
}
step "s1_rc_sn_1" {
	select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

permutation "s1_mk_sn"
