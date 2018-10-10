teardown {
	select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
	DROP TABLE IF EXISTS foo;
}

session "s1"
step "s1_mk_sn" {
	select 1 as "make_snapshot" from pg_make_snapshot();
	select pg_get_snapshot_size( ( select recent_snapshot from pg_control_snapshot() ) );
	create table foo as select 'test_data';
	select pg_get_snapshot_size( ( select recent_snapshot from pg_control_snapshot() ) ) > 0 as size_not_zero;
}

permutation "s1_mk_sn"
