# Making and switching the snapshots during the transaction.

setup
{
	create table t1 as
	select generate_series as id, trim( to_char( generate_series, '000000000' ) ) as "name"
	from generate_series( 1, 1000 );
}

teardown
{
 select pg_switch_to_snapshot( 0 );
 select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
 DROP TABLE t1;
 --select pg_control_snapshot();
}

session "s1"
step "s1_begin" {
	begin;
}

step "s1_upd" {
	update t1 set "name" = "name" || '_updated' where id = 566;
}

step "s1_commit" {
	commit;
}

step "s1_rollback" {
	rollback;
}
step "s1_sel" {
	select "name" from t1 where id = 566;
}

session "s2"
step "s2_sel" {
	select "name" from t1 where id = 566;
}
step "s2_make_sn" {
	select pg_make_snapshot();
	--select pg_control_snapshot();
}
step "s2_sw_0" {
	select pg_switch_to_snapshot( 0 );
	select pg_control_snapshot();
}
step "s2_sw_1" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 3 from pg_control_snapshot() ) );
	select pg_control_snapshot();
}
step "s2_sw_2" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );
	select pg_control_snapshot();
}
step "s2_sw_3" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );
	select pg_control_snapshot();
}
step "s2_sw_4" {
	select pg_switch_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
	select pg_control_snapshot();
}

session "s3"
step "s3_sel" {
	select "name" from t1 where id = 566;
}

permutation "s1_begin" "s2_make_sn" "s1_upd" "s1_sel" "s2_sel" "s3_sel" "s2_make_sn" "s1_sel" "s2_sel" "s3_sel" "s2_sw_3" "s1_sel" "s2_sel" "s3_sel" "s1_commit" "s1_sel" "s2_sel" "s3_sel" "s2_sw_0" "s1_sel" "s2_sel" "s3_sel"
