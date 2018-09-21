# Making and updating temp table before and after the snapshot.
# Snapshots making and creating will process in the another session.

teardown {
	select pg_switch_to_snapshot( 0 );
	select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

session "s1"
step "s1_cr_tab" {
	create temp table tmp1 as
	select generate_series as id, trim( to_char( generate_series, '000000000' ) ) as "name"
	from generate_series( 1, 1000 );
}

step "s1_upd" {
	update tmp1 set "name" = "name" || '_updated1' where id = 566;
}

step "s1_sel" {
	select "name" from tmp1 where id = 566;
}

step "s1_drop" {
	drop table tmp1;
}

session "s2"
step "s2_mk_sn" {
	select 1 as "make_snapshot" from pg_make_snapshot();
}

step "s2_sw_sn_1" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 3 from pg_control_snapshot() ) );
}
step "s2_sw_sn_2" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );
}
step "s2_sw_sn_3" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );
}
step "s2_sw_sn_4" {
	select pg_switch_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}
step "s2_sw_sn_0" {
	select pg_switch_to_snapshot( 0 );
}

permutation "s2_mk_sn" "s1_cr_tab" "s2_mk_sn" "s1_upd" "s2_mk_sn" "s1_drop" "s2_mk_sn" "s2_sw_sn_1" "s1_sel" "s2_sw_sn_2" "s1_sel" "s2_sw_sn_3" "s1_sel" "s2_sw_sn_4" "s1_sel" "s2_sw_sn_0" "s1_sel"
