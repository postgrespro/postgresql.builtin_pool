# Base checks of making and switching the snapshots
# We are using three backends: read-write, read, snapshot.

setup
{
	select 0 from pg_make_snapshot();

	create table t1 as
	select generate_series as id, trim( to_char( generate_series, '000000000' ) ) as "name"
	from generate_series( 1, 1000 );
}

teardown
{
 select pg_switch_to_snapshot( 0 );
 select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
 DROP TABLE IF EXISTS t1;
}

session "s1"

step "s1_sel_r" {
	select "name" from t1 where id = 566;
}

step "s1_update_1" {
	update t1 set "name" = "name" || '_update_1';
} 

step "s1_update_2" {
	update t1 set "name" = "name" || '_update_2';
}

session "s2"
step "s2_sel_r" {
	select "name" from t1 where id = 566;
}

session "s3"
step "s3_make_sn" {
	select 1 from pg_make_snapshot();
}

step "s3_sw_0" {
	select pg_switch_to_snapshot( 0 );
}

step "s3_sw_1" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );
}

step "s3_sw_2" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );
}

step "s3_sw_3" {
	select pg_switch_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

permutation "s1_sel_r" "s3_make_sn" "s1_update_1" "s3_make_sn" "s1_update_2" "s1_sel_r" "s2_sel_r" "s3_sw_1" "s1_sel_r" "s2_sel_r" "s3_sw_2" "s1_sel_r" "s2_sel_r" "s3_sw_3" "s1_sel_r" "s2_sel_r" "s3_sw_0" "s1_sel_r" "s2_sel_r"
