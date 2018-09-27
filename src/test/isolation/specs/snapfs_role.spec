# Role and permissions checks
# We are using three backends: read-write, read, snapshot.

setup {
	create table t1 as
	select generate_series as id, trim( to_char( generate_series, '000' ) ) as "name"
	from generate_series( 1, 100 );
}

teardown {
	select pg_switch_to_snapshot( 0 );
	select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
	DROP TABLE t1 CASCADE;
}

session "s1"
step "s1_cr" {
	create role test_role1 nologin;
}
step "s1_check" {
	select rolname, rolcanlogin from pg_roles where rolname in ( 'test_role1', 'test_role2' );
}
step "s1_ar" {
	alter role test_role1 rename to test_role2;
	alter role test_role2 login;
}
step "s1_dr" {
	drop role test_role2;
}
step "s1_sb_0" {
	select pg_set_backend_snapshot( 0 );
}
step "s1_sb_1" {
	select pg_set_backend_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );
}
step "s1_sb_2" {
	select pg_set_backend_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );
}
step "s1_sb_3" {
	select pg_set_backend_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

session "s2"
step "s2_check" {
	select rolname, rolcanlogin from pg_roles where rolname in ( 'test_role1', 'test_role2' );
}
step "s2_sb_0" {
	select pg_set_backend_snapshot( 0 );
}
step "s2_sb_1" {
	select pg_set_backend_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );
}
step "s2_sb_2" {
	select pg_set_backend_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );
}
step "s2_sb_3" {
	select pg_set_backend_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

session "s3"
step "s3_mk_sn" {
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
step "s3_rc_sn" {
  select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}
step "s3_rc_sn_1" {
  select pg_recover_to_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );
}
step "s3_rc_sn_2" {
  select pg_recover_to_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );
}

permutation "s3_mk_sn" "s1_cr" "s3_mk_sn" "s1_check" "s1_ar" "s3_mk_sn" "s1_dr" "s1_check" "s2_check" "s3_sw_1" "s1_check" "s2_check" "s3_sw_2" "s1_check" "s2_check" "s3_sw_3" "s1_check" "s2_check" "s3_sw_0" "s1_check" "s2_check"
#permutation "s3_mk_sn" "s1_cr" "s3_mk_sn" "s1_check" "s1_ar" "s3_mk_sn" "s1_dr" "s1_check" "s2_check" "s1_sb_1" "s1_check" "s2_check" "s1_sb_2" "s1_check" "s2_check" "s1_sb_3" "s1_check" "s2_check" "s1_sb_0" "s1_check" "s2_check"
#permutation "s3_mk_sn" "s1_cr" "s3_mk_sn" "s1_check" "s1_ar" "s3_mk_sn" "s1_dr" "s1_check" "s2_check" "s2_sb_1" "s1_check" "s2_check" "s2_sb_2" "s1_check" "s2_check" "s2_sb_3" "s1_check" "s2_check" "s2_sb_0" "s1_check" "s2_check"
#permutation "s3_mk_sn" "s1_cr" "s3_mk_sn" "s1_check" "s1_ar" "s3_mk_sn" "s1_dr" "s1_check" "s2_check" "s3_rc_sn_2" "s1_check" "s2_check"
permutation "s3_mk_sn" "s1_cr" "s3_mk_sn" "s1_check" "s1_ar" "s3_mk_sn" "s1_dr" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check"
