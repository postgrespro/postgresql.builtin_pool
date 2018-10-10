# Role and permissions checks
# We are using three backends: read-write, read, snapshot.

setup {
	create role regressuser0;
	create role regressuser1;
	create role regressuser2;
	create role regressuser3;
	create table t1 as
	select generate_series as id, trim( to_char( generate_series, '00' ) ) as "name"
	from generate_series( 1, 10 );
	alter table t1 owner to regressuser0;
}

teardown {
	select pg_switch_to_snapshot( 0 );
	select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
	DROP TABLE IF EXISTS t1 CASCADE;
	DROP ROLE IF EXISTS regressuser3;
	DROP ROLE IF EXISTS regressuser2;
	DROP ROLE IF EXISTS regressuser1;
	DROP ROLE IF EXISTS regressuser0;
}

session "s_1"
step "s1_g_1" {
	grant select, insert, update, delete on t1 to regressuser1;
}
step "s1_g_2" {
	alter table t1 owner to regressuser2;
	revoke all on t1 from regressuser2;
	grant select, update, insert, delete on t1 to regressuser2;
}
step "s1_g_3" {
	grant select on t1 to regressuser3;
}
step "s1_r_1" {
	revoke delete, insert on table t1 from regressuser1;
}
step "s1_r_2" {
	revoke delete, insert, update on table t1 from regressuser2;
}
step "s1_check" {
	SELECT grantee, grantor, privilege_type, ( select tableowner from pg_tables where ( table_schema || '.' || table_name ) = ( schemaname || '.' || tablename ) ) FROM information_schema.role_table_grants WHERE table_name = 't1' order by grantor, grantee, privilege_type;
	select count(*) as cnt from t1;
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
	SELECT grantee, grantor, privilege_type, ( select tableowner from pg_tables where ( table_schema || '.' || table_name ) = ( schemaname || '.' || tablename ) ) FROM information_schema.role_table_grants WHERE table_name = 't1' order by grantor, grantee, privilege_type;
	select count(*) as cnt from t1;
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

permutation "s3_mk_sn" "s1_g_1" "s3_mk_sn" "s1_g_2" "s1_g_3" "s1_r_1" "s3_mk_sn" "s1_r_2" "s3_sw_1" "s1_check" "s2_check" "s3_sw_2" "s1_check" "s2_check" "s3_sw_3" "s1_check" "s2_check" "s3_sw_0" "s1_check" "s2_check"
permutation "s3_mk_sn" "s1_g_1" "s3_mk_sn" "s1_g_2" "s1_g_3" "s1_r_1" "s3_mk_sn" "s1_r_2" "s1_sb_1" "s1_check" "s2_check" "s1_sb_2" "s1_check" "s2_check" "s1_sb_3" "s1_check" "s2_check" "s1_sb_0" "s1_check" "s2_check"
permutation "s3_mk_sn" "s1_g_1" "s3_mk_sn" "s1_g_2" "s1_g_3" "s1_r_1" "s3_mk_sn" "s1_r_2" "s2_sb_1" "s1_check" "s2_check" "s2_sb_2" "s1_check" "s2_check" "s2_sb_3" "s1_check" "s2_check" "s2_sb_0" "s1_check" "s2_check"
permutation "s3_mk_sn" "s1_g_1" "s3_mk_sn" "s1_g_2" "s1_g_3" "s1_r_1" "s3_mk_sn" "s1_r_2" "s3_rc_sn_2" "s1_check" "s2_check"
permutation "s3_mk_sn" "s1_g_1" "s3_mk_sn" "s1_g_2" "s1_g_3" "s1_r_1" "s3_mk_sn" "s1_r_2" "s3_rc_sn" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check" "s3_mk_sn"
