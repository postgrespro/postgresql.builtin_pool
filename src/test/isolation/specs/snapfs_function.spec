# Function checks
# We are using three backends: read-write, read, snapshot.

setup {
	create table t1 as
	select generate_series as id, trim( to_char( generate_series, '00' ) ) as "name"
	from generate_series( 1, 10 );
}

teardown {
	select pg_switch_to_snapshot( 0 );
	select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
	DROP TABLE IF EXISTS t1 CASCADE;
	DROP FUNCTION IF EXISTS get_some_data();
}

session "s_1"
step "s1_c_f" {
	CREATE OR REPLACE FUNCTION get_some_data() RETURNS character varying
	LANGUAGE 'plpgsql' AS $BODY$ BEGIN	RETURN 'some_data'; END; $BODY$;
}
step "s1_r_f" {
	CREATE OR REPLACE FUNCTION get_some_data() RETURNS character varying
	LANGUAGE 'plpgsql' AS $BODY$ DECLARE res varchar; BEGIN	select string_agg( name, ' ' ) into res from ( select * from t1 order by id ) as foo; RETURN res; END; $BODY$;
}
step "s1_d_f" {
	drop function get_some_data();
}
step "s1_check" {
	select * from get_some_data();
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
	select * from get_some_data();
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
step "s3_control_sn" {
	select * from pg_control_snapshot();
}

permutation "s3_mk_sn" "s1_c_f" "s3_mk_sn" "s1_r_f" "s3_mk_sn" "s1_d_f" "s3_sw_1" "s1_check" "s2_check" "s3_sw_2" "s1_check" "s2_check" "s3_sw_3" "s1_check" "s2_check" "s3_sw_0" "s1_check" "s2_check" 
permutation "s3_mk_sn" "s1_c_f" "s3_mk_sn" "s1_r_f" "s3_mk_sn" "s1_d_f" "s1_sb_1" "s1_check" "s2_check" "s1_sb_2" "s1_check" "s2_check" "s1_sb_3" "s1_check" "s2_check" "s1_sb_0" "s1_check" "s2_check" 
permutation "s3_mk_sn" "s1_c_f" "s3_mk_sn" "s1_r_f" "s3_mk_sn" "s1_d_f" "s2_sb_1" "s1_check" "s2_check" "s2_sb_2" "s1_check" "s2_check" "s1_sb_2" "s2_sb_3" "s1_check" "s2_check" "s1_sb_0" "s2_sb_0" "s1_check" "s2_check" 
permutation "s3_mk_sn" "s1_c_f" "s3_mk_sn" "s1_r_f" "s3_mk_sn" "s1_d_f" "s3_rc_sn_2" "s1_check" "s2_check" "s1_d_f"
permutation "s3_mk_sn" "s1_c_f" "s3_mk_sn" "s1_r_f" "s3_mk_sn" "s1_d_f" "s3_rc_sn" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check" "s3_mk_sn"
