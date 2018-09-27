# Vacuum checks
# We are using three backends: read-write, read, snapshot.

setup {
	create table t1 as
	select generate_series as id, trim( to_char( generate_series, '000000' ) ) as "name"
	from generate_series( 1, 100000 );
	alter table t1 add constraint t1_pk PRIMARY KEY ( id );
}

teardown {
 select pg_switch_to_snapshot( 0 );
 select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
 DROP TABLE t1;
}

session "s1"
step "s1_sel" {
	select * from ( select count(*) as cnt, avg( id )::float as avg from t1 ) as part1 left join ( select * from t1 where id = 566 ) as part2 on ( 1 = 1 );
}
step "s1_ins" {
	insert into t1
	select generate_series as id, trim( to_char( generate_series, '000000' ) ) || '_new' as "name"
  from generate_series( 1, 100000 );
}
step "s1_upd" {
	update t1 set name = name || '_upd';
}
step "s1_del" {
	delete from t1;
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
step "s2_sel" {
	select * from ( select count(*) as cnt, avg( id )::float as avg from t1 ) as part1 left join ( select * from t1 where id = 566 ) as part2 on ( 1 = 1 );
}
step "s2_v" {
	vacuum t1;
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

permutation "s3_mk_sn" "s1_del" "s3_mk_sn" "s2_v" "s1_ins" "s3_mk_sn" "s1_upd" "s1_sel" "s2_sel" "s3_sw_1" "s1_sel" "s2_sel" "s3_sw_2" "s1_sel" "s2_sel" "s3_sw_3" "s1_sel" "s2_sel" "s3_sw_0" "s1_sel" "s2_sel"
permutation "s3_mk_sn" "s1_del" "s3_mk_sn" "s2_v" "s1_ins" "s3_mk_sn" "s1_upd" "s1_sel" "s2_sel" "s1_sb_1" "s1_sel" "s2_sel" "s1_sb_2" "s1_sel" "s2_sel" "s1_sb_3" "s1_sel" "s2_sel" "s1_sb_0" "s1_sel" "s2_sel"
permutation "s3_mk_sn" "s1_del" "s3_mk_sn" "s2_v" "s1_ins" "s3_mk_sn" "s1_upd" "s1_sel" "s2_sel" "s3_rc_sn" "s1_sel" "s2_sel" "s3_rc_sn" "s1_sel" "s2_sel" "s3_rc_sn" "s1_sel" "s2_sel" "s3_mk_sn"
permutation "s3_mk_sn" "s1_del" "s3_mk_sn" "s2_v" "s1_ins" "s3_mk_sn" "s1_upd" "s1_sel" "s2_sel" "s3_rc_sn_1" "s1_sel" "s2_sel" "s3_mk_sn"
