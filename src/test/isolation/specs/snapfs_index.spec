# Making and dropping indexes before and after the snapshot.

setup {
	create table t1 as
  select generate_series as id, trim( to_char( generate_series, '00000' ) ) as "name", generate_series as field_to_sort
  from generate_series( 1, 10000 );
}

teardown {
 select pg_switch_to_snapshot( 0 );
 select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
 DROP TABLE IF EXISTS t1 CASCADE;
}

session "s1"
step "s1_cr_i1" {
	create unique index t1_id_idx on t1 ( id );
}
step "s1_cr_i2" {
	create index t1_name_idx on t1 ( name );
}
step "s1_cr_i3" {
	create index t1_id4_idx on t1 ( ( id % 4 ) );
}
step "s1_dr_i1" {
	drop index t1_id_idx;
}
step "s1_dr_i2" {
	drop index t1_name_idx;
}
step "s1_dr_i3" {
	drop index t1_id4_idx;
}
step "s1_check" {
	select * from pg_indexes where tablename = 't1';
	analyse t1;
	select * from t1 where id % 4 = 3 order by field_to_sort limit 10;
	select * from t1 order by field_to_sort limit 10 offset 5550;
	select * from t1 where id = 5555;
}
step "s1_upd" {
	update t1 set name = name || '_upd_sess1' where id % 4 = 0;
}
step "s1_del" {
	delete from t1 where id % 4 = 1;
}
step "s1_ins" {
	insert into t1
	select generate_series as id, trim( to_char( generate_series, '00000' ) ) || '_new' as "name"
  from generate_series( 10001, 100010 );
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
	select * from pg_indexes where tablename = 't1';
	analyse t1;
	select * from t1 where id % 4 = 3 order by field_to_sort limit 10;
	select * from t1 order by field_to_sort limit 10 offset 5550;
	select * from t1 where id = 5555;
}
step "s2_upd" {
	update t1 set name = name || '_upd_sess2' where id % 4 = 2;
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

permutation "s1_cr_i1" "s3_mk_sn" "s1_cr_i2" "s1_ins" "s1_cr_i3" "s3_mk_sn" "s1_upd" "s2_upd" "s1_del" "s3_mk_sn" "s1_dr_i1" "s1_dr_i2" "s1_dr_i3" "s3_sw_1" "s1_check" "s2_check" "s3_sw_2" "s1_check" "s2_check" "s3_sw_3" "s1_check" "s2_check" "s3_sw_0" "s1_check" "s2_check"
permutation "s1_cr_i1" "s3_mk_sn" "s1_cr_i2" "s1_ins" "s1_cr_i3" "s3_mk_sn" "s1_upd" "s2_upd" "s1_del" "s3_mk_sn" "s1_dr_i1" "s1_dr_i2" "s1_dr_i3" "s1_sb_1" "s1_check" "s2_check" "s1_sb_2" "s1_check" "s2_check" "s1_sb_3" "s1_check" "s2_check" "s1_sb_0" "s1_check" "s2_check"
permutation "s1_cr_i1" "s3_mk_sn" "s1_cr_i2" "s1_ins" "s1_cr_i3" "s3_mk_sn" "s1_upd" "s2_upd" "s1_del" "s3_mk_sn" "s1_dr_i1" "s1_dr_i2" "s1_dr_i3" "s2_sb_1" "s1_check" "s2_check" "s2_sb_2" "s1_check" "s2_check" "s2_sb_3" "s1_check" "s2_check" "s2_sb_0" "s1_check" "s2_check"
permutation "s1_cr_i1" "s3_mk_sn" "s1_cr_i2" "s1_ins" "s1_cr_i3" "s3_mk_sn" "s1_upd" "s2_upd" "s1_del" "s3_mk_sn" "s1_dr_i1" "s1_dr_i2" "s1_dr_i3" "s3_rc_sn_2" "s1_check" "s2_check"
permutation "s1_cr_i1" "s3_mk_sn" "s1_cr_i2" "s1_ins" "s1_cr_i3" "s3_mk_sn" "s1_upd" "s2_upd" "s1_del" "s3_mk_sn" "s1_dr_i1" "s1_dr_i2" "s1_dr_i3" "s3_rc_sn" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check" "s3_rc_sn" "s1_check" "s2_check" "s3_mk_sn"
