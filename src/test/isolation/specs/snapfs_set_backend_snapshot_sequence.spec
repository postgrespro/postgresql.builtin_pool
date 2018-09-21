# Sequence testing.

setup {
	select 1 as "make_snapshot" from pg_make_snapshot();
	create sequence seq1;
}

teardown {
	select pg_set_backend_snapshot( 0 );
	select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
	drop sequence seq1;
}

session "s1"
step "s1_mk_sn" {
	select 1 as "make_snapshot" from pg_make_snapshot();
}
step "s1_sb_sn_1" {
	select pg_set_backend_snapshot( ( select recent_snapshot - 3 from pg_control_snapshot() ) );
}
step "s1_sb_sn_2" {
	select pg_set_backend_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );
}
step "s1_sb_sn_3" {
	select pg_set_backend_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );
}
step "s1_sb_sn_4" {
	select pg_set_backend_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

step "s1_sb_sn_0" {
	select pg_set_backend_snapshot( 0 );
}

step "s1_seq_n" {
	select nextval( 'seq1' );
}

step "s1_seq_c" {
	select currval( 'seq1' );
}

session "s2"
step "s2_seq_n" {
	select nextval( 'seq1' );
}

step "s2_seq_c" {
	select currval( 'seq1' );
}

permutation "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_sb_sn_1" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_sb_sn_2" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_sb_sn_3" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_sb_sn_4" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_sb_sn_0" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c"
permutation "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_sb_sn_4" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_sb_sn_3" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_sb_sn_2" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_sb_sn_1" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_sb_sn_0" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c"
