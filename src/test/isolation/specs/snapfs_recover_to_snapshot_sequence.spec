# Sequence testing. Reverting the database back step by step

setup {
	select 1 as "make_snapshot" from pg_make_snapshot();
	create sequence seq1;
}

session "s1"
step "s1_mk_sn" {
	select 1 as "make_snapshot" from pg_make_snapshot();
}
step "s1_rc_sn_1" {
	select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
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

permutation "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_mk_sn" "s1_seq_n" "s2_seq_n" "s1_rc_sn_1" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_rc_sn_1" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_rc_sn_1" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c" "s1_rc_sn_1" "s1_seq_n" "s2_seq_n" "s1_seq_c" "s2_seq_c"
