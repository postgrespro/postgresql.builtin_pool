# Table testing. Reverting the database back step by step

setup {
	select 1 as "make_snapshot" from pg_make_snapshot();
	create table foo( id integer not null primary key, name varchar );
}

session "s1"
step "s1_mk_sn" {
	select 1 as "make_snapshot" from pg_make_snapshot();
}
step "s1_rc_sn" {
	select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

step "s1_tab_i" {
	insert into foo ( id, name ) values ( 1, 'some_text_session_1' );
}

step "s1_tab_s" {
	select * from foo;
}

session "s2"
step "s2_tab_i" {
	insert into foo ( id, name ) values ( 2, 'some_text_session_2' );
}

step "s2_tab_s" {
	select * from foo;
}

permutation "s1_mk_sn" "s1_tab_i" "s1_mk_sn" "s2_tab_i" "s1_tab_s" "s2_tab_s" "s1_rc_sn" "s1_tab_s" "s2_tab_s" "s1_rc_sn" "s1_tab_s" "s2_tab_s" "s1_tab_i" "s1_tab_s" "s2_tab_s" "s1_rc_sn" "s1_tab_s" "s2_tab_s" 
