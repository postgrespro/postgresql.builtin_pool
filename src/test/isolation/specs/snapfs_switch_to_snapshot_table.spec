# Table testing.

setup {
	create table foo( id serial not null primary key, name varchar );
}

teardown {
	select pg_switch_to_snapshot( 0 );
	select pg_remove_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
	drop table foo;
}

session "s1"
step "s1_mk_sn" {
	select 1 as "make_snapshot" from pg_make_snapshot();
}

step "s1_sw_sn_1" {
	select pg_switch_to_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );
}

step "s1_sw_sn_2" {
	select pg_switch_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );
}

step "s1_sw_sn_0" {
	select pg_switch_to_snapshot( 0 );
}

step "s1_tab_i" {
	insert into foo ( name ) values ( 'some_text_sess_1' );
}

step "s1_tab_s" {
	select * from foo;
}

session "s2"
step "s2_tab_i" {
	insert into foo ( name ) values ( 'some_text_sess_2' );
}

step "s2_tab_u" {
	update foo set name = name || '_upd_sess_2';
}

step "s2_tab_s" {
	select * from foo;
}

permutation "s1_mk_sn" "s1_tab_i" "s2_tab_i" "s1_mk_sn" "s2_tab_u" "s1_tab_s" "s2_tab_s" "s1_sw_sn_1" "s1_tab_s" "s2_tab_s" "s1_sw_sn_2" "s2_tab_u" "s1_tab_s" "s2_tab_s" "s1_sw_sn_0" "s2_tab_u" "s1_tab_s" "s2_tab_s"
