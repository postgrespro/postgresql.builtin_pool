# Replication checks
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More;

# Wait until replay to complete
sub replay_wait( $$ ) {
	my $node_master = shift;
	my $node_standby = shift;
	my $until_lsn =
	  $node_master->safe_psql('postgres', "SELECT pg_current_wal_lsn()");

	$node_standby->poll_query_until('postgres',
		"SELECT (pg_last_wal_replay_lsn() - '$until_lsn'::pg_lsn) >= 0")
	  or die "standby never caught up";

	$node_master->safe_psql( 'postgres', "select pg_sleep( 1 );" );
}

my ( $ret, $stdout, $stderr );
my ( $ret_standby, $stdout_standby, $stderr_standby );

# Initialize
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Create table
$node_master->safe_psql( 'postgres',
	"CREATE TABLE t1 AS SELECT generate_series as id, trim( to_char( generate_series, '0000' ) ) as \"name\" from generate_series(1, 1000)" );
$node_master->safe_psql( 'postgres',
	"alter table t1 add constraint t1_pk PRIMARY KEY ( id )" );

## Make first snapshot
$node_master->safe_psql('postgres',
	"select pg_make_snapshot();");

# Create another table
$node_master->safe_psql( 'postgres',
	"CREATE TABLE t2 AS SELECT generate_series as id, trim( to_char( generate_series, '0000' ) ) as \"name\" from generate_series(1, 1000)" );
$node_master->safe_psql( 'postgres',
	"alter table t2 add constraint t2_pk PRIMARY KEY ( id )" );

# Take backup
my $backup_name = 'my_backup';
$node_master->backup($backup_name);

# Create streaming standby from backup
my $node_standby = get_new_node('standby');

$node_standby->init_from_backup($node_master, $backup_name,
	has_streaming => 1);

$node_standby->start;

# Drop constraint
$node_master->safe_psql( 'postgres', "alter table t1 drop constraint t1_pk;" );

# Create role
$node_master->safe_psql( 'postgres', "create role regression_testrole nologin;" );


## Make second snapshot
$node_master->safe_psql( 'postgres',
	"select pg_make_snapshot();" );

# Truncate table
$node_master->safe_psql( 'postgres', "truncate table t1;" );

# Insert
$node_master->safe_psql( 'postgres',
	"insert into t2 SELECT generate_series as id, trim( to_char( generate_series, '0000' ) ) as \"name\" from generate_series(1001, 2000)" );

# Update
$node_master->safe_psql( 'postgres',
	"update t2 set name = name || '_updated' where id <= 1000" );

# Drop role
$node_master->safe_psql( 'postgres', "drop role regression_testrole;" );

replay_wait( $node_master, $node_standby );

# Make third snapshot
$node_master->safe_psql( 'postgres',
	"select pg_make_snapshot();" );

# Delete
$node_master->safe_psql( 'postgres',
	"delete from t2 where id > 1000;" );

# Drop table
$node_master->safe_psql( 'postgres', "drop table t1;" );

replay_wait( $node_master, $node_standby );


# Checks start here

# t1
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );

like( $stderr, '/ERROR:  relation "t1" does not exist/', 't1 initial check master' );
like( $stderr_standby, '/ERROR:  relation "t1" does not exist/', 't1 initial check standby' );

# role
$ret = $node_master->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );

ok( $ret eq $ret_standby, 'initial role check' );

# t2
$ret = $node_master->safe_psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );

ok( $ret eq $ret_standby, 't2 initial check' );


## switch to snapshot checks
# Snapshot 1
$node_master->safe_psql( 'postgres',
	"select pg_switch_to_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );" );

# t1
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout eq '1000|500.5000000000000000|0566', 'switch to snapshot 1 t1 master check' );

( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
like( $stderr_standby, '/ERROR:  relation "t1" does not exist/', 'switch to snapshot 1 t1 standby check' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );

like( $stdout, '/Index Scan using t1_pk on t1/', 'switch to snapshot 1 t1 primary key master check' );

# t2
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
like( $stderr, '/ERROR:  relation "t2" does not exist/', 'switch to snapshot 1 t2 master check' );

( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
ok( $stdout_standby eq '1000|500.5000000000000000|0566_updated', 'switch to snapshot 1 t2 standby check' );

# role
$ret = $node_master->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );

ok( $ret eq '', 'switch to snapshot 1 role master check' );
ok( $ret_standby eq '', 'switch to snapshot 1 role standby check' );

# Snapshot 2
$node_master->safe_psql( 'postgres',
	"select pg_switch_to_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) );" );

# t1
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );

like( $stdout, '/Seq Scan on t1/', 'switch to snapshot 2 t1 primary key master check' );

# role
$ret = $node_master->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );

ok( $ret eq 'regression_testrole|f', 'switch to snapshot 2 role master check' );
ok( $ret_standby eq '', 'switch to snapshot 2 role standby check' );


# Snapshot 3
$node_master->safe_psql( 'postgres',
	"select pg_switch_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );" );

# t1
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout eq '0||', 'switch to snapshot 3 t1 master check' );

( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
like( $stderr_standby, '/ERROR:  relation "t1" does not exist/', 'switch to snapshot 3 t1 standby check' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );

like( $stdout, '/Seq Scan on t1/', 'switch to snapshot 3 t1 primary key master check' );

# role
$ret = $node_master->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );

ok( $ret eq '', 'switch to snapshot 3 role master check' );
ok( $ret_standby eq '', 'switch to snapshot 3 role standby check' );


#teardown
$node_master->safe_psql( 'postgres',
	"select pg_switch_to_snapshot( 0 );" );
replay_wait( $node_master, $node_standby );


## set backend checks
# t1
( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) ); explain select id, name from t1 where id = 566;" );
like( $stdout, '/Index Scan using t1_pk on t1/', 'set backend snapshot 1 t1 standby check' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) ); explain select id, name from t1 where id = 566;" );
like( $stdout, '/Seq Scan on t1/', 'set backend snapshot 2 t1 standby check' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot from pg_control_snapshot() ) ); explain select id, name from t1 where id = 566;" );
like( $stdout, '/Seq Scan on t1/', 'set backend snapshot 3 t1 standby check' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot from pg_control_snapshot() ) ); select id, name from t1 where id = 566;" );
ok( $stdout eq '', 'set backend snapshot 3 t1 truncate standby check' );

# t2
( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) ); select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
like( $stderr, '/ERROR:  relation "t2" does not exist/', 'set backend snapshot 1 t2 standby check' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) ); select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
like( $stdout, '/1000|500.5000000000000000|0566/', 'set backend snapshot 2 t2 standby check' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot from pg_control_snapshot() ) ); select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
like( $stdout, '/2000|1000.5000000000000000|0566_updated/', 'set backend snapshot 3 t2 standby check' );

# role
# Snapshot 1
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) ); select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );

ok( $ret_standby eq '', 'set backend snapshot 1 role standby check' );

# Snapshot 2
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot - 1 from pg_control_snapshot() ) ); select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );

like( $ret_standby, '/regression_testrole|f/', 'set backend snapshot 2 role standby check' );

# Snapshot 3
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select pg_set_backend_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) ); select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );

ok( $ret_standby eq '', 'set backend snapshot 3 role standby check' );

## recover to snapshot checks

# Snapshot 3
$node_master->safe_psql( 'postgres',
	"select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );" );
replay_wait( $node_master, $node_standby );

# t1
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout eq '0||', 'recover to snapshot 3 t1 master check' );
( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout_standby eq '0||', 'recover to snapshot 3 t1 standby check' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );
like( $stdout, '/Seq Scan on t1/', 'recover to snapshot 3 t1 primary key master check' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );
like( $stdout, '/Seq Scan on t1/', 'recover to snapshot 3 t1 primary key standby check' );

# t2
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
ok( $stdout eq '2000|1000.5000000000000000|0566_updated', 'recover to snapshot 3 t2 master check' );
( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
ok( $stdout_standby eq '2000|1000.5000000000000000|0566_updated', 'recover to snapshot 3 t2 standby check' );

# role
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );
ok( $ret_standby eq '', 'recover to snapshot 3 role standby check' );

# Snapshot 2
$node_master->safe_psql( 'postgres',
	"select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );" );
replay_wait( $node_master, $node_standby );

# t1
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout eq '1000|500.5000000000000000|0566', 'recover to snapshot 2 t1 master check' );
( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout_standby eq '1000|500.5000000000000000|0566', 'recover to snapshot 2 t1 standby check' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );
like( $stdout, '/Seq Scan on t1/', 'recover to snapshot 2 t1 primary key master check' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );
like( $stdout, '/Seq Scan on t1/', 'recover to snapshot 2 t1 primary key standby check' );

# t2
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
ok( $stdout eq '1000|500.5000000000000000|0566', 'recover to snapshot 2 t2 master check' );
( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
ok( $stdout_standby eq '1000|500.5000000000000000|0566', 'recover to snapshot 2 t2 standby check' );

# role
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );
ok( $ret_standby eq 'regression_testrole|f', 'recover to snapshot 2 role standby check' );

# Snapshot 1
$node_master->safe_psql( 'postgres',
	"select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );" );
replay_wait( $node_master, $node_standby );

# t1
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout eq '1000|500.5000000000000000|0566', 'recover to snapshot 1 t1 master check' );
( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout_standby eq '1000|500.5000000000000000|0566', 'recover to snapshot 1 t1 standby check' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );
like( $stdout, '/Index Scan using t1_pk on t1/', 'recover to snapshot 1 t1 primary key master check' );
( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres',
	"explain select id, name from t1 where id = 566;" );
like( $stdout, '/Index Scan using t1_pk on t1/', 'recover to snapshot 1 t1 primary key standby check' );

# t2
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
like( $stderr, '/ERROR:  relation "t2" does not exist/', 'recover to snapshot 1 t2 master check' );

( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t2 where id = 566 ) from t2;" );
like( $stderr_standby, '/ERROR:  relation "t2" does not exist/', 'recover to snapshot 1 t2 standby check' );

# role
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select rolname, rolcanlogin from pg_roles where rolname = 'regression_testrole';" );
ok( $ret_standby eq '', 'recover to snapshot 1 role standby check' );


done_testing();
