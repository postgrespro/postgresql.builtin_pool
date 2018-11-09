# Replication checks
use Data::Dumper;
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

	my $last_wal_replay_lsn =
	  $node_standby->safe_psql('postgres', "SELECT pg_last_wal_replay_lsn()");
	
	$node_standby->poll_query_until('postgres',
		"SELECT (pg_last_wal_replay_lsn() - '$until_lsn'::pg_lsn) >= 0")
	  or die "standby never caught up";

	print Dumper( $until_lsn );
	print Dumper( $last_wal_replay_lsn );
}

my ( $ret, $stdout, $stderr );
my ( $ret_standby, $stdout_standby, $stderr_standby );

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Make first snapshot
$node_master->safe_psql('postgres',
	"select pg_make_snapshot();");

# Create table
$node_master->safe_psql( 'postgres',
	"CREATE TABLE t1 AS SELECT generate_series as id, trim( to_char( generate_series, '0000' ) ) as \"name\" from generate_series(1, 1000)" );
$node_master->safe_psql( 'postgres',
	"alter table t1 add constraint t1_pk PRIMARY KEY ( id )" );

# Take backup
my $backup_name = 'my_backup';
$node_master->backup($backup_name);

# Create streaming standby from backup
my $node_standby = get_new_node('standby');

$node_standby->init_from_backup($node_master, $backup_name,
	has_streaming => 1);

$node_standby->start;

# Make second snapshot
$node_master->safe_psql( 'postgres',
	"select pg_make_snapshot();" );

$node_master->safe_psql( 'postgres',
	"insert into t1 SELECT generate_series as id, trim( to_char( generate_series, '0000' ) ) as \"name\" from generate_series(1001, 2000)" );

$node_master->safe_psql( 'postgres',
	"update t1 set name = name || '_updated' where id <= 1000" );

# Make third snapshot
$node_master->safe_psql( 'postgres',
	"select pg_make_snapshot();" );

$node_master->safe_psql( 'postgres',
	"delete from t1 where id > 1000;" );

replay_wait( $node_master, $node_standby );

$ret = $node_master->safe_psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
$ret_standby = $node_standby->safe_psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );

ok( $ret eq $ret_standby, 'initial check' );

# switch to snapshot check
$node_master->safe_psql( 'postgres',
	"select pg_switch_to_snapshot( ( select recent_snapshot - 2 from pg_control_snapshot() ) );" );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
like( $stderr, '/ERROR:  relation "t1" does not exist/', 'switch to snapshot 1 master check' );

( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout_standby eq '1000|500.5000000000000000|0566_updated', 'switch to snapshot 1 standby check' );

#teardown
$node_master->safe_psql( 'postgres',
	"select pg_switch_to_snapshot( 0 );" );

# recover to snapshot checks
$node_master->safe_psql( 'postgres',
	"select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );" );
replay_wait( $node_master, $node_standby );
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout eq '2000|1000.5000000000000000|0566_updated', 'recover to snapshot 1 master check' );
( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout_standby eq '2000|1000.5000000000000000|0566_updated', 'recover to snapshot 1 standby check' );

$node_master->safe_psql( 'postgres',
	"select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );" );
replay_wait( $node_master, $node_standby );
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout eq '1000|500.5000000000000000|0566', 'recover to snapshot 2 master check' );
( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
ok( $stdout_standby eq '1000|500.5000000000000000|0566', 'recover to snapshot 2 standby check' );

$node_master->safe_psql( 'postgres',
	"select pg_recover_to_snapshot( ( select recent_snapshot from pg_control_snapshot() ) );" );
replay_wait( $node_master, $node_standby );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
like( $stderr, '/ERROR:  relation "t1" does not exist/', 'recover to snapshot 3 master check' );

#$node_master->safe_psql( 'postgres',
#	"select pg_sleep( 10 );" );
#replay_wait( $node_master, $node_standby );

( $ret_standby, $stdout_standby, $stderr_standby ) = $node_standby->psql( 'postgres',
	"select count(*) as cnt, avg(id), ( select name from t1 where id = 566 ) from t1;" );
like( $stderr_standby, '/ERROR:  relation "t1" does not exist/', 'recover to snapshot 3 standby check' );

print Dumper( $ret );
print Dumper( $stdout );
print Dumper( $stderr );
print Dumper( $ret_standby );
print Dumper( $stdout_standby );
print Dumper( $stderr_standby );



#( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select pg_switch_to_snapshot( 0 );" );
#$ret = $node_master->safe_psql( 'postgres', "select pg_recover_to_snapshot( 2 );" );
#ok( $ret eq '', 'pg_recover_to_snapshot() on master' );

#print Dumper( $ret );
#print Dumper( $stdout );
#print Dumper( $stderr );


done_testing();
