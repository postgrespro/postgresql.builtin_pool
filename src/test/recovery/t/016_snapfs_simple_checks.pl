# Simple checks
#use Data::Dumper;
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
}

my ( $ret, $stdout, $stderr );

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Make first snapshot
$node_master->safe_psql('postgres',
	"select pg_make_snapshot();");

# And some content
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT generate_series(1, 10) AS a");

# Take backup
my $backup_name = 'my_backup';
$node_master->backup($backup_name);

# Create streaming standby from backup
my $node_standby = get_new_node('standby');

$node_standby->init_from_backup($node_master, $backup_name,
	has_streaming => 1);

$node_standby->start;

# Make second snapshot
$node_master->safe_psql('postgres',
	"select pg_make_snapshot();");

replay_wait( $node_master, $node_standby );

# Check for pg_control_snapshot() results
my ( $master_out, $standby_out );
$master_out = $node_master->safe_psql( 'postgres', "select * from pg_control_snapshot()" );
$standby_out = $node_standby->safe_psql( 'postgres', "select * from pg_control_snapshot()" );

ok( $master_out eq '1|2|0', 'pg_control_snapshot() on master' );
ok( $standby_out eq '1|1|0', 'pg_control_snapshot() on standby' );

# Make third snapshot
$node_master->safe_psql('postgres',
	"select pg_make_snapshot();");

replay_wait( $node_master, $node_standby );

# Check for pg_control_snapshot() results another one
$master_out = $node_master->safe_psql( 'postgres', "select * from pg_control_snapshot()" );
$standby_out = $node_standby->safe_psql( 'postgres', "select * from pg_control_snapshot()" );

ok( $master_out eq '1|3|0', 'pg_control_snapshot() on master 2' );
ok( $standby_out eq '1|2|0', 'pg_control_snapshot() on standby 2' );

# Standby checks
( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_recover_to_snapshot( 3 );" );
like( $stderr, '/ERROR:  Operation is not possible at replica/', 'pg_recover_to_snapshot() is prohibited on replica' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_make_snapshot();" );
like( $stderr, '/ERROR:  Operation is not possible at replica/', 'pg_make_snapshot() is prohibited on replica' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_switch_to_snapshot( 2 );" );
like( $stderr, '/ERROR:  Operation is not possible at replica/', 'pg_switch_to_snapshot() is prohibited on replica' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_set_backend_snapshot( 3 );" );
like( $stderr, '/ERROR:  Invalid snapshot/', 'Invalid snapshot number passed to pg_set_backend_snapshot() on standby' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_set_backend_snapshot( 2 );" );
is( $ret, 0, 'Standby pg_set_backend_snapshot()' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_set_backend_snapshot( 2 ); select pg_get_backend_snapshot();" );

$stdout =~ s/\s//;

is( $stdout, 2, 'Standby pg_get_backend_snapshot()' );

# Master checks

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_set_backend_snapshot( 3 );" );
like( $stderr, '/ERROR:  Invalid snapshot/', 'Invalid snapshot number passed to pg_set_backend_snapshot() on master' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select pg_set_backend_snapshot( 2 );" );
is( $ret, 0, 'Master pg_set_backend_snapshot()' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select pg_set_backend_snapshot( 2 ); select pg_get_backend_snapshot();" );

$stdout =~ s/\s//;

is( $stdout, 2, 'Master pg_get_backend_snapshot()' );


#print Dumper( $ret );
#print Dumper( $stdout );
#print Dumper( $stderr );


done_testing();
