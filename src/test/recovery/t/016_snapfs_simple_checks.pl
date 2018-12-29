# Simple checks
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 21;

# Wait until replay to complete
sub replay_wait( $$ ) {
	my $node_master = shift;
	my $node_standby = shift;
	my $until_lsn =
	  $node_master->safe_psql('postgres', "SELECT pg_current_wal_lsn()");
	
	$node_standby->poll_query_until('postgres',
		"SELECT (pg_last_wal_replay_lsn() - '$until_lsn'::pg_lsn) >= 0")
	  or die "standby never caught up";

	# the function does not work correctly
	$node_master->safe_psql( 'postgres', "select pg_sleep(1);" );
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

# Make third snapshot
$node_master->safe_psql('postgres',
	"select pg_make_snapshot();");

replay_wait( $node_master, $node_standby );

# Check for pg_control_snapshot() results
my $master_out = $node_master->safe_psql( 'postgres', "select * from pg_control_snapshot()" );
my $standby_out = $node_standby->safe_psql( 'postgres', "select * from pg_control_snapshot()" );

ok( $master_out eq '1|3|0', 'pg_control_snapshot() on master' );
ok( $standby_out eq '1|3|0', 'pg_control_snapshot() on standby' );

# Standby simple checks
( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_make_snapshot();" );
like( $stderr, '/ERROR:  Operation is not possible at replica/', 'pg_make_snapshot() is prohibited on standby' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_recover_to_snapshot( 3 );" );
like( $stderr, '/ERROR:  Operation is not possible at replica/', 'pg_recover_to_snapshot() is prohibited on standby' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_switch_to_snapshot( 2 );" );
like( $stderr, '/ERROR:  Operation is not possible at replica/', 'pg_switch_to_snapshot() is prohibited on standby' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select pg_set_backend_snapshot( 4 );" );
like( $stderr, '/ERROR:  Invalid snapshot/', 'Invalid snapshot number passed to pg_set_backend_snapshot() on standby' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select * from pg_set_backend_snapshot( 2 ); select * from pg_get_backend_snapshot(); select * from pg_set_backend_snapshot( 0 );" );
$stdout =~ s/\s//g;
is( $stdout, 2, 'pg_get_backend_snapshot() on standby' );

my $snapshot_size = $node_standby->safe_psql( 'postgres', "select pg_get_snapshot_size( 1 ) > 0" );
ok( $snapshot_size eq 't', 'Snapshot size is greater than 0 on standby' );

my $snapshot_timestamp = $node_standby->safe_psql( 'postgres', "select ( now() - pg_get_snapshot_timestamp( 1 ) ) > interval '0 seconds'" );
ok( $snapshot_timestamp eq 't', 'Snapshot age is greater than 0 on standby' );
$snapshot_timestamp = $node_standby->safe_psql( 'postgres', "select ( now() - pg_get_snapshot_timestamp( 1 ) ) < interval '180 seconds'" );
ok( $snapshot_timestamp eq 't', 'Snapshot age is less than 180 seconds on standby' );

( $ret, $stdout, $stderr ) = $node_standby->psql( 'postgres', "select coalesce( pg_get_snapshot_timestamp( snap_id ), now() ) = coalesce( snap_created, now() ) and pg_size_pretty( pg_get_snapshot_size( snap_id ) ) = snap_size from snapfs_snapshots;" );
$ret = () = $stdout =~ /t/g;
is( $ret, 3, 'snapfs_snapshots view check on standby' );

# Master simple checks
( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select pg_set_backend_snapshot( 4 );" );
like( $stderr, '/ERROR:  Invalid snapshot/', 'Invalid snapshot number passed to pg_set_backend_snapshot() on master' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select * from pg_set_backend_snapshot( 2 ); select * from pg_get_backend_snapshot(); select * from pg_set_backend_snapshot( 0 );" );
$stdout =~ s/\s//g;
is( $stdout, 2, 'pg_get_backend_snapshot() on master' );

$snapshot_size = $node_master->safe_psql( 'postgres', "select pg_get_snapshot_size( 1 ) > 0" );
ok( $snapshot_size eq 't', 'Snapshot size is greater than 0 on master' );

$snapshot_timestamp = $node_master->safe_psql( 'postgres', "select ( now() - pg_get_snapshot_timestamp( 1 ) ) > interval '0 seconds'" );
ok( $snapshot_timestamp eq 't', 'Snapshot age is greater than 0 on master' );
$snapshot_timestamp = $node_master->safe_psql( 'postgres', "select ( now() - pg_get_snapshot_timestamp( 1 ) ) < interval '180 seconds'" );
ok( $snapshot_timestamp eq 't', 'Snapshot age is less than 180 seconds on master' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select coalesce( pg_get_snapshot_timestamp( snap_id ), now() ) = coalesce( snap_created, now() ) and pg_size_pretty( pg_get_snapshot_size( snap_id ) ) = snap_size from snapfs_snapshots;" );
$ret = () = $stdout =~ /t/g;
is( $ret, 3, 'snapfs_snapshots view check on master' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select pg_switch_to_snapshot( 4 );" );
like( $stderr, '/ERROR:  Invalid snapshot \d+, existed snapshots/', 'Invalid snapshot number passed to pg_recover_to_snapshot() on master' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select pg_switch_to_snapshot( 2 );" );
ok( $ret eq 0, 'pg_switch_to_snapshot() on master' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select pg_recover_to_snapshot( 2 );" );
like( $stderr, '/ERROR:  Can not perform operation inside snapshot/', 'pg_recover_to_snapshot() is prohibited in snapshot on master' );

( $ret, $stdout, $stderr ) = $node_master->psql( 'postgres', "select pg_switch_to_snapshot( 0 );" );
$ret = $node_master->safe_psql( 'postgres', "select pg_recover_to_snapshot( 2 );" );
ok( $ret eq '', 'pg_recover_to_snapshot() on master' );
