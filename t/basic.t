use strict;
use warnings;
use Test::More 0.96;
use Test::FailWarnings;
use Test::Requires qw/MongoDB::MongoClient/;

use Config;
use Parallel::Iterator qw/iterate/;

plan skip_all => "Requires forking" unless $Config{d_fork};

#--------------------------------------------------------------------------#
# Fixtures
#--------------------------------------------------------------------------#

my $coll_name = "moose_role_mongodb_test";

my $conn = eval { MongoDB::MongoClient->new }
  or plan skip_all => "No MongoDB on localhost";

$conn->get_database("test")->get_collection($coll_name)->drop;

{

    package MongoManager;
    use Moose;
    with 'MooseX::Role::MongoDB';
}

#--------------------------------------------------------------------------#
# Tests
#--------------------------------------------------------------------------#

my $mgr;

$mgr = new_ok( 'MongoManager' => [ mongo_default_database => "test2" ] );

$mgr = new_ok('MongoManager');

isa_ok( $mgr->mongo_database, "MongoDB::Database" );

isa_ok( $mgr->mongo_database("test2"), "MongoDB::Database" );

isa_ok( $mgr->mongo_collection( test => $coll_name ), "MongoDB::Collection" );

ok( $mgr->mongo_collection($coll_name)->insert( { job => '-1', 'when' => time } ),
    "insert before fork" );

my $num_forks = 3;

my $iter = iterate(
    sub {
        my ( $id, $job ) = @_;
        $mgr->mongo_collection($coll_name)->insert( { job => $job, 'when' => time } );
        return {
            pid        => $$,
            cached_pid => $mgr->_mongo_pid,
        };
    },
    [ 1 .. $num_forks ],
);

while ( my ( $index, $value ) = $iter->() ) {
    isnt( $value->{cached_pid}, $$, "child $index updated cached pid" )
      or diag explain $value;
}

is(
    $mgr->mongo_collection($coll_name)->count,
    $num_forks + 1,
    "children created $num_forks objects"
);

done_testing;
# COPYRIGHT
# vim: ts=4 sts=4 sw=4 et:
