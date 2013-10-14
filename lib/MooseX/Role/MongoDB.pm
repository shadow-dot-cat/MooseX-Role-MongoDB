use v5.10;
use strict;
use warnings;

package MooseX::Role::MongoDB;
# ABSTRACT: Provide MongoDB connections, databases and collections
# VERSION

use Moose::Role 2;
use MooseX::AttributeShortcuts;

use Log::Any;
use MongoDB::MongoClient 0.702;
use String::Flogger qw/flog/;
use Type::Params qw/compile/;
use Types::Standard qw/:types/;
use namespace::autoclean;

#--------------------------------------------------------------------------#
# Configuration attributes
#--------------------------------------------------------------------------#

has _mongo_client_class => (
    is  => 'lazy',
    isa => 'Str',
);

sub _build__mongo_client_class { return 'MongoDB::MongoClient' }

has _mongo_client_options => (
    is  => 'lazy',
    isa => HashRef, # hashlike?
);

sub _build__mongo_client_options { return {} }

has _mongo_default_database => (
    is  => 'lazy',
    isa => Str,
);

sub _build__mongo_default_database { return 'test' }

#--------------------------------------------------------------------------#
# Caching attributes
#--------------------------------------------------------------------------#

has _mongo_pid => (
    is      => 'rwp',     # private setter so we can update on fork
    isa     => 'Num',
    default => sub { $$ },
);

has _mongo_client => (
    is      => 'lazy',
    isa     => InstanceOf ['MongoDB::MongoClient'],
    clearer => 1,
);

sub _build__mongo_client {
    my ($self) = @_;
    my $options = { %{ $self->_mongo_client_options } };
    $options->{db_name} //= $self->_mongo_default_database;
    $self->_mongo_log( debug => "connecting to MongoDB with %s", $options );
    return MongoDB::MongoClient->new($options);
}

has _mongo_database_cache => (
    is      => 'lazy',
    isa     => HashRef,
    clearer => 1,
);

sub _build__mongo_database_cache { return {} }

has _mongo_collection_cache => (
    is      => 'lazy',
    isa     => HashRef,
    clearer => 1,
);

sub _build__mongo_collection_cache { return {} }

#--------------------------------------------------------------------------#
# Logging attribute
#--------------------------------------------------------------------------#

# XXX eventually, isa will be Log::Any::Proxy, but that hasn't shipped yet
has _mongo_logger => (
    is      => 'ro',
    isa     => 'Object',
    default => sub { Log::Any->get_logger },
);

#--------------------------------------------------------------------------#
# Public methods
#--------------------------------------------------------------------------#

=method mongo_database

    $obj->mongo_database( $database_name );

Returns a L<MongoDB::Database>.  The argument is the database name.

=cut

sub mongo_database {
    state $check = compile( Object, Optional [Str] );
    my ( $self, $database ) = $check->(@_);
    $database //= $self->_mongo_default_database;
    $self->_mongo_check_pid;
    $self->_mongo_log( debug => "retrieving database $database" );
    return $self->_mongo_database_cache->{$database} //=
      $self->_mongo_client->get_database($database);
}

=method mongo_collection

    $obj->mongo_collection( $database_name, $collection_name );
    $obj->mongo_collection( $collection_name );

Returns a L<MongoDB::Collection>.  With two arguments, the first argument is
the database name and the second is the collection name.  With a single
argument, the argument is the collection name from the default database name.

=cut

sub mongo_collection {
    state $check = compile( Object, Str, Optional [Str] );
    my ( $self, @args ) = $check->(@_);
    my ( $database, $collection ) =
      @args > 1 ? @args : ( $self->_mongo_default_database, $args[0] );
    $self->_mongo_check_pid;
    $self->_mongo_log( debug => "retrieving collection $database.$collection" );
    return $self->_mongo_collection_cache->{$database}{$collection} //=
      $self->mongo_database($database)->get_collection($collection);
}

#--------------------------------------------------------------------------#
# Private methods
#--------------------------------------------------------------------------#

# check if we've forked and need to reconnect
sub _mongo_check_pid {
    my ($self) = @_;
    if ( $$ != $self->_mongo_pid ) {
        $self->_mongo_log( debug => "clearing MongoDB caches" );
        $self->_set__mongo_pid($$);
        $self->_clear_mongo_collection_cache;
        $self->_clear_mongo_database_cache;
        $self->_clear_mongo_client;
    }
    return;
}

sub _mongo_log {
    my ( $self, $level, @msg ) = @_;
    $msg[0] = "$self ($$) $msg[0]";
    $self->_mongo_logger->$level( flog( [@msg] ) );
}

1;

=for Pod::Coverage BUILD

=head1 SYNOPSIS

In your module:

    package MyData;
    use Moose;
    with 'MooseX::Role::MongoDB';

    has database => (
        is       => 'ro',
        isa      => 'Str',
        required => 1,
    );

    has client_options => (
        is       => 'ro',
        isa      => 'HashRef',
        default  => sub { {} },
    );

    sub _build__mongo_default_database { return $_[0]->database }
    sub _build__mongo_client_options   { return $_[0]->client_options }

In your code:

    my $obj = MyData->new(
        database => 'MyDB',
        client_options  => {
            host => "mongodb://example.net:27017",
            username => "willywonka",
            password => "ilovechocolate",
        },
    );

    $obj->mongo_database("test");                 # test database
    $obj->mongo_collection("books");              # in default database
    $obj->mongo_collection("otherdb" => "books"); # in other database

=head1 DESCRIPTION

This role helps create and manage L<MongoDB> objects.  All MongoDB objects will
be generated lazily on demand and cached.  The role manages a single
L<MongoDB::MongoClient> connection, but many L<MongoDB::Database> and
L<MongoDB::Collection> objects.

The role also compensates for forks.  If a fork is detected, the object caches
are cleared and new connections and objects will be generated in the new
process.

When using this role, you should not hold onto MongoDB objects for long if
there is a chance of your code forking.  Instead, request them again
each time you need them.

=head1 CONFIGURING

The role uses several private attributes to configure itself:

=for :list
* C<_mongo_client_class> — name of the client class
* C<_mongo_client_options> — passed to client constructor
* C<_mongo_default_database> — default name used if not specified

Each of these have lazy builders that you can override in your class to
customize behavior of the role.

The builders are:

=for :list
* C<_build__mongo_client_class> — default is C<MongoDB::MongoClient>
* C<_build__mongo_client_options> — default is an empty hash reference
* C<_build__mongo_default_database> — default is the string 'test'

You will generally want to at least override C<_build__mongo_client_options> to
allow connecting to different hosts.  You may want to set it explicitly or you
may want to have your own public attribute for users to set (as shown in the
L</SYNOPSIS>).  The choice is up to you.

Note that the C<_mongo_default_database> is also used as the default database for
authentication, unless a C<db_name> is provided to C<_mongo_client_options>.

=head1 SEE ALSO

=for :list
* L<Moose>
* L<MongoDB>

=cut

# vim: ts=4 sts=4 sw=4 et:
