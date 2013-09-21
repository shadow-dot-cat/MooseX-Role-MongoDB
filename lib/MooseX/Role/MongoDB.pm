use v5.10;
use strict;
use warnings;

package MooseX::Role::MongoDB;
# ABSTRACT: Provide MongoDB connections, databases and collections
# VERSION

use Moose::Role 2;
use MooseX::AttributeShortcuts;

use MongoDB::MongoClient 0.702;
use Type::Params qw/compile/;
use Types::Standard qw/:types/;
use namespace::autoclean;

#--------------------------------------------------------------------------#
# Public attributes and builders
#--------------------------------------------------------------------------#

=attr client_options

A hash reference of L<MongoDB::MongoClient> options that will be passed to its
C<connect> method.

=method _build_client_options

Returns an empty hash reference.  Override this to provide your own
defaults.

=cut

has client_options => (
    is  => 'lazy',
    isa => HashRef, # hashlike?
);

sub _build_client_options { return {} }

=attr default_database

The name of a MongoDB database to use as a default collection source if not
specifically requested.  Defaults to 'test'.  If set to anything other than
'test', it will also be a default for C<db_name> in C<client_options>, which
indicates the default database for authentication.

=cut

has default_database => (
    is  => 'lazy',
    isa => Str,
);

sub _build_default_database { return 'test' }

#--------------------------------------------------------------------------#
# Private attributes and builders
#--------------------------------------------------------------------------#

has _pid => (
    is      => 'rwp',     # private setter so we can update on fork
    isa     => 'Num',
    default => sub { $$ },
);

has _collection_cache => (
    is      => 'lazy',
    isa     => HashRef,
    clearer => 1,
);

sub _build__collection_cache { return {} }

has _database_cache => (
    is      => 'lazy',
    isa     => HashRef,
    clearer => 1,
);

sub _build__database_cache { return {} }

has _mongo_client => (
    is      => 'lazy',
    isa     => InstanceOf ['MongoDB::MongoClient'],
    clearer => 1,
);

sub _build__mongo_client {
    my ($self) = @_;
    return MongoDB::MongoClient->new( $self->client_options );
}

sub BUILD {
    my ($self) = @_;

    if ( $self->default_database ne 'test' ) {
        $self->client_options->{db_name} //= $self->default_database;
    }
}

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
    $database //= $self->default_database;
    $self->_check_pid;
    return $self->_database_cache->{$database} //=
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
      @args > 1 ? @args : ( $self->default_database, $args[0] );
    $self->_check_pid;
    return $self->_collection_cache->{$database}{$collection} //=
      $self->mongo_database($database)->get_collection($collection);
}

#--------------------------------------------------------------------------#
# Private methods
#--------------------------------------------------------------------------#

# check if we've forked and need to reconnect
sub _check_pid {
    my ($self) = @_;
    if ( $$ != $self->_pid ) {
        $self->_set__pid($$);
        $self->_clear_collection_cache;
        $self->_clear_database_cache;
        $self->_clear_mongo_client;
    }
    return;
}

1;

=for Pod::Coverage BUILD

=head1 SYNOPSIS

In your module:

    package MyClass;
    use Moose;
    with 'MooseX::Role::MongoDB';

In your code:

    my $obj = MyClass->new(
        default_database => 'MyDB',
        client_options  => {
            host => "mongodb://example.net:27017",
            username => "willywonka",
            password => "ilovechocolate",
        },
    );

    $obj_>mongo_database("test");                 # test database
    $obj->mongo_collection("books");              # in default database
    $obj->mongo_collection("otherdb" => "books"); # other database

=head1 DESCRIPTION

This role lets a class work with MongoDB.  It's major value is providing
fork-safety.  It caches all MongoDB objects and regenerates them on-demand
after a fork.

=usage

=head1 USAGE

Good luck!

=head1 SEE ALSO

=for :list
* L<MongoDB>

=cut

# vim: ts=4 sts=4 sw=4 et:
