package MongoSQL::Simple;
use strict;
use warnings;
use utf8;
use 5.010001;
use parent qw/MongoSQL/;
use Data::Validator;
use Data::Clone;
use Data::Dumper;
use Carp;

sub insert_tables {
    my $self = shift;
    state $rule = Data::Validator->new(
        tables      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    my $tables = $args->{tables};
    my $id = $self->insert_table(table => $self->table, values => $tables->{$self->table});
    return $id;
}

sub update_tables {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id         => { isa => 'Str' },
        tables      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    my $tables = $args->{tables};
    $self->update_table(table => $self->table, set => $tables->{$self->table}, where => { $self->_id_col => $args->{_id} });
}

sub delete_tables {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id      => { isa => 'Str' },
    );
    my $args = $rule->validate(@_);
    $self->delete_table(table => $self->table, where => { $self->_id_col => $args->{_id} });
}

1;
