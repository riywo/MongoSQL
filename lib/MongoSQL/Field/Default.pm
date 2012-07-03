package MongoSQL::Field::Default;
use strict;
use warnings;
use utf8;
use 5.010001;
use parent qw/MongoSQL::Field/;
use Carp;
use Class::Accessor::Lite (
    ro => [qw/column/],
);
use Data::Validator;
use Data::Clone;
use Data::Dumper;
use Hash::Flatten qw/flatten unflatten/;
use Scalar::Util qw/looks_like_number/;
use Encode;
use JSON;

sub new {
    my $klass = shift;
    state $rule = Data::Validator->new(
        column        => { isa => 'Str' },
        field         => { isa => 'Str' },
        isa           => { isa => 'Str'      , optional => 1 },
        enum          => { isa => 'ArrayRef' , optional => 1 },
        read_only     => { isa => 'Bool'     , optional => 1 },
        not_null      => { isa => 'Bool'     , optional => 1 },

        context       => { isa => 'Ref' },
        handler       => { isa => 'Object' },
    );
    my $args = clone $rule->validate(@_);
    my $column = $args->{column};
    delete $args->{column};
    my $self = $klass->SUPER::new($args);
    $self->{column} = $column;
    return $self;
}

sub where {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'HashRef | Str' },
    );
    my $args = $rule->validate(@_);

    my ($where, @params);
    if (ref $args->{value} eq 'HASH') {
        my ($op, $value) = (%{$args->{value}});
        $args->{value} = $value;
        if      ($op eq '$ne') {
            ($where, @params) = $self->_where_ne($args);
        } elsif ($op eq '$in') {
            ($where, @params) = $self->_where_in($args);
        } elsif ($op eq '$nin') {
            ($where, @params) = $self->_where_nin($args);
        } elsif ($op eq '$exists') {
            ($where, @params) = $self->_where_exists($args);
        } elsif ($op eq '$lt') {
            ($where, @params) = $self->_where_lt($args);
        } elsif ($op eq '$gt') {
            ($where, @params) = $self->_where_gt($args);
        } elsif ($op eq '$lte') {
            ($where, @params) = $self->_where_lte($args);
        } elsif ($op eq '$gte') {
            ($where, @params) = $self->_where_gte($args);
        } else {
            confess "unsupported query for ". $self->field ." => ". encode_json {$op => $value};
        }
    } else {
        ($where, @params) = $self->_where_eq($args);
    }

    return ($where, @params);
}

sub select_by_id { return undef; }

sub expr {
    my $self = shift;
    return $self->column.' AS `'.$self->field.'`';
}

sub filter {
    my $self = shift;
    state $rule = Data::Validator->new(
        value => { isa => 'Str | Undef' },
    );
    my $args = $rule->validate(@_);
    return $args->{value} unless defined $args->{value};
    return undef if $args->{value} eq '';

    my $value = $args->{value};
    $value = decode_utf8($value) if !Encode::is_utf8($value);
    $value = $value + 0 if looks_like_number($value);
    return $value;
}

sub make_table_data {
    my $self = shift;
    state $rule = Data::Validator->new(
        value => { isa => 'Str|Undef' },
    );
    my $args = $rule->validate(@_);
    my $value = defined $args->{value} ? $args->{value} : undef;
    return unflatten { $self->column => $value };
}

sub make_object {
    my $self = shift;
    state $rule = Data::Validator->new(
        query   => { isa => 'HashRef' },
        current => { isa => 'Undef|Str|ArrayRef|HashRef' },
    );
    my $args = $rule->validate(@_);

    my $obj;
    my ($modifier, $value) = %{$args->{query}};
    if      ($modifier eq '$set') {
        $obj = $self->_update_set(value => $value);
    } elsif ($modifier eq '$unset') {
        $obj = $self->_update_unset(value => $value);
    } elsif ($modifier eq '$push') {
        $obj = $self->_update_push(value => $value, current => $args->{current});
    } elsif ($modifier eq '$pull') {
        $obj = $self->_update_pull(value => $value, current => $args->{current});
    } else {
        confess "unsupported modifier $modifier";
    }
    return $obj;
}

#--------------------------------------------------

sub _where_eq {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Str' },
    );
    my $args = $rule->validate(@_);
    return ($self->column." = ?", $args->{value});
}

sub _where_ne {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Str' },
    );
    my $args = $rule->validate(@_);
    return ($self->column." != ?", $args->{value});
}

sub _where_in {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'ArrayRef' },
    );
    my $args = $rule->validate(@_);
    my @in = @{$args->{value}};
    return ($self->column." IN (" . join(", ", map {'?'} @in) . ")", @in);
}

sub _where_nin {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'ArrayRef' },
    );
    my $args = $rule->validate(@_);
    my @in = @{$args->{value}};
    my $column = $self->column;
    return ("($column IS NULL OR $column NOT IN (" . join(", ", map {'?'} @in) . "))", @in);
}

sub _where_exists {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Bool' },
    );
    my $args = $rule->validate(@_);
    return ($self->column." IS NULL")     if $args->{value} == 0;
    return ($self->column." IS NOT NULL") if $args->{value} == 1;
}

sub _where_lt {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Num' },
    );
    my $args = $rule->validate(@_);
    return ($self->column." < ?", $args->{value});
}

sub _where_gt {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Num' },
    );
    my $args = $rule->validate(@_);
    return ($self->column." > ?", $args->{value});
}

sub _where_lte {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Num' },
    );
    my $args = $rule->validate(@_);
    return ($self->column." <= ?", $args->{value});
}

sub _where_gte {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Num' },
    );
    my $args = $rule->validate(@_);
    return ($self->column." >= ?", $args->{value});
}

sub _update_set {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Str' },
    );
    my $args = $rule->validate(@_);
    return $args->{value};
}

sub _update_unset {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Str' },
    );
    my $args = $rule->validate(@_);
    return undef;
}

sub _update_push { confess "not implemented " . (caller(0))[3] }
sub _update_pull { confess "not implemented " . (caller(0))[3] }

1;
