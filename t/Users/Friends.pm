package t::Users::Friends;
use strict;
use warnings;
use parent qw/MongoSQL::Field::Default/;
use 5.010001;
use Class::Accessor::Lite (
    ro => [qw/table/],
);
use Data::Validator;
use Data::Clone;
use Carp;

sub new {
    my $klass = shift;
    state $rule = Data::Validator->new(
        field         => { isa => 'Str' },
        isa           => { isa => 'Str'      , optional => 1 },
        read_only     => { isa => 'Bool'     , optional => 1 },
        not_null      => { isa => 'Bool'     , optional => 1 },

        context       => { isa => 'Ref' },
        handler       => { isa => 'Object' },
    );
    my $args = clone $rule->validate(@_);
    my $self = $klass->SUPER::new(%$args,
        column     => 'friends_map.friend_id',
    );
    $self->{table} = "users LEFT JOIN friends_map ON users.id = friends_map.user_id";

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
#        } elsif ($op eq '$nin') {
#            ($where, @params) = $self->_where_nin($args);
        } elsif ($op eq '$exists') {
            ($where, @params) = $self->_where_exists($args);
        } else {
            confess "unsupported query for ". $self->field ." => ". encode_json {$op => $value};
        }
    } else {
        ($where, @params) = $self->_where_eq($args);
    }

    my $id_expr = $self->context->{_id_col}." AS `_id`";
    my $ids = $self->handler->dbh->selectcol_arrayref(
        "SELECT DISTINCT $id_expr FROM ".$self->table." WHERE $where", {}, @params);
    return $self->_make_where_id(_id => $ids);
}

sub select_by_id {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id        => { isa => 'Str | ArrayRef[Str]' },
        for_update => { isa => 'Bool', default => 0 },
    );
    my $args = $rule->validate(@_);
    my $for_update = $args->{for_update} ? 'FOR UPDATE' : '';

    my $expr = "friends_map.user_id, GROUP_CONCAT(friends_map.friend_id) AS friend_id";
    my ($where, @bind) = $self->_make_where_id(_id => $args->{_id});
    my $rows = $self->handler->dbh->selectall_arrayref(
        "SELECT $expr FROM ".$self->table." WHERE $where GROUP BY user_id $for_update", { Slice => +{} }, @bind);

    my $merge_rows = +{};
    for my $row (@$rows) {
        next unless defined $row->{user_id};
        $merge_rows->{$row->{user_id}}->{$self->field} = [ map {int $_} split /,/, $row->{friend_id} ];
    }
    return $merge_rows;
}

sub expr { return undef; }
sub filter {
    my $self = shift;
    state $rule = Data::Validator->new(
        value  => { isa => 'Defined' },
    );
    my $args = $rule->validate(@_);
    return $args->{value};
}

sub make_table_data {
    my $self = shift;
    state $rule = Data::Validator->new(
        value => { isa => 'ArrayRef[Int]|Undef' },
    );
    my $args = $rule->validate(@_);
    my @friends = ();
    for my $friend_id (@{$args->{value}}) {
        push @friends, { friend_id => $friend_id };
    }
    return { friends_map => \@friends };
}



sub _where_eq {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Int|ArrayRef[Int]' },
    );
    my $args = $rule->validate(@_);
    if (ref $args->{value} eq 'ARRAY') {
        
    } else {
        return ($self->column." = ?", $args->{value});
    }
}

sub _where_ne {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Int|ArrayRef[Int]' },
    );
    my $args = $rule->validate(@_);
    if (ref $args->{value} eq 'ARRAY') {
        
    } else {
        return ("user_id NOT IN (SELECT user_id FROM friends_map WHERE friend_id = ?) AND ".$self->column." != ?", $args->{value}, $args->{value});
    }
}





sub _update_set {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'ArrayRef[Int]' },
    );
    my $args = $rule->validate(@_);
    return $args->{value};
}

sub _update_push {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Int' },
        current    => { isa => 'Undef | ArrayRef[Int]' },
    );
    my $args = $rule->validate(@_);

    my @friends = defined $args->{current} ? @{$args->{current}} : ();
    push @friends, $args->{value} unless(grep { $_ eq $args->{value} } @friends);
    return \@friends;
}

sub _update_pull {
    my $self = shift;
    state $rule = Data::Validator->new(
        value      => { isa => 'Int' },
        current    => { isa => 'Undef | ArrayRef[Int]' },
    );
    my $args = $rule->validate(@_);

    my @friends = defined $args->{current} ? @{$args->{current}} : ();
    @friends = grep { $_ ne $args->{value} } @friends;
    return \@friends;
}

1;
