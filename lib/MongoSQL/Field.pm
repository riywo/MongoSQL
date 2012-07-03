package MongoSQL::Field;
use strict;
use warnings;
use utf8;
use 5.010001;
use Class::Accessor::Lite (
    ro => [qw/field isa enum not_null read_only context handler/],
);
use Carp;
use Data::Validator;
use Data::Clone;
use Data::Dumper;
use Hash::Flatten qw/flatten unflatten/;

sub new {
    my $klass = shift;
    state $rule = Data::Validator->new(
        field         => { isa => 'Str' },
        isa           => { isa => 'Str'      , optional => 1 },
        enum          => { isa => 'ArrayRef' , optional => 1 },
        read_only     => { isa => 'Bool'     , optional => 1 },
        not_null      => { isa => 'Bool'     , optional => 1 },

        context       => { isa => 'Ref' },
        handler       => { isa => 'Object' },
    );
    my $args = clone $rule->validate(@_);
    my $self = bless $args, $klass;

    confess "invalid isa rule ".$self->isa
        if(defined $self->isa and $self->isa !~ /^(string|number|boolean|array)$/);
    $self->{context} = ${$self->{context}};
    return $self;
}

sub where { confess "not implemented " . (caller(0))[3] }
sub select_by_id { confess "not implemented " . (caller(0))[3] }
sub expr { return undef; }

sub filter {
    my $self = shift;
    state $rule = Data::Validator->new(
        value  => { isa => 'Defined' },
    );
    my $args = $rule->validate(@_);
    return $args->{value};
}

sub fields {
    my $self = shift;
    state $rule = Data::Validator->new(
        value  => { isa => 'Defined' },
        fields => { isa => 'Undef|ArrayRef' },
    );
    my $args = $rule->validate(@_);
    return $args->{value};
}

sub sort {
    my $self = shift;
    state $rule = Data::Validator->new(
        a     => { isa => 'Undef|Value|ArrayRef' },
        b     => { isa => 'Undef|Value|ArrayRef' },
        asc   => { isa => 'Bool', xor => ['desc'] },
        desc  => { isa => 'Bool', xor => ['asc'] },
    );
    my $args = $rule->validate(@_);
    my $a = $args->{a}; my $b = $args->{b};
    my $return = 0;

    $return = -1 unless defined $a;
    $return =  1 unless defined $b;
    if ($return == 0) {
        if ($self->isa eq 'string') {
            $return = $a cmp $b;
        } elsif ($self->isa eq 'number' or $self->isa eq 'boolean') {
            $return = $a <=> $b;
        }
    }

    return exists $args->{desc} ? $return * -1 : $return;
}

sub flatten_object {
    my $self = shift;
    state $rule = Data::Validator->new(
        value  => { isa => 'Defined' },
    );
    my $args = $rule->validate(@_);
    return $args->{value};
}

sub validate {
    my $self = shift;
    state $rule = Data::Validator->new(
        value     => { isa => 'Undef|Str|ArrayRef|HashRef' },
    );
    my $args = $rule->validate(@_);

    return $args->{value} if(!defined $self->isa and !$self->not_null); 

    state $isa = Data::Validator->new(
        boolean => { isa => 'Undef|Bool'    ,  xor => [qw/string number array/] },
        string  => { isa => 'Undef|Str'     ,  xor => [qw/boolean number array/] },
        number  => { isa => 'Undef|Num'     ,  xor => [qw/string boolean array/] },
        array   => { isa => 'Undef|ArrayRef',  xor => [qw/string boolean number/] },
    )->validate($self->isa => $args->{value});

    if (defined $self->enum and defined $args->{value}) {
        my @values = $self->isa eq 'array' ? @{$args->{value}} : ($args->{value});
        for my $value (@values) {
            confess "unmatch $value to ".join(',', @{$self->enum})
                if(scalar(grep { $_ eq $value } @{$self->enum}) == 0);
        }
    }

    if ($self->not_null) {
        confess $self->field." is not null" unless(defined $args->{value});
    }
}

sub make_table_data { confess "not implemented " . (caller(0))[3] }

sub make_object { confess "not implemented " . (caller(0))[3] }

#------------------------------------------------------------

sub _make_where_id {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id        => { isa => 'Str | ArrayRef[Str]' },
    );
    my $args = $rule->validate(@_);

    my $id_column = $self->context->{_id_col};
    my ($where, @bind);
    if (ref $args->{_id} eq 'ARRAY') {
        @bind = @{$args->{_id}};
        $where = "$id_column IN (" . join(',',map {'?'} @bind ) . ')';
    } else {
        $where = "$id_column = ?";
        @bind = ($args->{_id});
    }

    return ($where, @bind);
}


1;
