package t::Users::Otaku;
use strict;
use warnings;
use parent qw/MongoSQL::Field::Default/;
use 5.010001;
use Data::Validator;
use Data::Clone;

my %OTAKU_MAP = (
    1 => 'AKB',
    2 => 'anime',
    3 => 'momoclo',
);
my %OTAKU_MAP_REVERSE = (reverse %OTAKU_MAP);

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
        column     => 'users.some_flg',
        enum       => [ values %OTAKU_MAP ],
    );

    return $self;
}

sub filter {
    my $self = shift;
    state $rule = Data::Validator->new(
        value => { isa => 'Int' },
    );
    my $args = $rule->validate(@_);
    $args->{value} = $OTAKU_MAP{$args->{value}};
    return $self->SUPER::filter($args);
}

sub make_table_data {
    my $self = shift;
    state $rule = Data::Validator->new(
        value => { isa => 'Str' },
    );
    my $args = $rule->validate(@_);
    my $value = $OTAKU_MAP_REVERSE{$args->{value}};
    return $self->SUPER::make_table_data(value => $value);
}

1;
