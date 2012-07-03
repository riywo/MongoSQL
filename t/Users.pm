package t::Users;
use strict;
use warnings;
use 5.010001;
use parent qw/MongoSQL/;

sub new {
    my $klass = shift;
    my $self = $klass->SUPER::new(
        dbi    => [ $ENV{TEST_DSN}, '', '' ],
        schema => {
            '_id'               => { column => 'users.id'             , isa => 'number', read_only => 1 },
            'name'              => { column => 'users.nickname'       , isa => 'string', not_null => 1 },
            'data1.todofuken'   => { column => 'locations.prefecture' , isa => 'string', not_null => 1 },
            'data1.age'         => { column => 'users.age'            , isa => 'number', not_null => 1 },
            'data2.toshi'       => { column => 'locations.city'       , isa => 'string', not_null => 1 },
            'data2.otaku'       => { module => 't::Users::Otaku'      , isa => 'string' },
            'friends'           => { module => 't::Users::Friends'    , isa => 'array' },
        },
        table => 'users LEFT JOIN locations   ON users.location_id = locations.id',
    );
    return $self;
}

sub insert_tables {
    my $self = shift;
    state $rule = Data::Validator->new(
        tables      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    my $tables = $args->{tables};

    die if(!exists $tables->{locations});
    $tables->{users}->{location_id} = $self->_master_id('locations.id', $tables->{locations});
    my $id = $self->insert_table(table => 'users', values => $tables->{users});

    my @friends = @{$tables->{friends_map}};
    for my $row (@friends) {
        $row->{user_id} = $id;
        $self->insert_table(table => 'friends_map', values => $row);
     }
    return $id;
}

sub update_tables {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id         => { isa => 'Str' },
        tables      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    my $id = $args->{_id};
    my $tables = $args->{tables};

    $tables->{users}->{location_id} = $self->_master_id('locations.id', $tables->{locations})
        if exists $tables->{locations};
    $self->update_table(table => 'users', set => $tables->{users}, where => { id => $id });

    if (exists $tables->{friends_map}) {
        $self->delete_table(table => 'friends_map', where => { user_id => $id });
        my @friends = @{$tables->{friends_map}};
        for my $row (@friends) {
            $row->{user_id} = $id;
            $self->insert_table(table => 'friends_map', values => $row);
        }
    }
}

sub delete_tables {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id      => { isa => 'Str' },
    );
    my $args = $rule->validate(@_);
    my $id = $args->{_id};

    $self->delete_table(table => 'friends_map', where => { user_id => $id });
    $self->delete_table(table => 'users', where => { id => $id });
}

sub _master_id {
    my $self = shift;
    my $table_col = shift;
    my $where = shift;
    my ($table, $id_col) = split /\./, $table_col;

    my $master = $self->select_table(table => $table, where => $where);
    die "$table not found" unless defined $master;
    die "no master data for $table" if(scalar @$master == 0);
    die "more than one $table" if(scalar @$master > 1);

    return $master->[0]->{$id_col};
}

1;
