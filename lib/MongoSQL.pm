package MongoSQL;
use strict;
use warnings;
use utf8;
use 5.010001;
use Class::Accessor::Lite (
    ro => [qw/schema table handler context/],
);
use Carp;
use Hash::Flatten qw/flatten unflatten/;
use MongoSQL::Field::Default;
use Module::Load;
use Data::Dumper;
use Data::Validator;
use Data::Clone;
use DBIx::Handler;
use List::MoreUtils qw/uniq any/;
use Hash::Merge qw/merge/;

our $VERSION = 0.01;

my $DEFAULT_FIELD = 'MongoSQL::Field::Default';

sub new {
    my $klass = shift;
    state $rule = Data::Validator->new(
        schema     => { isa => 'HashRef' },
        table      => { isa => 'Str' },
        dbi        => { isa => 'ArrayRef' },
    );
    my $args = $rule->validate(@_);
    my $self = bless clone($args), $klass;
    $self->{handler} = DBIx::Handler->connect(@{$args->{dbi}}) or confess $!;
    $self->{context} = +{};
    $self->{schema} = $self->_load_schema;
    return $self;
}

sub find {
    my $self = shift;
    state $rule = Data::Validator->new(
        query      => { isa => 'HashRef', default => +{} },
        fields     => { isa => 'ArrayRef[Str]', default => [] },
        sort       => { isa => 'ArrayRef[HashRef]', default => [] },
        limit      => { isa => 'Int', default => 0 },
        skip       => { isa => 'Int', default => 0 },
    );
    my $args = $rule->validate(@_);

    my $docs = $self->find_by_query(query => $args->{query});
    $docs = $self->sort(docs => $docs, sort => $args->{sort}, limit => $args->{limit}, skip => $args->{skip});
    $docs = $self->fields(docs => $docs, fields => $args->{fields});

    return $docs;
}

sub find_by_query {
    my $self = shift;
    state $rule = Data::Validator->new(
        query      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);

    my ($where, @bind) = $self->_where(query => $args->{query});
    my $id_expr = $self->field_obj('_id')->column." AS `_id`";
    my $ids = $self->handler->dbh->selectcol_arrayref(
        "SELECT DISTINCT $id_expr FROM ".$self->table." WHERE $where", {}, @bind);

    return [] if scalar @$ids == 0;
    my $rows = $self->find_by_id(_id => $ids);
    return $rows;
}

sub find_by_id {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id        => { isa => 'Undef | Str | ArrayRef[Str]' },
        for_update => { isa => 'Bool', default => 0 },
    );
    my $args = $rule->validate(@_);
    return [] unless defined $args->{_id};
    my $merge_rows = +{};

    for my $field ($self->_get_fields) {
        my $obj = $self->field_obj($field);
        my $rows = $obj->select_by_id($args); # { '_id1' => { field => { ... } }, ...}
        $merge_rows = merge $merge_rows, $rows if(defined $rows);
    }
    $merge_rows = merge $merge_rows, $self->_select_by_id($args);

    my $rows;
    for my $id (sort keys %$merge_rows) {
        my $row = $merge_rows->{$id};
        $row->{_id} = $id;
        push @$rows, $row;
    }

    return $self->_unflatten_rows(rows => $rows);
}

sub sort {
    my $self = shift;
    state $rule = Data::Validator->new(
        docs     => { isa => 'ArrayRef[HashRef]' },
        sort     => { isa => 'ArrayRef[HashRef]' },
        limit    => { isa => 'Int' },
        skip     => { isa => 'Int' },
    );
    my $args = $rule->validate(@_);

    my @sorted = sort { $self->_sort_fields($a, $b, $args->{sort}) } @{$args->{docs}}; 

    my $limit = $args->{limit} == 0 ? scalar @sorted : $args->{limit};
    my $start_idx = $args->{skip} > scalar @sorted ? scalar @sorted : $args->{skip};
    my $end_idx = $start_idx + $limit - 1 > $#sorted ? $#sorted : $start_idx + $limit - 1;

    my @results = @sorted[$start_idx .. $end_idx];
    return \@results;
}

sub fields {
    my $self = shift;
    state $rule = Data::Validator->new(
        docs   => { isa => 'ArrayRef[HashRef]' },
        fields => { isa => 'ArrayRef[Str]' },
    );
    my $args = $rule->validate(@_);
    return $args->{docs} if(scalar @{$args->{fields}} == 0);

    my $fields;
    for my $field (@{$args->{fields}}, '_id') {
        next if(any { $_ =~ /^$field\./ } @{$args->{fields}});
        my $field_obj = $self->field_obj($field);
        if (defined $field_obj) {
            $fields->{$field} = undef;
        } else {
            my @children = grep { $_ =~ /^$field\./ } $self->_get_fields;
            if (scalar @children > 0) {
                $fields->{$_} = undef for (@children);
            } else {
                my @keys = split /\./, $field;
                my @values;
                unshift @values, pop(@keys);
                while (scalar @keys > 0) {
                    $field = join '.', @keys;
                    if (defined $self->field_obj($field)) {
                        push @{$fields->{$field}}, join('.', @values);
                        last;
                    }
                    unshift @values, pop(@keys);
                }
            }
        }
    }

    my $filtered = [];
    push @$filtered, $self->_fields_single(doc => $_, fields => $fields)
        for (@{$args->{docs}});
    return $filtered;
}

sub validate {
    my $self = shift;
    state $rule = Data::Validator->new(
        object        => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);

    for my $field ($self->_get_fields) {
        my $field_obj = $self->field_obj($field);
        $field_obj->validate(value => $args->{object}->{$field});
    }
}

sub insert {
    my $self = shift;
    state $rule = Data::Validator->new(
        object      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);

    my $new_id;
    my $trx = $self->_trx_start;
    local $@;
    eval {
        my $query = { '$set' => $self->_flatten_object($args) };
        my $obj = $self->_make_object(query => $query);
        $self->validate(object => $obj);

        my $tables = $self->_make_table_data(object => $obj);
        $new_id = $self->insert_tables(tables => $tables);
    };
    $self->_trx_end($trx, $@);
    return $new_id;
}
sub insert_tables { confess "not implemented " . (caller(0))[3] }

sub update {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id        => { isa => 'Str' },
        query      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);

    my $trx = $self->_trx_start;
    local $@;
    eval {
        my $current = $self->find_by_id(_id => $args->{_id}, for_update => 1)->[0];
        confess "not found _id = $args->{_id}" unless defined $current;

        $current = $self->_flatten_object(object => $current);
        my $obj = $self->_make_object(query => $args->{query}, current => $current);
        $self->_check_read_only(object => $obj);

        my $new_obj = +{ %$current, %$obj };
        $self->validate(object => $new_obj);

        my $tables = $self->_make_table_data(object => $obj);
        $self->update_tables(_id => $args->{_id}, tables => $tables);
    };
    $self->_trx_end($trx, $@);

}
sub update_tables { confess "not implemented " . (caller(0))[3] }

sub remove {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id        => { isa => 'Str' },
    );
    my $args = $rule->validate(@_);

    my $trx = $self->_trx_start;
    local $@;
    eval {
        my $current = $self->find_by_id(_id => $args->{_id}, for_update => 1)->[0];
        confess "not found _id = $args->{_id}" unless defined $current;

        $current = $self->_flatten_object(object => $current);
        $self->validate_remove(_id => $args->{_id}, current => $current);

        $self->delete_tables(_id => $args->{_id});
    };
    $self->_trx_end($trx, $@);

}

sub validate_remove {
    my $self = shift;
    state $rule = Data::Validator->new(
        object      => { isa => 'HashRef' },
        current     => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    # do nothing
}
sub delete_tables { confess "not implemented " . (caller(0))[3] }

sub field_obj {
    my $self = shift;
    my $field = shift;
    return $self->schema->{$field} or confess "not found schema object of $field";
}

sub select_table {
    my $self = shift;
    state $rule = Data::Validator->new(
        table      => { isa => 'Str' },
        where      => { isa => 'HashRef[Value]' },
    );
    my $args = $rule->validate(@_);

    my (@where, @bind);
    for my $col (keys %{$args->{where}}) {
        push @where, "$col = ?";
        push @bind, $args->{where}->{$col};
    }

    my $where = join(' AND ', @where);
    return $self->handler->dbh->selectall_arrayref(
        "SELECT * FROM $args->{table} WHERE $where", +{ Slice => +{} }, @bind);
}

sub insert_table {
    my $self = shift;
    state $rule = Data::Validator->new(
        table      => { isa => 'Str' },
        values     => { isa => 'HashRef[Value|Undef]' },
    );
    my $args = $rule->validate(@_);
    my $values = $args->{values};
    my (@cols, @bind);
    for my $col (keys %$values) {
        push @cols, $col; push @bind, $values->{$col};
    }

    my $sql_cols = join(', ', @cols);
    my $sql_ph = join(', ', map {'?'} @cols);
    my $sth = $self->handler->dbh->prepare("INSERT INTO $args->{table} ($sql_cols) VALUES ($sql_ph)");
    $sth->execute(@bind) or confess $!;
    return $self->handler->dbh->{'mysql_insertid'};
}

sub update_table {
    my $self = shift;
    state $rule = Data::Validator->new(
        table      => { isa => 'Str' },
        set        => { isa => 'HashRef[Value|Undef]' },
        where      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    my $values = $args->{set};
    my (@sets, @bind);
    for my $col (keys %$values) {
        push @sets, "$col = ?"; push @bind, $values->{$col};
    }
    my @where;
    for my $col (keys %{$args->{where}}) {
        push @where, "$col = ?"; push @bind, $args->{where}->{$col};
    }

    my $set = join(', ', @sets);
    my $where = join(' AND ', @where);
    my $sth = $self->handler->dbh->prepare("UPDATE $args->{table} SET $set WHERE $where");
    $sth->execute(@bind) or confess $!;
}

sub delete_table {
    my $self = shift;
    state $rule = Data::Validator->new(
        table      => { isa => 'Str' },
        where      => { isa => 'HashRef[Value]' },
    );
    my $args = $rule->validate(@_);
    my (@where, @bind);
    for my $col (keys %{$args->{where}}) {
        push @where, "$col = ?"; push @bind, $args->{where}->{$col};
    }

    my $where = join(' AND ', @where);
    my $sth = $self->handler->dbh->prepare("DELETE FROM $args->{table} WHERE $where");
    $sth->execute(@bind) or confess $!;
}

#-----------------------------------------------

sub _trx_start {
    my $self = shift;
    my $trx = $self->handler->txn_scope;
    return $trx;
}

sub _trx_end {
    my $self = shift;
    my ($trx, $error) = @_;
    if ($error) {
        $trx->rollback;
        confess $error;
    }
    $trx->commit;
}

sub _load_schema {
    my $self = shift;
    state $rule = Data::Validator->new(
        column        => { isa => 'Str'      , xor => [qw/module/] },
        module        => { isa => 'Str'      , xor => [qw/column/] },
        isa           => { isa => 'Str'      , optional => 1 },
        enum          => { isa => 'ArrayRef' , optional => 1 },
        read_only     => { isa => 'Bool'     , optional => 1 },
        not_null      => { isa => 'Bool'     , optional => 1 },
    );

    my $schema = +{};
    for my $field (keys %{$self->schema}) {
        my $args = clone $rule->validate($self->schema->{$field});

        $args->{field}      = $field;
        $args->{context}    = \$self->context;
        $args->{handler}    = $self->handler;

        my $module = $DEFAULT_FIELD;
        if (exists $args->{module}) {
            $module = $args->{module};
            load $module;
            delete $args->{module};
        }
        $schema->{$field} = $module->new($args);
    }

    confess "error load schema" if(scalar keys %$schema == 0);
    confess "_id isn't set" unless(grep { $_ eq '_id' } keys %$schema);
    $self->context->{_id_col} = $schema->{_id}->column;
    return $schema;
}

sub _make_where_id {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id        => { isa => 'Str | ArrayRef[Str]' },
    );
    my $args = $rule->validate(@_);

    my $id_column = $self->field_obj('_id')->column;
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

sub _unflatten_rows {
    my $self = shift;
    state $rule = Data::Validator->new(
        rows       => { isa => 'ArrayRef[HashRef]' },
    );
    my $args = $rule->validate(@_);

    my $docs = [];
    for my $row (@{$args->{rows}}) {
        for my $field ($self->_get_fields) {
            if (defined $row->{$field}) {
                $row->{$field} = $self->field_obj($field)->filter(value => $row->{$field});
                delete $row->{$field} if(!defined $row->{$field});
            } else {
                delete $row->{$field}
            }
        }
        push @$docs, unflatten $row;
    }
    return $docs;
}

sub _flatten_object {
    my $self = shift;
    state $rule = Data::Validator->new(
        object      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    my $object = clone $args->{object};

    my $flatten = +{};
    for my $field ($self->_get_fields) {
        my $tmp = $object;
        my @keys = split /\./, $field;
        $tmp = $tmp->{$_} for (@keys);

        if (defined $tmp) {
            $flatten->{$field} = $self->field_obj($field)->flatten_object(value => $tmp);

            $tmp = $object;
            my $del = pop @keys;
            $tmp = $tmp->{$_} for (@keys);
            delete $tmp->{$del};
        }
    }
    confess "incorrect object ".Dumper($object) if(scalar(keys %{flatten $object}) > 0);
    return $flatten;
}

sub _get_fields {
    my $self = shift;
    return keys %{$self->schema};
}

sub _where {
    my $self = shift;
    state $rule = Data::Validator->new(
        query      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    my @where_and = ();
    my @bind = ();

    for my $field (keys %{$args->{query}}) {
        if ($field eq '$or') {
            my @where_or = ();
            for (@{$args->{query}->{$field}}) {
                my ($where, @params) = $self->_where(query => $_);
                push @where_or, $where;
                push @bind, @params;
            }
            push(@where_and, '(' . join(' OR ', @where_or). ')');
        } elsif ($field eq '$and') {
            for (@{$args->{query}->{$field}}) {
                my ($where, @params) = $self->_where(query => $_);
                push @where_and, $where;
                push @bind, @params;
            }
        } else {
            my $obj = $self->field_obj($field);
            my $value = $args->{query}->{$field};
            if (!defined $obj) {
                my @keys = split /\./, $field;
                my @value_keys;
                unshift @value_keys, pop(@keys);
                while (scalar @keys > 0) {
                    my $_field = join '.', @keys;
                    my $_value_key = join '.', @value_keys;
                    $obj = $self->field_obj($_field);
                    if (defined $obj) {
                        $value = unflatten { $_value_key => $value };
                        last;
                    }
                    unshift @value_keys, pop(@keys);
                }
                confess "can't find $field on schema" if(!defined $obj);
            }
            my ($where, @params) = $obj->where(value => $value);
            push @where_and, $where;
            push @bind, @params;
        }
    }

    my $where = scalar @where_and > 1 ? '(' . join(' AND ', @where_and) . ')'
              : (defined $where_and[0] and $where_and[0] ne '') ? $where_and[0]
              : '1 = 1';

    return ($where, @bind);    
}

sub _select_by_id {
    my $self = shift;
    state $rule = Data::Validator->new(
        _id        => { isa => 'Str | ArrayRef[Str]' },
        for_update => { isa => 'Bool', default => 0 },
    );
    my $args = $rule->validate(@_);
    my $for_update = $args->{for_update} ? 'FOR UPDATE' : '';

    my @exprs;
    for my $field ($self->_get_fields) {
        my $expr = $self->field_obj($field)->expr;
        next unless defined $expr;
        push @exprs, $expr;
    }
    my $expr = join(", ", @exprs);

    my ($where, @bind) = $self->_make_where_id(_id => $args->{_id});
    my $rows = $self->handler->dbh->selectall_hashref(
        "SELECT $expr FROM ".$self->table." WHERE $where $for_update",
        '_id', +{ Slice => 1 }, @bind
    );
    return $rows;
}

sub _fields_single {
    my $self = shift;
    state $rule = Data::Validator->new(
        doc    => { isa => 'HashRef' },
        fields => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);

    my $filtered = +{};
    for my $field (keys %{$args->{fields}}) {
        my $value = $args->{doc};
        my @keys = split /\./, $field;
        $value = $value->{$_} for (@keys);
        next unless defined $value;
        my $extra = $args->{fields}->{$field};
        $filtered->{$field} = $self->field_obj($field)->fields(value => $value, fields => $extra);
    }
    return unflatten $filtered;

}

sub _sort_fields {
    my $self = shift;
    my ($a, $b, $sort_arg) = @_;
    my $return = 0;
    for my $sort (@$sort_arg) {
        my ($field, $type) = %$sort;
        my $arg = $type == 1 ? { asc => 1 } : $type == -1 ? { desc => 1 }
                : confess "order bype must be 1 or -1";
        my @keys = split /\./, $field;
        my $_a = $a; my $_b = $b;
        $_a = $_a->{$_} for @keys;
        $_b = $_b->{$_} for @keys;
        $arg->{a} = $_a;
        $arg->{b} = $_b;
        $return = $self->field_obj($field)->sort($arg);
        last if $return != 0;
    }

    return $return;
}

sub _make_table_data {
    my $self = shift;
    state $rule = Data::Validator->new(
        object      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);

    my $object = $args->{object};
    my $table = +{};

    for my $field ($self->_get_fields) {
        my $field_obj = $self->field_obj($field);
        next unless(exists $object->{$field});
        my $t = $field_obj->make_table_data(value => $object->{$field});
        $table = merge $t, $table;
    }

    return $table;
}

sub _make_object {
    my $self = shift;
    state $rule = Data::Validator->new(
        query      => { isa => 'HashRef[HashRef]' },
        current    => { isa => 'HashRef', default => +{} },
    );
    my $args = $rule->validate(@_);
    my $obj = +{};
    for my $modifier (keys %{$args->{query}}) {
        for my $field (keys %{$args->{query}->{$modifier}}) {
            confess "can't use multiple modifier for single column $field"
                if (exists $obj->{$field});
            my $value = $self->field_obj($field)->flatten_object(value => $args->{query}->{$modifier}->{$field});
            $obj->{$field} = +{ $modifier => $value };
        }
    }

    for my $field (keys %$obj) {
        my $field_obj = $self->field_obj($field) or confess "$field not found";
        $obj->{$field} = $field_obj->make_object(query => $obj->{$field}, current => $args->{current}->{$field});
    }
    return $obj;
}

sub _check_read_only {
    my $self = shift;
    state $rule = Data::Validator->new(
        object      => { isa => 'HashRef' },
    );
    my $args = $rule->validate(@_);
    my $object = $args->{object};

    for my $field (keys %$object) {
        my $field_obj = $self->field_obj($field);
        confess $field_obj->field." is read only" if(defined $field_obj->read_only);
    }
}

1;
