#!perl -w
use strict;
use Test::More;
use t::Util;

use MongoSQL::Simple;

reset_test_data();

my $locations = MongoSQL::Simple->new(
    schema => {
        '_id'         => { column => 'locations.id'         , isa => 'number', read_only => 1 },
        'todofuken'   => { column => 'locations.prefecture' , isa => 'string', not_null => 1 },
        'toshi'       => { column => 'locations.city'       , isa => 'string', not_null => 1 },
    },
    table  => 'locations',
    dbi    => [ $ENV{TEST_DSN}, '', '' ],
);

subtest 'find' => sub {
    is_deeply $locations->find(),
    [
        {
            '_id' => 1,
            'todofuken' => 'tokyo',
            'toshi' => 'shibuya'
        },
        {
            '_id' => 2,
            'todofuken' => 'tokyo',
            'toshi' => 'roppongi'
        },
        {
            '_id' => 3,
            'todofuken' => 'kanagawa',
            'toshi' => 'uminomukou'
        }
    ];

    is_deeply $locations->find(
        query => { todofuken => 'tokyo' },
    ),
    [
        {
            '_id' => 1,
            'todofuken' => 'tokyo',
            'toshi' => 'shibuya'
        },
        {
            '_id' => 2,
            'todofuken' => 'tokyo',
            'toshi' => 'roppongi'
        }
    ];
};

subtest 'find fields' => sub {
    is_deeply $locations->find(
        query  => { _id => 1 },
        fields => [qw/todofuken/],
    ),
    [
        {
            '_id' => 1,
            'todofuken' => 'tokyo',
        }
    ];
};

subtest 'find sort' => sub {
    is_deeply $locations->find(
        fields => [qw/_id/],
        sort   => [{ todofuken => -1 }, { _id => -1 }],
    ),
    [
        { _id => 2 },
        { _id => 1 },
        { _id => 3 },
    ];
};

done_testing;
