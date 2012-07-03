#!perl -w
use strict;
use Test::More;
use t::Util;
use t::Users;

reset_test_data();

my $users = t::Users->new;

subtest 'find' => sub {
pass;
};

my $id;
subtest 'insert' => sub {
    $id = $users->insert(object => {
        name    => 'sugyan',
        data1   => {
            todofuken => 'tokyo',
            age       => 10,
        },
        data2   => {
            toshi     => 'shibuya',
            otaku     => 'momoclo',
        },
        friends => [4],
    });

    is_deeply $users->find(query => { _id => $id }),
    [{
        _id     => $id,
        name    => 'sugyan',
        data1   => {
            todofuken => 'tokyo',
            age       => 10,
        },
        data2   => {
            toshi     => 'shibuya',
            otaku     => 'momoclo',
        },
        friends => [4],
    }];
};

subtest 'update' => sub {
    $users->update(
        _id => $id,
        query => {
            '$set'  => { 'data2.otaku' => 'AKB' },
            '$push' => { 'friends' => 1 },
        },
    );
    is_deeply $users->find(query => {_id => $id})->[0]->{friends},
    [1, 4];

    $users->update(
        _id => $id,
        query => {
            '$pull' => { 'friends' => 4 },
        },
    );
    is_deeply $users->find(query => {_id => $id})->[0]->{friends},
    [1];
};

subtest 'remove' => sub {
    $users->remove(_id => $id);
    is scalar @{$users->find()}, 4;
};

done_testing;
