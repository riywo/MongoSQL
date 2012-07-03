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

subtest 'insert' => sub {
    note explain $users->insert(object => {
        name    => 'sugyan',
        data1   => {
            todofuken => 'tokyo',
            age       => 10,
        },
        data2   => {
            toshi     => 'shibuya',
            otaku     => 'momoclo',
        },
        friends => [1, 4],
    });
note explain $users->find(query => { friends => { '$ne' => 4 } });
};

done_testing;
