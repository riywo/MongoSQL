#!perl -w
use strict;
use Test::More tests => 1;

BEGIN {
    use_ok 'MongoSQL';
}

diag "Testing MongoSQL/$MongoSQL::VERSION";
