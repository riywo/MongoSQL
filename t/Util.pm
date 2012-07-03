package t::Util;
use parent qw/Exporter/;
use Test::More 0.98;
use Test::mysqld;
use Test::Fixture::DBI;
our @EXPORT = qw(dbh);

{
    # utf8 hack.
    binmode Test::More->builder->$_, ":utf8" for qw/output failure_output todo_output/;
    no warnings 'redefine';
    my $code = \&Test::Builder::child;
    *Test::Builder::child = sub {
        my $builder = $code->(@_);
        binmode $builder->output,         ":utf8";
        binmode $builder->failure_output, ":utf8";
        binmode $builder->todo_output,    ":utf8";
        return $builder;
    };
}

my $MYSQLD;
{
    my $dsn = $ENV{TEST_DSN};
    unless (defined $dsn) {
        $MYSQLD = Test::mysqld->new(
            my_cnf => {
                "skip-networking" => ""
            }
        );
        $ENV{TEST_DSN} = $MYSQLD->dsn;

        my $dbh = dbh();
        construct_database(
            dbh      => $dbh,
            database => 't/schema.yaml',
        );

        construct_fixture(
            dbh     => $dbh,
            fixture => "t/fixture_$_.yaml",
        ) for (qw/users locations friends_map/);
    }
}

END {
    undef $MYSQLD;
}

sub dbh {
    my $dsn = $ENV{TEST_DSN};
    DBI->connect( $dsn, '', '', +{ AutoCommit => 0, RaiseError => 1, } );
}

1;
