use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/..";
use Test::More;

use_ok('Koha::Plugin::imCode::KohaSS12000::ExportUsers::Leavers');
my $L = 'Koha::Plugin::imCode::KohaSS12000::ExportUsers::Leavers';

# A1, A2: with loans -> ASLUT, without loans -> AWAIT; B: with loans -> BSLUT, no loans: stays; C: only loans
my %map = (
    A1 => { loans => 'ASLUT', noloans => 'AWAIT' },
    A2 => { loans => 'ASLUT', noloans => 'AWAIT' },
    B  => { loans => 'BSLUT', noloans => undef },
);
sub d {
    my ($cat, $loans, $days, %o) = @_;
    my $prev = delete $o{prev};
    my $anon = delete $o{anonymized};
    return $L->can('decide')->({ category => $cat, previous_category => $prev, has_loans => $loans, days => $days, anonymized => $anon },
        \%map, { delete_after_days => 30, anonymize_after_days_loans => 300, %o });
}

# ---- with loans left
is(d('A1', 1, 0)->{action}, 'move', 'loans left: moved');
is(d('A1', 1, 0)->{to}, 'ASLUT', 'to the category for patrons with loans');
is(d('ASLUT', 1, 50, prev => 'A1')->{action}, 'keep', 'already there: kept');
is(d('A1', 1, 299)->{action}, 'move', 'with loans, 299 days: not removed yet');
is(d('ASLUT', 1, 299, prev => 'A1')->{action}, 'keep', '299 days in its left category: kept');
is(d('ASLUT', 1, 300, prev => 'A1')->{action}, 'anonymize', 'with loans after 300 days: the account is anonymized (it cannot be removed while loans are open)');
is(d('A1', 1, 400)->{action}, 'anonymize', 'also when it has not been moved yet');
is(d('ASLUT', 1, 400, prev => 'A1', anonymized => 1)->{action}, 'keep', 'an anonymized account with loans is kept until the loans are back');
is(d('A1', 1, 9999, anonymize_after_days_loans => undef)->{action}, 'move', 'no limit for loans (empty): never anonymized, however long ago');
is(d('ASLUT', 1, 9999, prev => 'A1', anonymize_after_days_loans => undef)->{action}, 'keep', 'no limit for loans: kept for ever');
is(d('ASLUT', 1, 9999, prev => 'A1', anonymize_after_days_loans => '')->{action}, 'keep', 'an empty string also means never');
is(d('ASLUT', 0, 31, prev => 'A1', anonymized => 1)->{action}, 'delete', 'an anonymized account whose loans are back is removed after the waiting time');

# ---- without loans
is(d('A1', 0, 0)->{action}, 'move', 'no loans: moved to the waiting category');
is(d('A1', 0, 0)->{to}, 'AWAIT', 'which is its own category');
is(d('AWAIT', 0, 10, prev => 'A1')->{action}, 'keep', 'in the waiting category: kept');
is(d('AWAIT', 0, 29, prev => 'A1')->{action}, 'keep', '29 days: kept');
is(d('AWAIT', 0, 30, prev => 'A1')->{action}, 'delete', 'no loans after 30 days: removed');
is(d('A1', 0, 30)->{action}, 'delete', 'a patron first seen long ago is removed directly');
is(d('A1', 0, 30, delete_after_days => 10)->{action}, 'delete', 'the number of days can be changed');
is(d('A1', 0, 9, delete_after_days => 10)->{action}, 'move', 'before the changed number of days');
is(d('A1', 0, 0, delete_after_days => 0)->{action}, 'delete', 'zero days removes at once');
is(d('B', 0, 3)->{action}, 'keep', 'no waiting category: stays in its own category');
is(d('B', 1, 3)->{to}, 'BSLUT', 'but moves when loans are left');
is(d('BSLUT', 0, 3, prev => 'B')->{action}, 'keep', 'loans returned: stays where it is when there is no waiting category');

# ---- the loans change over time
is(d('ASLUT', 0, 5, prev => 'A1')->{action}, 'move', 'loans returned: moves from the loans category to the waiting category');
is(d('ASLUT', 0, 5, prev => 'A1')->{to}, 'AWAIT', 'to the waiting category');
is(d('AWAIT', 1, 5, prev => 'A1')->{to}, 'ASLUT', 'a new loan: back to the loans category');
is(d('ASLUT', 0, 31, prev => 'A1')->{action}, 'delete', 'loans returned and 30 days have passed: removed');

# ---- patrons that are never touched
is(d('STAFF', 0, 999)->{action}, 'skip', 'a category that is not in the mapping is never touched');
is(d('STAFF', 1, 999)->{action}, 'skip', 'also not with loans');
is(d('ASLUT', 0, 999)->{action}, 'skip', 'a patron that was in a left category from the start is not touched');
is(d('', 0, 5)->{action}, 'skip', 'no category: skipped');
is($L->can('decide')->({ category => 'A1', has_loans => 1, days => 1 }, {}, {})->{action}, 'skip', 'empty mapping: nothing is touched');
is($L->can('decide')->({ category => 'A1', has_loans => 0, days => 30 }, \%map, undef)->{action}, 'delete', 'default is 30 days without loans');
is($L->can('decide')->({ category => 'A1', has_loans => 1, days => 9999 }, \%map, undef)->{action}, 'move', 'default for loans is never anonymize');

# ---- mapping validation
{
    my ($rows, $err) = $L->can('normalize_mapping')->([ { from => ' A1 ', to => 'ASLUT', to_noloans => ' AWAIT ' }, { from => 'A2', to => 'ASLUT' }, { from => '', to => '' } ]);
    is(scalar @$rows, 2, 'good rows are kept and empty rows ignored');
    is($rows->[0]{from}, 'A1', 'values are trimmed');
    is($rows->[0]{to_noloans}, 'AWAIT', 'the waiting category is trimmed');
    ok(!defined $rows->[1]{to_noloans}, 'an empty waiting category means it stays');
    is(scalar @$err, 0, 'no errors');

    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => 'X', to_noloans => 'A1' } ]);
    ok(!@$err && !defined $rows->[0]{to_noloans}, 'a waiting category equal to the own category means it stays');
    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => '' } ]);
    ok(@$err && $err->[0] =~ /needed/, 'a row without the loans category is an error');
    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => 'A1' } ]);
    ok(@$err && $err->[0] =~ /itself/, 'moving to itself is an error');
    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => 'X' }, { from => 'A1', to => 'Y' } ]);
    ok(@$err && $err->[0] =~ /twice/, 'the same source twice is an error');
    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => 'X' }, { from => 'X', to => 'Y' } ]);
    ok(@$err && $err->[0] =~ /target and also a source/, 'a chain A to X to Y is an error');
    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => 'X', to_noloans => 'A2' }, { from => 'A2', to => 'Y' } ]);
    ok(@$err && $err->[0] =~ /target and also a source/, 'a waiting category that is a source is an error');
    ($rows, $err) = $L->can('normalize_mapping')->('x');
    ok(@$err, 'not a list is an error');
}

# ---- mode
is($L->can('normalize_mode')->('apply'), 'apply', 'apply');
is($L->can('normalize_mode')->('report'), 'report', 'report');
is($L->can('normalize_mode')->('off'), 'off', 'off');
is($L->can('normalize_mode')->('delete everything'), 'off', 'unknown mode is off');
is($L->can('normalize_mode')->(undef), 'off', 'missing mode is off');

done_testing();
