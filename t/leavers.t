use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/..";
use Test::More;

use_ok('Koha::Plugin::imCode::KohaSS12000::ExportUsers::Leavers');
my $L = 'Koha::Plugin::imCode::KohaSS12000::ExportUsers::Leavers';

my %map = (A1 => 'ASLUT', A2 => 'ASLUT', B => 'BSLUT');
sub d { my ($cat, $loans, $days, %o) = @_; return $L->can('decide')->({ category => $cat, has_loans => $loans, days => $days }, \%map, { delete_after_days => 7, %o }) }

# ---- decisions
is(d('A1', 1, 0)->{action}, 'move', 'loans left: moved to the left category');
is(d('A1', 1, 0)->{to}, 'ASLUT', 'to the mapped category');
is(d('A2', 1, 400)->{action}, 'move', 'loans left: still moved, never removed, however long ago');
is(d('A1', 0, 0)->{action}, 'move', 'no loans but not a leaver long enough: moved first');
is(d('A1', 0, 6)->{action}, 'move', 'six days is not seven');
is(d('A1', 0, 7)->{action}, 'delete', 'no loans after seven days: removed');
is(d('A1', 0, 7, delete_after_days => 30)->{action}, 'move', 'the number of days can be changed');
is(d('A1', 0, 30, delete_after_days => 30)->{action}, 'delete', 'removed after the changed number of days');
is(d('A1', 0, 0, delete_after_days => 0)->{action}, 'delete', 'zero days removes at once');
is(d('ASLUT', 1, 50)->{action}, 'keep', 'already in the left category with loans: kept');
is(d('ASLUT', 0, 3)->{action}, 'keep', 'already in the left category, no loans, too early: kept');
is(d('ASLUT', 0, 8)->{action}, 'delete', 'in the left category, loans returned, long enough: removed');
is(d('STAFF', 0, 999)->{action}, 'skip', 'a category that is not in the mapping is never touched');
is(d('STAFF', 1, 999)->{action}, 'skip', 'also not with loans');
is(d('', 0, 5)->{action}, 'skip', 'no category: skipped');
is($L->can('decide')->({ category => 'A1', has_loans => 0 }, \%map, {})->{action}, 'move', 'missing days count as zero');
is($L->can('decide')->({ category => 'A1', has_loans => 1, days => 1 }, {}, {})->{action}, 'skip', 'empty mapping: nothing is touched');
is($L->can('decide')->({ category => 'A1', has_loans => 0, days => 7 }, \%map, undef)->{action}, 'delete', 'default is seven days');

# ---- mapping validation
{
    my ($rows, $err) = $L->can('normalize_mapping')->([ { from => ' A1 ', to => 'ASLUT' }, { from => 'A2', to => 'ASLUT' }, { from => '', to => '' } ]);
    is(scalar @$rows, 2, 'good rows are kept and empty rows ignored');
    is($rows->[0]{from}, 'A1', 'values are trimmed');
    is(scalar @$err, 0, 'no errors');

    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => '' } ]);
    ok(@$err && $err->[0] =~ /both categories/, 'a half filled row is an error');
    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => 'A1' } ]);
    ok(@$err && $err->[0] =~ /itself/, 'moving to itself is an error');
    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => 'X' }, { from => 'A1', to => 'Y' } ]);
    ok(@$err && $err->[0] =~ /twice/, 'the same source twice is an error');
    ($rows, $err) = $L->can('normalize_mapping')->([ { from => 'A1', to => 'X' }, { from => 'X', to => 'Y' } ]);
    ok(@$err && $err->[0] =~ /target and also a source/, 'a chain A to X to Y is an error');
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
