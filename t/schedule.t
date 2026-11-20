use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/..";
use POSIX qw(mktime strftime);
use Test::More;

use_ok('Koha::Plugin::imCode::KohaSS12000::ExportUsers::Schedule');
my $S = 'Koha::Plugin::imCode::KohaSS12000::ExportUsers::Schedule';

sub at { my ($y, $m, $d, $h, $mi) = @_; return mktime(0, $mi, $h, $d, $m - 1, $y - 1900) }

# 2026-11-23 is a Monday
is($S->can('weekday_of')->(at(2026, 11, 23, 12, 0)), 1, 'Monday is 1');
is($S->can('weekday_of')->(at(2026, 11, 28, 12, 0)), 6, 'Saturday is 6');
is($S->can('weekday_of')->(at(2026, 11, 29, 12, 0)), 7, 'Sunday is 7');

# ---- validation
{
    my ($e, $err) = $S->can('normalize_entries')->([
        { weekday => 1, time => '8:00' },
        { weekday => '2', time => '20:00', job_type => 'import' },
        { weekday => 6, time => '13:00' },
        { weekday => 1, time => '08:00' },
    ]);
    is(scalar @$err, 0, 'good rows give no errors');
    is_deeply([ map { "$_->{weekday} $_->{time} $_->{job_type}" } @$e ], [ '1 08:00 import', '2 20:00 import', '6 13:00 import' ], 'time is normalised, rows are sorted and duplicates removed');

    ($e, $err) = $S->can('normalize_entries')->([ { weekday => 0, time => '08:00' }, { weekday => 8, time => '08:00' }, { weekday => 'mon', time => '08:00' } ]);
    is(scalar @$e, 0, 'bad weekdays are rejected');
    is(scalar @$err, 3, 'one error per bad row');

    ($e, $err) = $S->can('normalize_entries')->([ { weekday => 1, time => '24:00' }, { weekday => 1, time => '8.00' }, { weekday => 1, time => '08:60' }, { weekday => 1 } ]);
    is(scalar @$e, 0, 'bad times are rejected');

    ($e, $err) = $S->can('normalize_entries')->([ { weekday => 1, time => '08:00', job_type => 'drop_tables' } ]);
    ok(!@$e && $err->[0] =~ /unknown job type/, 'unknown job types are rejected');

    ($e, $err) = $S->can('normalize_entries')->('nope');
    ok(!@$e && @$err, 'not a list is rejected');
    ($e, $err) = $S->can('normalize_entries')->([ ({ weekday => 1, time => '08:00' }) x 51 ]);
    ok(!@$e && @$err, 'too many rows are rejected');
    ($e, $err) = $S->can('normalize_entries')->([]);
    is_deeply([ $e, $err ], [ [], [] ], 'an empty schedule is valid');
}

my ($entries) = $S->can('normalize_entries')->([
    { weekday => 1, time => '08:00' },
    { weekday => 2, time => '20:00' },
    { weekday => 6, time => '13:00' },
]);

sub due { my ($now, $grace) = @_; return [ map { $_->{key} } $S->can('due_entries')->($entries, $now, $grace) ] }

# ---- due entries
is_deeply(due(at(2026, 11, 23, 7, 59)), [], 'not due one minute before');
is_deeply(due(at(2026, 11, 23, 8, 0)), ['1-08:00-import:2026-11-23'], 'due on the minute');
is_deeply(due(at(2026, 11, 23, 8, 30)), ['1-08:00-import:2026-11-23'], 'still due 30 minutes later (missed ticks)');
is_deeply(due(at(2026, 11, 23, 9, 0)), ['1-08:00-import:2026-11-23'], 'due at the end of the grace period');
is_deeply(due(at(2026, 11, 23, 9, 1)), [], 'not due after the grace period');
is_deeply(due(at(2026, 11, 23, 9, 1), 120), ['1-08:00-import:2026-11-23'], 'grace period can be changed');
is_deeply(due(at(2026, 11, 24, 8, 0)), [], 'Tuesday 08:00 is not on the schedule');
is_deeply(due(at(2026, 11, 24, 20, 0)), ['2-20:00-import:2026-11-24'], 'Tuesday 20:00');
is_deeply(due(at(2026, 11, 28, 13, 5)), ['6-13:00-import:2026-11-28'], 'Saturday 13:00');
is_deeply(due(at(2026, 11, 29, 13, 5)), [], 'Sunday is not on the schedule');
is_deeply(due(at(2026, 11, 30, 8, 0)), ['1-08:00-import:2026-11-30'], 'the next Monday has a new key');

# ---- the same entry gets the same key on the whole day, different days differ
{
    my @a = $S->can('due_entries')->($entries, at(2026, 11, 23, 8, 0));
    my @b = $S->can('due_entries')->($entries, at(2026, 11, 23, 8, 40));
    is($a[0]{key}, $b[0]{key}, 'same key at 08:00 and 08:40, so it is not started twice');
}

# ---- across midnight
{
    my ($late) = $S->can('normalize_entries')->([ { weekday => 1, time => '23:50' } ]);
    my @d = $S->can('due_entries')->($late, at(2026, 11, 24, 0, 10));
    is(scalar @d, 1, 'a run planned at 23:50 is still due at 00:10 the next day');
    is($d[0]{key}, '1-23:50-import:2026-11-23', 'and keeps the date of the planned day');
    @d = $S->can('due_entries')->($late, at(2026, 11, 24, 1, 0));
    is(scalar @d, 0, 'but not after the grace period');
    @d = $S->can('due_entries')->($late, at(2026, 11, 23, 23, 55));
    is(scalar @d, 1, 'and due at 23:55 the same day');
}

# ---- several entries on the same day, and the same time for two job types
{
    my ($many) = $S->can('normalize_entries')->([ { weekday => 1, time => '08:00' }, { weekday => 1, time => '08:00', job_type => 'discover' }, { weekday => 1, time => '08:30' } ]);
    my @d = $S->can('due_entries')->($many, at(2026, 11, 23, 8, 35));
    is_deeply([ sort map { $_->{key} } @d ], [ '1-08:00-discover:2026-11-23', '1-08:00-import:2026-11-23', '1-08:30-import:2026-11-23' ], 'several due entries are all returned');
}

is_deeply([ $S->can('due_entries')->([], at(2026, 11, 23, 8, 0)) ], [], 'empty schedule: nothing is due');

done_testing();
