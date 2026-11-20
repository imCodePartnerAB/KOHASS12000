package Koha::Plugin::imCode::KohaSS12000::ExportUsers::Schedule;

# Run schedule for the SS12000 plugin: pure functions without Koha dependencies (see t/schedule.t).
#
# A schedule is a list of entries { weekday => 1..7 (Monday = 1, Sunday = 7), time => 'HH:MM', job_type => 'import' }.
# A cron job started every minute asks which entries are due. An entry is due from its time and for
# $grace minutes after it, so a missed minute does not lose a run, and it is identified by a key that
# contains the date, so the same entry never runs twice on the same day.

use strict;
use warnings;

use POSIX qw(strftime mktime);

our $VERSION = '2.0';

my %JOB_TYPES = (import => 1, discover => 1);
our $MAX_ENTRIES = 50;

sub trim { my $v = $_[0]; $v = '' unless defined $v; $v =~ s/^\s+|\s+$//g; return $v }

# Returns (\@entries, \@errors). Entries are sorted by weekday and time, duplicates are removed.
sub normalize_entries {
    my ($raw) = @_;
    my (@entries, @errors, %seen);
    return ([], ['the schedule must be a list']) unless ref $raw eq 'ARRAY';
    return ([], ["the schedule has more than $MAX_ENTRIES rows"]) if @$raw > $MAX_ENTRIES;

    my $n = 0;
    foreach my $r (@$raw) {
        $n++;
        unless (ref $r eq 'HASH') { push @errors, "row $n: not an object"; next }
        my $weekday = trim($r->{weekday});
        my $time    = trim($r->{time});
        my $type    = trim($r->{job_type}) || 'import';
        unless ($weekday =~ /^[1-7]$/) { push @errors, "row $n: weekday must be 1 (Monday) to 7 (Sunday)"; next }
        unless ($time =~ /^(?:[01]?\d|2[0-3]):[0-5]\d$/) { push @errors, "row $n: time must be HH:MM"; next }
        unless ($JOB_TYPES{$type}) { push @errors, "row $n: unknown job type '$type'"; next }
        $time = sprintf('%02d:%02d', split(/:/, $time));
        next if $seen{"$weekday-$time-$type"}++;
        push @entries, { weekday => $weekday + 0, time => $time, job_type => $type };
    }
    @entries = sort { $a->{weekday} <=> $b->{weekday} || $a->{time} cmp $b->{time} || $a->{job_type} cmp $b->{job_type} } @entries;
    return (\@entries, \@errors);
}

sub weekday_of {
    my ($epoch) = @_;
    my $wday = (localtime($epoch))[6];    # 0 = Sunday
    return $wday == 0 ? 7 : $wday;
}

# Entries that are due at $now (epoch): a list of { key, entry, scheduled } where scheduled is the epoch
# of the planned start. Today and yesterday are looked at, so a run planned just before midnight
# is still found just after it.
sub due_entries {
    my ($entries, $now, $grace_minutes) = @_;
    $grace_minutes = 60 unless defined $grace_minutes;
    my @due;
    foreach my $back (0, 1) {
        my @t = localtime($now);
        $t[3] -= $back;
        my $day = mktime(0, 0, 0, $t[3], $t[4], $t[5]);
        my $weekday = weekday_of($day + 12 * 3600);
        my $date    = strftime('%Y-%m-%d', localtime($day + 12 * 3600));
        foreach my $e (@{ $entries || [] }) {
            next unless $e->{weekday} == $weekday;
            my ($h, $m) = split /:/, $e->{time};
            my $scheduled = mktime(0, $m, $h, $t[3], $t[4], $t[5]);
            my $age = $now - $scheduled;
            next if $age < 0 || $age > $grace_minutes * 60;
            push @due, { key => "$e->{weekday}-$e->{time}-$e->{job_type}:$date", entry => $e, scheduled => $scheduled };
        }
    }
    return @due;
}

1;
