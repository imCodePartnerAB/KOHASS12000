package Koha::Plugin::imCode::KohaSS12000::ExportUsers::Leavers;

# Decisions for patrons that are no longer in the school register (pure functions, see t/leavers.t).
#
# A leaver that has loans left is moved to the "left" category that belongs to its category and its
# account expires (no new loans). A leaver without loans is removed when it has been a leaver for
# $delete_after_days. Only patrons whose category is in the mapping (or is one of its target
# categories) are ever touched; everything else (staff, system accounts, ...) is left alone.

use strict;
use warnings;

our $VERSION = '2.0';

our %MODES = (off => 1, report => 1, apply => 1);

sub trim { my $v = $_[0]; $v = '' unless defined $v; $v =~ s/^\s+|\s+$//g; return $v }

# Returns (\@rows, \@errors); rows are { from => 'CODE', to => 'CODE' }, duplicates of "from" are refused
sub normalize_mapping {
    my ($raw) = @_;
    return ([], ['the mapping must be a list']) unless ref $raw eq 'ARRAY';
    my (@rows, @errors, %seen);
    my $n = 0;
    foreach my $r (@$raw) {
        $n++;
        unless (ref $r eq 'HASH') { push @errors, "row $n: not an object"; next }
        my ($from, $to) = (trim($r->{from}), trim($r->{to}));
        next if $from eq '' && $to eq '';
        if ($from eq '' || $to eq '') { push @errors, "row $n: both categories are needed"; next }
        if ($from eq $to) { push @errors, "row $n: '$from' cannot move to itself"; next }
        if ($seen{$from}++) { push @errors, "row $n: '$from' is listed twice"; next }
        push @rows, { from => $from, to => $to };
    }
    my %from = map { $_->{from} => 1 } @rows;
    foreach my $r (@rows) {
        push @errors, "'$r->{to}' is a target and also a source of another row" if $from{ $r->{to} };
    }
    return (\@rows, \@errors);
}

sub normalize_mode { my ($m) = @_; $m = trim($m); return $MODES{$m} ? $m : 'off' }

# $leaver: { category, has_loans (0/1), days (days since it was first seen as a leaver) }
# $mapping: { FROM => TO }
# $opts: { delete_after_days }
# Returns { action => 'skip'|'keep'|'move'|'delete', to => CATEGORY|undef, why => text }
sub decide {
    my ($leaver, $mapping, $opts) = @_;
    $opts ||= {};
    my $category = $leaver->{category};
    return { action => 'skip', why => 'no category' } unless defined $category && $category ne '';
    my %target = map { $_ => 1 } values %{ $mapping || {} };
    my $mapped = ($mapping || {})->{$category};
    my $is_target = $target{$category} ? 1 : 0;

    return { action => 'skip', why => "category $category is not in the mapping" } unless defined $mapped || $is_target;

    my $days_needed = defined $opts->{delete_after_days} ? $opts->{delete_after_days} : 7;
    if (!$leaver->{has_loans} && ($leaver->{days} // 0) >= $days_needed) {
        return { action => 'delete', why => 'no loans left and a leaver for ' . ($leaver->{days} // 0) . ' days' };
    }
    return { action => 'keep', why => $leaver->{has_loans} ? 'in its left category, loans left' : 'in its left category, waiting to be removed' } if $is_target;
    return { action => 'move', to => $mapped, why => $leaver->{has_loans} ? 'loans left' : 'no loans, removed after ' . $days_needed . ' days' };
}

1;
