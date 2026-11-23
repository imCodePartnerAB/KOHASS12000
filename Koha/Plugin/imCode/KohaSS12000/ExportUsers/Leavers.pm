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

# Returns (\@rows, \@errors); rows are { from, to, to_noloans }:
#   to          category while the patron has loans left (required)
#   to_noloans  category while the patron has no loans (optional; empty = stays in its category until removed)
sub normalize_mapping {
    my ($raw) = @_;
    return ([], ['the mapping must be a list']) unless ref $raw eq 'ARRAY';
    my (@rows, @errors, %seen);
    my $n = 0;
    foreach my $r (@$raw) {
        $n++;
        unless (ref $r eq 'HASH') { push @errors, "row $n: not an object"; next }
        my ($from, $to, $nl) = (trim($r->{from}), trim($r->{to}), trim($r->{to_noloans}));
        next if $from eq '' && $to eq '' && $nl eq '';
        if ($from eq '' || $to eq '') { push @errors, "row $n: the category and the category with loans left are needed"; next }
        if ($from eq $to) { push @errors, "row $n: '$from' cannot move to itself"; next }
        $nl = '' if $nl eq $from;
        if ($seen{$from}++) { push @errors, "row $n: '$from' is listed twice"; next }
        push @rows, { from => $from, to => $to, to_noloans => ($nl eq '' ? undef : $nl) };
    }
    my %from = map { $_->{from} => 1 } @rows;
    foreach my $r (@rows) {
        foreach my $t (grep { defined } $r->{to}, $r->{to_noloans}) {
            push @errors, "'$t' is a target and also a source of another row" if $from{$t};
        }
    }
    return (\@rows, \@errors);
}

sub normalize_mode { my ($m) = @_; $m = trim($m); return $MODES{$m} ? $m : 'off' }

# $leaver:  { category (now), previous_category (when it was first seen as a leaver), has_loans (0/1), days (as a leaver),
#             anonymized (0/1, the personal data is already wiped) }
# $mapping: { FROM => { loans => CATEGORY, noloans => CATEGORY|undef } }
# $opts:    { delete_after_days (no loans, default 30), anonymize_after_days_loans (with loans, undef = never) }
# Returns { action => 'skip'|'keep'|'move'|'delete'|'anonymize', to => CATEGORY|undef, why => text }
# A patron with open loans cannot be removed (Koha refuses), so after the limit the account is anonymized instead:
# nobody can be identified from it any more, and it is removed later when the loans are back.
# 'delete' is a decision: Koha can still refuse (fines, guarantees).
sub decide {
    my ($leaver, $mapping, $opts) = @_;
    $opts ||= {};
    my $category = $leaver->{category};
    return { action => 'skip', why => 'no category' } unless defined $category && $category ne '';
    my $prev  = defined $leaver->{previous_category} && $leaver->{previous_category} ne '' ? $leaver->{previous_category} : $category;
    my $entry = ($mapping || {})->{$prev} || ($mapping || {})->{$category};
    return { action => 'skip', why => "category $prev is not in the mapping" } unless $entry;

    my $days   = $leaver->{days} // 0;
    my $loans  = $leaver->{has_loans} ? 1 : 0;
    if ($loans) {
        my $alimit = $opts->{anonymize_after_days_loans};
        if (defined $alimit && $alimit ne '' && $days >= $alimit && !$leaver->{anonymized}) {
            return { action => 'anonymize', why => "loans left, a leaver for $days days" };
        }
    } else {
        my $limit = defined $opts->{delete_after_days} ? $opts->{delete_after_days} : 30;
        return { action => 'delete', why => "no loans, a leaver for $days days" } if $limit ne '' && $days >= $limit;
    }

    my $want = $loans ? $entry->{loans} : $entry->{noloans};
    return { action => 'keep', why => 'anonymized, waiting for the loans to be returned' } if $leaver->{anonymized} && $loans;
    return { action => 'move', to => $want, why => ($loans ? 'loans left' : 'no loans, waiting to be removed') } if defined $want && $want ne $category;
    return { action => 'keep', why => $loans ? 'loans left, already in place' : 'no loans, waiting to be removed' };
}

1;
