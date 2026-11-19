package Koha::Plugin::imCode::KohaSS12000::ExportUsers::Rules;

# Rule engine for the SS12000 plugin: pure functions without Koha dependencies,
# so it can be unit tested with plain `prove` (see t/rules.t).
#
# A rule:  { id, priority, name, enabled, scope => 'category'|'branch',
#            conditions => [ { field, op, values => [...] } ],
#            action => { category => 'X' | not_import => 1 | branch => 'Y' } }
# Facts:   hash of field => scalar | arrayref (see build_facts). A missing or empty
#          fact only matches the operator "empty". A fact listed in
#          $facts->{unavailable} (for example roles when /duties is forbidden)
#          matches nothing at all, not even "empty".

use strict;
use warnings;
use utf8;

use JSON::PP ();

our $VERSION = '2.0';

my %FIELDS = (
    school          => { type => 'text'   },
    school_group    => { type => 'text'   },
    school_type     => { type => 'text'   },
    school_year     => { type => 'number' },
    education_code  => { type => 'text'   },
    class           => { type => 'text'   },
    role            => { type => 'text'   },
    assignment_role => { type => 'text'   },
    age             => { type => 'number' },
    person_kind     => { type => 'text'   },
    duty_school     => { type => 'text'   },
);

my %OPS_BY_TYPE = (
    text   => [qw(in starts_with empty not_empty)],
    number => [qw(in gte lte empty not_empty)],
    any    => [qw(in starts_with gte lte empty not_empty)],
);

my %NEEDS_VALUES = (in => 1, starts_with => 1, gte => 1, lte => 1);

sub field_names { return sort keys %FIELDS }

sub field_type {
    my ($field) = @_;
    return 'any' if defined $field && $field =~ /^custom:/;
    return $FIELDS{$field // ''} ? $FIELDS{$field}{type} : undef;
}

sub operators_for {
    my ($field) = @_;
    my $type = field_type($field) or return ();
    return @{ $OPS_BY_TYPE{$type} };
}

sub trim { my $v = $_[0]; $v = '' unless defined $v; $v =~ s/^\s+|\s+$//g; return $v }

sub is_number { return defined $_[0] && $_[0] =~ /^-?\d+(?:\.\d+)?$/ ? 1 : 0 }

# Returns (\%rule, undef) with a normalised copy, or (undef, $error_message)
sub normalize_rule {
    my ($raw) = @_;
    return (undef, 'rule is not an object') unless ref $raw eq 'HASH';

    my %rule;
    $rule{id}       = defined $raw->{id} && $raw->{id} =~ /^\d+$/ ? $raw->{id} + 0 : undef;
    $rule{priority} = defined $raw->{priority} && $raw->{priority} =~ /^\d+$/ ? $raw->{priority} + 0 : 1;
    $rule{name}     = substr(trim($raw->{name}), 0, 120);
    $rule{enabled}  = (defined $raw->{enabled} && !$raw->{enabled}) ? 0 : 1;
    $rule{scope}    = (($raw->{scope} // 'category') eq 'branch') ? 'branch' : 'category';

    my $conds = $raw->{conditions};
    return (undef, 'a rule needs at least one condition') unless ref $conds eq 'ARRAY' && @$conds;

    my @clean;
    foreach my $c (@$conds) {
        return (undef, 'condition is not an object') unless ref $c eq 'HASH';
        my $field = trim($c->{field});
        my $op    = trim($c->{op});
        if ($field =~ /^custom:/) {
            return (undef, "invalid field path '$field'") unless $field =~ /^custom:[A-Za-z0-9_.\[\]-]{1,120}$/;
        } elsif (!$FIELDS{$field}) {
            return (undef, "unknown field '$field'");
        }
        return (undef, "operator '$op' is not allowed for field '$field'") unless grep { $_ eq $op } operators_for($field);

        my %cond = (field => $field, op => $op);
        if ($NEEDS_VALUES{$op}) {
            my $values = ref $c->{values} eq 'ARRAY' ? $c->{values} : (defined $c->{value} ? [$c->{value}] : []);
            my (%seen, @vals);
            foreach my $v (map { trim($_) } grep { defined && !ref } @$values) {
                next if $v eq '' || $seen{lc $v}++;
                push @vals, $v;
            }
            return (undef, "condition on '$field' needs a value") unless @vals;
            if ($op eq 'gte' || $op eq 'lte') {
                @vals = ($vals[0]);
                return (undef, "'$vals[0]' is not a number") unless is_number($vals[0]);
            } elsif (field_type($field) eq 'number') {
                my @bad = grep { !is_number($_) } @vals;
                return (undef, "'$bad[0]' is not a number") if @bad;
            }
            $cond{values} = \@vals;
        }
        push @clean, \%cond;
    }
    $rule{conditions} = \@clean;

    my $act = ref $raw->{action} eq 'HASH' ? $raw->{action} : {};
    if ($rule{scope} eq 'branch') {
        my $branch = trim($act->{branch});
        return (undef, 'a branch rule needs a branch') if $branch eq '';
        $rule{action} = { branch => $branch };
    } elsif ($act->{not_import}) {
        $rule{action} = { not_import => 1 };
    } else {
        my $category = trim($act->{category});
        return (undef, 'a category rule needs a category or "do not import"') if $category eq '';
        $rule{action} = { category => $category };
    }
    return (\%rule, undef);
}

# Returns (\@rules, \@errors); each error is "rule N: message" (N is the 1-based position)
sub normalize_rules {
    my ($raw_list) = @_;
    my (@rules, @errors);
    return (\@rules, ['rules must be a list']) unless ref $raw_list eq 'ARRAY';
    my $n = 0;
    foreach my $raw (@$raw_list) {
        $n++;
        my ($rule, $err) = normalize_rule($raw);
        if ($err) { push @errors, "rule $n: $err"; next }
        push @rules, $rule;
    }
    return (\@rules, \@errors);
}

sub sort_rules {
    return sort {
        $a->{priority} <=> $b->{priority}
          || ($a->{id} // 1e12) <=> ($b->{id} // 1e12)
    } @_;
}

# Priorities used by more than one enabled rule of the same scope: [ [scope, priority], ... ]
sub duplicate_priorities {
    my ($rules) = @_;
    my %count;
    $count{ $_->{scope} }{ $_->{priority} }++ for grep { $_->{enabled} } @{ $rules || [] };
    return [ map { my $s = $_; map { [ $s, $_ + 0 ] } grep { $count{$s}{$_} > 1 } sort { $a <=> $b } keys %{ $count{$s} } } sort keys %count ];
}

# Set of field names used by enabled rules: { school_year => 1, ... }
sub fields_used {
    my ($rules) = @_;
    my %used;
    foreach my $r (grep { $_->{enabled} } @{ $rules || [] }) {
        $used{ $_->{field} } = 1 for @{ $r->{conditions} };
    }
    return \%used;
}

sub custom_paths {
    my ($rules) = @_;
    my %paths;
    foreach my $r (grep { $_->{enabled} } @{ $rules || [] }) {
        foreach my $c (@{ $r->{conditions} }) {
            $paths{ substr($c->{field}, 7) } = 1 if $c->{field} =~ /^custom:/;
        }
    }
    return sort keys %paths;
}

# Scalar values found at a dotted path ("enrolments.educationCode"), arrays are walked
sub extract_values {
    my ($data, $path) = @_;
    my @nodes = ($data);
    foreach my $key (split /\./, $path // '') {
        my @next;
        foreach my $n (@nodes) {
            $n = [$n] unless ref $n eq 'ARRAY';
            foreach my $item (@$n) {
                next unless ref $item eq 'HASH' && defined $item->{$key};
                push @next, $item->{$key};
            }
        }
        @nodes = @next;
    }
    my @vals;
    foreach my $n (@nodes) {
        push @vals, (ref $n eq 'ARRAY' ? grep { defined && !ref } @$n : (ref $n ? () : $n));
    }
    return @vals;
}

sub age_on {
    my ($birthdate, $today) = @_;
    return undef unless defined $birthdate && defined $today
        && $birthdate =~ /^(\d{4})-(\d{2})-(\d{2})$/;
    my ($by, $bm, $bd) = ($1, $2, $3);
    return undef unless $today =~ /^(\d{4})-(\d{2})-(\d{2})$/;
    my ($ty, $tm, $td) = ($1, $2, $3);
    my $age = $ty - $by;
    $age-- if $tm < $bm || ($tm == $bm && $td < $bd);
    return $age < 0 ? undef : $age;
}

# Age from a Swedish civic number (YYYYMMDDNNNN, day + 60 for coordination numbers)
sub age_from_civic_no {
    my ($civic, $today) = @_;
    return undef unless defined $civic && $civic =~ /^(\d{4})(\d{2})(\d{2})\d{4}$/;
    my ($y, $m, $d) = ($1, $2, $3);
    $d -= 60 if $d > 60;
    return undef unless $m >= 1 && $m <= 12 && $d >= 1 && $d <= 31 && $y >= 1900;
    return age_on(sprintf('%04d-%02d-%02d', $y, $m, $d), $today);
}

sub duty_is_active {
    my ($duty, $today) = @_;
    my $start = $duty->{startDate} // $duty->{start} // '0000-00-00';
    my $end   = $duty->{endDate}   // $duty->{end}   // '9999-12-31';
    return $start le $today && $today le $end ? 1 : 0;
}

# Builds the facts for one person from already resolved parts:
#   enrolment        current enrolment hash (schoolType, schoolYear, educationCode) or undef
#   school_codes     organisation codes of the enrolment school (own first, then parents)
#   school_group_ids ids of the parent organisations
#   klass            class name or undef
#   duties           arrayref of { dutyRole, assignmentRole, school, startDate, endDate },
#                    or undef when the duties endpoint is not available
#   birthdate, civic_no, today
#   custom           { 'enrolments.educationCode' => value|[values] }
sub build_facts {
    my (%a) = @_;
    my $today = $a{today};
    my $enr   = $a{enrolment};

    my @duties = grep { duty_is_active($_, $today) } @{ $a{duties} || [] };
    my $kind   = ($enr && @duties) ? 'both' : @duties ? 'staff' : $enr ? 'student' : 'none';

    my %facts = (
        school          => [ @{ $a{school_codes}     || [] } ],
        school_group    => [ @{ $a{school_group_ids} || [] } ],
        school_type     => $enr ? $enr->{schoolType}     : undef,
        school_year     => $enr ? $enr->{schoolYear}     : undef,
        education_code  => $enr ? $enr->{educationCode}  : undef,
        class           => $a{klass},
        role            => [ map { $_->{dutyRole} }       grep { defined $_->{dutyRole}       && $_->{dutyRole}       ne '' } @duties ],
        assignment_role => [ map { $_->{assignmentRole} } grep { defined $_->{assignmentRole} && $_->{assignmentRole} ne '' } @duties ],
        duty_school     => [ map { $_->{school} }         grep { defined $_->{school}         && $_->{school}         ne '' } @duties ],
        age             => defined $a{birthdate} ? age_on($a{birthdate}, $today) : age_from_civic_no($a{civic_no}, $today),
        person_kind     => $kind,
        custom          => { %{ $a{custom} || {} } },
        unavailable     => {},
    );
    $facts{unavailable}{$_} = 1 for ($a{duties} ? () : qw(role assignment_role duty_school));
    return \%facts;
}

sub fact_values {
    my ($facts, $field) = @_;
    my $raw = $field =~ /^custom:(.*)$/ ? ($facts->{custom} || {})->{$1} : $facts->{$field};
    my @list = ref $raw eq 'ARRAY' ? @$raw : ($raw);
    return grep { length } map { trim($_) } grep { defined && !ref } @list;
}

sub condition_matches {
    my ($cond, $facts, $warnings) = @_;
    my $field = $cond->{field};
    my $op    = $cond->{op};
    return 0 if $field !~ /^custom:/ && ($facts->{unavailable} || {})->{$field};

    my @vals = fact_values($facts, $field);
    return @vals ? 0 : 1 if $op eq 'empty';
    return @vals ? 1 : 0 if $op eq 'not_empty';
    return 0 unless @vals;

    my $type    = field_type($field);
    my @wanted  = @{ $cond->{values} || [] };

    if ($op eq 'gte' || $op eq 'lte') {
        my $limit = $wanted[0];
        foreach my $v (@vals) {
            unless (is_number($v)) {
                push @$warnings, "value '$v' of field '$field' is not a number" if $warnings;
                next;
            }
            return 1 if $op eq 'gte' ? $v >= $limit : $v <= $limit;
        }
        return 0;
    }
    if ($op eq 'starts_with') {
        foreach my $v (@vals) {
            my $lv = lc $v;
            return 1 if grep { my $p = lc $_; substr($lv, 0, length $p) eq $p } @wanted;
        }
        return 0;
    }
    foreach my $v (@vals) {
        if ($type eq 'number') {
            unless (is_number($v)) {
                push @$warnings, "value '$v' of field '$field' is not a number" if $warnings;
                next;
            }
            return 1 if grep { $v == $_ } @wanted;
        } else {
            my $lv = lc $v;
            return 1 if grep { lc($_) eq $lv } @wanted;
        }
    }
    return 0;
}

sub rule_matches {
    my ($rule, $facts, $warnings) = @_;
    my @conds = @{ $rule->{conditions} || [] };
    return 0 unless @conds;
    foreach my $c (@conds) {
        return 0 unless condition_matches($c, $facts, $warnings);
    }
    return 1;
}

# Evaluates all enabled rules of one scope. Options:
#   scope             'category' (default) or 'branch'
#   valid_categories  { code => 1 }  rules with another category are skipped with a warning
#   valid_branches    { code => 1 }  same for branch rules
# Returns { winner, matched => [rules in order], shadowed => [rules that matched but lost], warnings => [] }
sub evaluate {
    my ($rules, $facts, %opt) = @_;
    my $scope = $opt{scope} // 'category';
    my (@matched, @warnings);

    foreach my $rule (sort_rules(grep { $_->{enabled} && $_->{scope} eq $scope } @{ $rules || [] })) {
        my $act = $rule->{action} || {};
        if ($opt{valid_categories} && defined $act->{category} && !$opt{valid_categories}{ $act->{category} }) {
            push @warnings, sprintf("%s skipped: Koha category '%s' does not exist", rule_label($rule), $act->{category});
            next;
        }
        if ($opt{valid_branches} && defined $act->{branch} && !$opt{valid_branches}{ $act->{branch} }) {
            push @warnings, sprintf("%s skipped: Koha branch '%s' does not exist", rule_label($rule), $act->{branch});
            next;
        }
        push @matched, $rule if rule_matches($rule, $facts, \@warnings);
    }

    my @shadowed = @matched > 1 ? @matched[1 .. $#matched] : ();
    return { winner => $matched[0], matched => \@matched, shadowed => \@shadowed, warnings => \@warnings };
}

sub rule_label {
    my ($rule) = @_;
    my $label = defined $rule->{id} ? "rule #$rule->{id}" : 'rule';
    $label .= " (priority $rule->{priority}";
    $label .= ", $rule->{name}" if length($rule->{name} // '');
    return "$label)";
}

sub action_text {
    my ($rule) = @_;
    my $act = $rule->{action} || {};
    return $act->{not_import} ? 'do not import' : defined $act->{branch} ? "branch $act->{branch}" : "category $act->{category}";
}

# One line for the log: which rule won and which others also matched but lost
sub explain {
    my ($result) = @_;
    return 'no rule matched' unless $result->{winner};
    my $text = rule_label($result->{winner}) . ' -> ' . action_text($result->{winner});
    if (@{ $result->{shadowed} }) {
        $text .= '; also matched but not used: ' . join(', ', map { rule_label($_) . ' -> ' . action_text($_) } @{ $result->{shadowed} });
    }
    return $text;
}

sub conditions_to_json {
    my ($conditions) = @_;
    return JSON::PP->new->canonical->utf8(0)->encode($conditions);
}

sub conditions_from_json {
    my ($json) = @_;
    my $data = eval { JSON::PP->new->utf8(0)->decode($json // '') };
    return ref $data eq 'ARRAY' ? $data : [];
}

1;
