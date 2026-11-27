use strict;
use warnings;
use utf8;
use FindBin;
use lib "$FindBin::Bin/..";
use Test::More;

binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

use_ok('Koha::Plugin::imCode::KohaSS12000::ExportUsers::Rules');
my $R = 'Koha::Plugin::imCode::KohaSS12000::ExportUsers::Rules';

my $today = '2026-11-19';

sub rule {
    my (%a) = @_;
    my ($r, $err) = $R->can('normalize_rule')->({
        id => $a{id}, priority => $a{priority} // 1, name => $a{name} // '', enabled => $a{enabled},
        scope => $a{scope}, conditions => $a{conditions}, action => $a{action} // { category => 'X' },
    });
    die "bad rule in test: $err" unless $r;
    return $r;
}

sub cond { my ($field, $op, @values) = @_; return { field => $field, op => $op, values => [@values] } }

sub facts {
    my (%a) = @_;
    return $R->can('build_facts')->(today => $today, %a);
}

sub winner_cat {
    my ($rules, $facts, %opt) = @_;
    my $res = $R->can('evaluate')->($rules, $facts, %opt);
    return $res->{winner} ? ($res->{winner}{action}{category} // ($res->{winner}{action}{not_import} ? 'NOIMPORT' : $res->{winner}{action}{branch})) : undef;
}

# ---- normalisation and validation
{
    my ($r, $e) = $R->can('normalize_rule')->({ conditions => [], action => { category => 'A' } });
    ok(!$r && $e =~ /at least one condition/, 'rule without conditions is rejected');

    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('nonsense', 'in', 'x') ], action => { category => 'A' } });
    ok(!$r && $e =~ /unknown field/, 'unknown field is rejected');

    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('school_year', 'starts_with', '1') ], action => { category => 'A' } });
    ok(!$r && $e =~ /not allowed/, 'operator not allowed for a number field');

    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('school_year', 'in', 'abc') ], action => { category => 'A' } });
    ok(!$r && $e =~ /not a number/, 'non numeric value for a number field is rejected');

    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('class', 'in') ], action => { category => 'A' } });
    ok(!$r && $e =~ /needs a value/, 'in without value is rejected');

    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('class', 'in', 'A') ], action => {} });
    ok(!$r && $e =~ /needs a category/, 'category rule without category is rejected');

    ($r, $e) = $R->can('normalize_rule')->({ scope => 'branch', conditions => [ cond('class', 'in', 'A') ], action => { category => 'A' } });
    ok(!$r && $e =~ /needs a branch/, 'branch rule without branch is rejected');

    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('custom:enrolments.educationCode', 'starts_with', 'CL') ], action => { category => 'A' } });
    ok($r && !$e, 'custom path field is accepted');

    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('custom:a b', 'in', 'x') ], action => { category => 'A' } });
    ok(!$r && $e =~ /invalid field path/, 'invalid custom path is rejected');

    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('class', 'in', ' A ', 'a', 'B') ], action => { not_import => 1, category => 'IGNORED' } });
    is_deeply($r->{conditions}[0]{values}, ['A', 'B'], 'values are trimmed and de-duplicated (case-insensitive)');
    is_deeply($r->{action}, { not_import => 1 }, 'not_import wins over a category');

    my ($rules, $errors) = $R->can('normalize_rules')->([ { conditions => [] }, { conditions => [ cond('class', 'in', 'A') ], action => { category => 'A' } } ]);
    is(scalar @$rules, 1, 'good rules are kept when others fail');
    like($errors->[0], qr/^rule 1:/, 'error names the position of the rule');
}

# ---- operators
{
    my $f = facts(enrolment => { schoolType => 'GY', schoolYear => 2, educationCode => 'PRG25' }, school_codes => ['SCHOOL1'], klass => 'CLS25A', civic_no => '200801010000');

    is(winner_cat([ rule(conditions => [ cond('school_year', 'in', 1, 2, 3) ], action => { category => 'YEAR' }) ], $f), 'YEAR', 'in: one of several numbers');
    is(winner_cat([ rule(conditions => [ cond('school_year', 'in', 4, 5) ], action => { category => 'YEAR' }) ], $f), undef, 'in: no match');
    is(winner_cat([ rule(conditions => [ cond('school_year', 'gte', 2) ], action => { category => 'GE' }) ], $f), 'GE', 'gte');
    is(winner_cat([ rule(conditions => [ cond('school_year', 'lte', 1) ], action => { category => 'LE' }) ], $f), undef, 'lte no match');
    is(winner_cat([ rule(conditions => [ cond('class', 'starts_with', 'cl') ], action => { category => 'CL' }) ], $f), 'CL', 'starts_with ignores case');
    is(winner_cat([ rule(conditions => [ cond('school', 'in', 'school1') ], action => { category => 'S' }) ], $f), 'S', 'text in ignores case');
    is(winner_cat([ rule(conditions => [ cond('education_code', 'starts_with', 'PRG', 'QR') ], action => { category => 'P' }) ], $f), 'P', 'starts_with with several prefixes (OR)');
    is(winner_cat([ rule(conditions => [ cond('school', 'in', 'SCHOOL1'), cond('school_year', 'in', 1) ], action => { category => 'AND' }) ], $f), undef, 'conditions are combined with AND');
    is(winner_cat([ rule(conditions => [ cond('school', 'in', 'SCHOOL1'), cond('school_year', 'in', 2) ], action => { category => 'AND' }) ], $f), 'AND', 'all conditions true gives a match');
    is(winner_cat([ rule(conditions => [ cond('age', 'lte', 20) ], action => { category => 'YOUNG' }) ], $f), 'YOUNG', 'age from civic number');
    is($R->can('age_on')->('2000-11-20', $today), 25, 'age_on: birthday not yet reached');
    is($R->can('age_on')->('2000-11-19', $today), 26, 'age_on: birthday today');
    is($R->can('age_from_civic_no')->('200001610000', $today), 26, 'age from coordination number (day + 60)');
    is($R->can('age_from_civic_no')->('garbage', $today), undef, 'age from broken civic number is undef');
    is(winner_cat([ rule(conditions => [ cond('class', 'in', 'cls25a') ], action => { category => 'C' }) ], $f), 'C', 'class equality ignores case');
}

# ---- contains
{
    my $f = facts(klass => 'ABIMXYZ26', enrolment => { schoolType => 'GY', schoolYear => 3 });
    is(winner_cat([ rule(conditions => [ cond('class', 'contains', 'im') ], action => { category => 'IM' }) ], $f), 'IM', 'contains matches in the middle of a value, ignoring case');
    is(winner_cat([ rule(conditions => [ cond('class', 'starts_with', 'IM') ], action => { category => 'IM' }) ], $f), undef, 'starts_with does not match the middle');
    is(winner_cat([ rule(conditions => [ cond('class', 'contains', 'ZZ', 'XYZ') ], action => { category => 'ANY' }) ], $f), 'ANY', 'contains with several values matches any of them');
    is(winner_cat([ rule(conditions => [ cond('class', 'contains', 'ZZ') ], action => { category => 'NO' }) ], $f), undef, 'contains without a hit');
    my $none = facts(enrolment => { schoolType => 'GY', schoolYear => 3 });
    is(winner_cat([ rule(conditions => [ cond('class', 'contains', 'IM') ], action => { category => 'NO' }) ], $none), undef, 'contains on a missing value never matches');
    my ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('school_year', 'contains', '1') ], action => { category => 'A' } });
    ok(!$r && $e =~ /not allowed/, 'contains is not allowed for a number field');
    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('custom:a.b', 'contains', 'x') ], action => { category => 'A' } });
    ok($r && !$e, 'contains is allowed for other fields (by path)');
    ($r, $e) = $R->can('normalize_rule')->({ conditions => [ cond('class', 'contains') ], action => { category => 'A' } });
    ok(!$r && $e =~ /needs a value/, 'contains needs a value');
    my $swe = facts(klass => 'KURSPÅ26', enrolment => { schoolType => 'GY', schoolYear => 1 });
    is(winner_cat([ rule(conditions => [ cond('class', 'contains', 'på') ], action => { category => 'PA' }) ], $swe), 'PA', 'contains compares Swedish letters ignoring case');
}

# ---- missing fields
{
    my $f = facts(enrolment => { schoolType => 'GR', schoolYear => 3 }, school_codes => ['SCHOOL2']);
    is(winner_cat([ rule(conditions => [ cond('class', 'empty') ], action => { category => 'NOCLASS' }) ], $f), 'NOCLASS', 'missing class matches empty');
    is(winner_cat([ rule(conditions => [ cond('class', 'in', 'A') ], action => { category => 'X' }) ], $f), undef, 'missing class matches nothing else');
    is(winner_cat([ rule(conditions => [ cond('education_code', 'not_empty') ], action => { category => 'X' }) ], $f), undef, 'missing education code does not match not_empty');
    is(winner_cat([ rule(conditions => [ cond('education_code', 'starts_with', 'CL') ], action => { category => 'X' }) ], $f), undef, 'starts_with on missing field');
}

# ---- roles, several roles and unavailable duties (403)
{
    my @duties = (
        { dutyRole => 'Lärare', school => 'SCHOOL2', startDate => '2026-01-01', endDate => '2027-06-30' },
        { dutyRole => 'Rektor', school => 'SCHOOL3', startDate => '2026-08-01' },
        { dutyRole => 'Kurator', school => 'SCHOOL4', startDate => '2020-01-01', endDate => '2021-01-01' },
    );
    my $f = facts(duties => \@duties);
    is($f->{person_kind}, 'staff', 'person kind staff from active duties');
    is_deeply([ sort @{ $f->{role} } ], ['Lärare', 'Rektor'], 'only active duties count');
    is(winner_cat([ rule(conditions => [ cond('role', 'in', 'Rektor') ], action => { category => 'R' }) ], $f), 'R', 'any role of several matches');
    is(winner_cat([ rule(conditions => [ cond('role', 'in', 'Kurator') ], action => { category => 'K' }) ], $f), undef, 'ended duty does not match');
    is(winner_cat([ rule(conditions => [ cond('duty_school', 'in', 'SCHOOL3') ], action => { category => 'D' }) ], $f), 'D', 'any duty school matches');

    my $nf = facts(enrolment => { schoolType => 'GY', schoolYear => 1 }, duties => undef);
    is($nf->{person_kind}, 'student', 'student when duties are unavailable');
    is(winner_cat([ rule(conditions => [ cond('role', 'empty') ], action => { category => 'X' }) ], $nf), undef, 'unavailable role does not even match empty');
    is(winner_cat([ rule(conditions => [ cond('role', 'in', 'Lärare') ], action => { category => 'X' }) ], $nf), undef, 'unavailable role matches nothing');
    is(winner_cat([ rule(conditions => [ cond('school_year', 'in', 1) ], action => { category => 'Y' }) ], $nf), 'Y', 'other fields still work without duties');

    my $none = facts(duties => []);
    is(winner_cat([ rule(conditions => [ cond('role', 'empty') ], action => { category => 'E' }) ], $none), 'E', 'no duties (endpoint available) matches empty');

    my $both = facts(enrolment => { schoolType => 'GY', schoolYear => 3 }, duties => \@duties);
    is($both->{person_kind}, 'both', 'person kind both');
    is(winner_cat([
        rule(id => 1, priority => 1, conditions => [ cond('person_kind', 'in', 'both') ], action => { category => 'BOTH' }),
        rule(id => 2, priority => 2, conditions => [ cond('person_kind', 'in', 'staff') ], action => { category => 'STAFF' }),
    ], $both), 'BOTH', 'person kind both is its own value');
}

# ---- priority, ties and shadowed matches
{
    my $f = facts(enrolment => { schoolType => 'GY', schoolYear => 2 }, school_codes => ['SCHOOL1'], klass => 'CLS25A');
    my @rules = (
        rule(id => 3, priority => 3, name => 'school and class', conditions => [ cond('school', 'in', 'SCHOOL1'), cond('class', 'starts_with', 'CL') ], action => { category => 'C3' }),
        rule(id => 1, priority => 1, name => 'role', conditions => [ cond('role', 'in', 'Lärare') ], action => { category => 'C1' }),
        rule(id => 2, priority => 2, name => 'class', conditions => [ cond('class', 'starts_with', 'CL') ], action => { category => 'C2' }),
    );
    my $res = $R->can('evaluate')->(\@rules, $f);
    is($res->{winner}{id}, 2, 'lowest priority number among matching rules wins');
    is_deeply([ map { $_->{id} } @{ $res->{matched} } ], [ 2, 3 ], 'matched rules are listed in priority order');
    is_deeply([ map { $_->{id} } @{ $res->{shadowed} } ], [3], 'later matches are reported as shadowed');
    like($R->can('explain')->($res), qr/rule #2 .* -> category C2; also matched but not used: rule #3 .* -> category C3/, 'explain names winner and shadowed rules');
    is($R->can('explain')->($R->can('evaluate')->([], $f)), 'no rule matched', 'explain with no match');

    my @tie = (
        rule(id => 9, priority => 5, conditions => [ cond('class', 'starts_with', 'CL') ], action => { category => 'NINE' }),
        rule(id => 4, priority => 5, conditions => [ cond('class', 'starts_with', 'CL') ], action => { category => 'FOUR' }),
    );
    is(winner_cat(\@tie, $f), 'FOUR', 'equal priority: lower id wins, deterministic');
    is_deeply($R->can('duplicate_priorities')->(\@tie), [ [ 'category', 5 ] ], 'duplicate priorities are reported');
    is_deeply($R->can('duplicate_priorities')->([ $tie[0], rule(id => 5, priority => 5, enabled => 0, conditions => [ cond('class', 'in', 'A') ]) ]), [], 'disabled rules do not count as duplicates');

    my @off = ( rule(id => 1, enabled => 0, conditions => [ cond('class', 'starts_with', 'CL') ], action => { category => 'OFF' }) );
    is(winner_cat(\@off, $f), undef, 'disabled rule is ignored');
}

# ---- actions, scopes and Koha validation
{
    my $f = facts(enrolment => { schoolType => 'GR', schoolYear => 5 }, school_codes => ['SCHOOL2']);
    my @rules = (
        rule(id => 1, priority => 1, conditions => [ cond('school', 'in', 'SCHOOL2') ], action => { not_import => 1 }),
        rule(id => 2, priority => 2, scope => 'branch', conditions => [ cond('school_year', 'in', 5) ], action => { branch => 'BRANCH2' }),
    );
    is(winner_cat(\@rules, $f), 'NOIMPORT', 'do-not-import action');
    is(winner_cat(\@rules, $f, scope => 'branch'), 'BRANCH2', 'branch rules are evaluated separately');

    my @miss = ( rule(id => 7, conditions => [ cond('school', 'in', 'SCHOOL2') ], action => { category => 'GONE' }) );
    my $res = $R->can('evaluate')->(\@miss, $f, valid_categories => { OTHER => 1 });
    ok(!$res->{winner}, 'rule with a category missing in Koha is skipped');
    like($res->{warnings}[0], qr/'GONE' does not exist/, 'and says why');

    my @bm = ( rule(id => 8, scope => 'branch', conditions => [ cond('school', 'in', 'SCHOOL2') ], action => { branch => 'NOPE' }) );
    $res = $R->can('evaluate')->(\@bm, $f, scope => 'branch', valid_branches => { BRANCH2 => 1 });
    ok(!$res->{winner} && $res->{warnings}[0] =~ /'NOPE' does not exist/, 'branch rule with unknown branch is skipped');
}

# ---- unexpected values never crash
{
    my $f = facts(enrolment => { schoolType => 'ZZZ', schoolYear => 'abc', educationCode => undef }, school_codes => ['NEW1']);
    my @rules = (
        rule(id => 1, priority => 1, conditions => [ cond('school_year', 'gte', 1) ], action => { category => 'N' }),
        rule(id => 2, priority => 2, conditions => [ cond('school_type', 'in', 'GY') ], action => { category => 'GY' }),
    );
    my $res = $R->can('evaluate')->(\@rules, $f);
    ok(!$res->{winner}, 'unknown values and a non-numeric year give no match');
    like($res->{warnings}[0], qr/not a number/, 'non-numeric year is warned about');
    my $weird = { school => [ { a => 1 }, undef ], class => [ [], 'A' ], role => 'single' };
    is(winner_cat([ rule(conditions => [ cond('class', 'in', 'A') ], action => { category => 'OK' }) ], $weird), 'OK', 'references and undef in facts are ignored');
    is(winner_cat([ rule(conditions => [ cond('role', 'in', 'single') ], action => { category => 'S' }) ], $weird), 'S', 'a scalar fact works like a one element list');
}

# ---- fields_used
{
    my @rules = (
        rule(id => 1, conditions => [ cond('role', 'in', 'A'), cond('school_year', 'in', 1) ]),
        rule(id => 2, enabled => 0, conditions => [ cond('class', 'in', 'A') ]),
    );
    is_deeply($R->can('fields_used')->(\@rules), { role => 1, school_year => 1 }, 'fields_used lists fields of enabled rules only');
    is_deeply($R->can('fields_used')->([]), {}, 'fields_used with no rules');
}

# ---- custom fields
{
    my @vals = $R->can('extract_values')->({ enrolments => [ { educationCode => 'PRG25' }, { educationCode => 'QRS25' }, { other => 1 } ], flat => 'x', obj => { a => { b => 'deep' } } }, 'enrolments.educationCode');
    is_deeply(\@vals, [ 'PRG25', 'QRS25' ], 'extract_values walks arrays');
    is_deeply([ $R->can('extract_values')->({ obj => { a => { b => 'deep' } } }, 'obj.a.b') ], ['deep'], 'extract_values walks objects');
    is_deeply([ $R->can('extract_values')->({}, 'nothing.here') ], [], 'extract_values on a missing path');

    my $r = rule(conditions => [ cond('custom:enrolments.educationCode', 'starts_with', 'QR') ], action => { category => 'QRTECH' });
    my $f = facts(custom => { 'enrolments.educationCode' => [ 'PRG25', 'QRS25' ] });
    is(winner_cat([$r], $f), 'QRTECH', 'custom field matches any value');
    is_deeply([ $R->can('custom_paths')->([$r]) ], ['enrolments.educationCode'], 'custom_paths lists the paths used by enabled rules');

    my $num = rule(conditions => [ cond('custom:x.level', 'gte', 3) ], action => { category => 'LV' });
    is(winner_cat([$num], facts(custom => { 'x.level' => '4' })), 'LV', 'custom field with numeric comparison');
    is(winner_cat([$num], facts(custom => { 'x.level' => 'high' })), undef, 'custom field with text and numeric comparison gives no match');
}

# ---- Swedish characters and JSON round trip
{
    my $f = facts(duties => [ { dutyRole => 'Förskollärare', school => 'ÖSCHOOL', startDate => '2026-01-01' } ]);
    is(winner_cat([ rule(conditions => [ cond('role', 'in', 'förskollärare') ], action => { category => 'FSK' }) ], $f), 'FSK', 'å, ä, ö compare ignoring case');
    is(winner_cat([ rule(conditions => [ cond('duty_school', 'starts_with', 'ös') ], action => { category => 'ON' }) ], $f), 'ON', 'Ö/ö prefix match');

    my $r = rule(id => 1, conditions => [ cond('role', 'in', 'Förskollärare') ]);
    my $json = $R->can('conditions_to_json')->($r->{conditions});
    is_deeply($R->can('conditions_from_json')->($json), $r->{conditions}, 'conditions survive a JSON round trip');
    is_deeply($R->can('conditions_from_json')->('not json'), [], 'broken JSON gives an empty list');
    is_deeply($R->can('conditions_from_json')->(undef), [], 'undef JSON gives an empty list');
}

done_testing();
