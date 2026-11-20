#!/usr/bin/perl

use Modern::Perl;

use C4::Context;
use Koha::Plugins;

use Koha::Plugin::imCode::KohaSS12000::ExportUsers;

my $plugin = Koha::Plugin::imCode::KohaSS12000::ExportUsers->new;
exit $plugin->tick_cli(@ARGV);

=head1 NAME

imcode_ss12000_tick.pl - once a minute: heartbeat, scheduled runs and "run now" requests

=head1 SYNOPSIS

imcode_ss12000_tick.pl                       prints IDLE, JOB:<id>:import or DONE:<id>:<status>
imcode_ss12000_tick.pl --finish <id> <done|failed> [message]
imcode_ss12000_tick.pl --release <id>

=cut
