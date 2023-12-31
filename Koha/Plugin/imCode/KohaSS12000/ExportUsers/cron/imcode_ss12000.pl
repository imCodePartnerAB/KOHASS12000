#!/usr/bin/perl

use Modern::Perl;

use Try::Tiny;

use C4::Context;
use C4::Log;
use Koha::Logger;
use Koha::Plugins;

use Koha::Plugin::imCode::KohaSS12000::ExportUsers;

my $plugin = Koha::Plugin::imCode::KohaSS12000::ExportUsers->new;
$plugin->cronjob("persons");
print "Cron job, persons, completed.\n";

=head1 NAME

imcode_ss12000.pl - Run imCode::KohaSS12000::ExportUsers

=head1 SYNOPSIS

imcode_ss12000.pl

=head1 AUTHOR

ImCode

=head1 LICENSE

=cut
