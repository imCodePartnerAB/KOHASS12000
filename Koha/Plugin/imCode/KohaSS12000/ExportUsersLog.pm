# Copyright (C) 2024-2026 imCode, https://www.imcode.com, <info@imcode.com>
#
# Companion to Koha::Plugin::imCode::KohaSS12000::ExportUsers.
# Koha lists one link per plugin on Administration > Plugins. This tiny plugin adds a
# second entry that opens the log page of the SS12000 plugin. It has no settings of its own.
#
# Plugin:   imCode::KohaSS12000::ExportUsersLog
# License:  https://www.gnu.org/licenses/gpl-3.0.html GNU General Public License v3.0
#
package Koha::Plugin::imCode::KohaSS12000::ExportUsersLog;

use Modern::Perl;

use base qw(Koha::Plugins::Base);

use Koha::Plugin::imCode::KohaSS12000::ExportUsers ();

our $VERSION = "1.91";

our $metadata = {
    name            => 'Imported SS12000 users (log)',
    author          => 'imCode.com',
    date_authored   => '2026-11-17',
    date_updated    => '2026-11-17',
    minimum_version => '20.05',
    maximum_version => undef,
    version         => $VERSION,
    description     => 'Log of the SS12000 import: import runs and changes made to patrons',
};

our $main_class = 'Koha::Plugin::imCode::KohaSS12000::ExportUsers';

sub new {
    my ( $class, $args ) = @_;

    # name and description come from the language files of the main plugin (ExportUsers/<code>.po)
    my $lang = Koha::Plugin::imCode::KohaSS12000::ExportUsers::i18n_language( $args->{cgi} );
    $args->{'metadata'} = {
        %$metadata,
        class       => $class,
        name        => Koha::Plugin::imCode::KohaSS12000::ExportUsers::i18n_tr( $lang, $metadata->{name} ),
        description => Koha::Plugin::imCode::KohaSS12000::ExportUsers::i18n_tr( $lang, $metadata->{description} ),
    };

    return $class->SUPER::new($args);
}

# Opened from Administration > Plugins: send the user on to the log page
sub admin {
    my ( $self, $args ) = @_;
    my $cgi = $self->{'cgi'};

    ( my $encoded = $main_class ) =~ s/:/%3A/g;
    print $cgi->redirect("/cgi-bin/koha/plugins/run.pl?class=$encoded&method=admin&op=log");
    return;
}

sub install   { return 1 }
sub upgrade   { return 1 }
sub uninstall { return 1 }

1;
