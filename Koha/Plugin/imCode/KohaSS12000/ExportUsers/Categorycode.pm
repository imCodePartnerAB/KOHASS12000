# Copyright (C) 2023 imCode, https://www.imcode.com, <info@imcode.com>
#
# This is a plugin for Koha
# It exports user data from the API in SS12000 format to your Koha database
#
# Category: Koha, https://koha-community.org 
# Plugin:   imCode::KohaSS12000::ExportUsers
# Author:   Tkachuk Serge, https://github.com/fly304625, <tkachuk.serge@gmail.com>
# License:  https://www.gnu.org/licenses/gpl-3.0.html GNU General Public License v3.0
#
package Koha::Plugin::imCode::KohaSS12000::ExportUsers::CategoryCode;

sub fetchCategoryCode {
    my (
        $response_data,
        $api_limit
        ) = @_;

    my $out = "";
    for my $i (1..$api_limit) {
        my $response_page_data = $response_data->{data}[$i-1];
        if ($response_page_data) { 
                    my $id = $response_page_data->{id};
                    my $displayName = $response_page_data->{displayName};
                    my $organisationType = $response_page_data->{organisationType};
                    my $municipalityCode = $response_page_data->{municipalityCode};
                    my $address = $response_page_data->{address}; # array: type, streetAddress, locality, postalCode
                    my $organisationCode = $response_page_data->{organisationCode};
                    my $organisationNumber = $response_page_data->{organisationNumber};
                    my $schoolUnitCode = $response_page_data->{schoolUnitCode};
                    my $schoolTypes = $response_page_data->{schoolTypes}; # object
                    my $email = $response_page_data->{email};
                    my $phoneNumber = $response_page_data->{phoneNumber};
                    my $parentOrganisation = $response_page_data->{parentOrganisation}; # "id":
                    $out .= "$i : $organisationCode; ";

                    # my $response_page_token = $response_page_data->{pageToken};
        }
    }

    return $out;
}

sub addOrUpdateCategoryCode {

}

1;