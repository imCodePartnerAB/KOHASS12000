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
package Koha::Plugin::imCode::KohaSS12000::ExportUsers::BranchCode;

sub fetchBranchCode {
    my (
        $response_data,
        $api_limit
        ) = @_;

    my $out = "";
    for my $i (1..$api_limit) {
        my $response_page_data = $response_data->{data}[$i-1];
        if ($response_page_data) { 
                    my $id = $response_page_data->{id};
                    my $dutyRole = $response_page_data->{dutyRole};
                    my $signature = $response_page_data->{signature};
                    my $startDate = $response_page_data->{startDate};
                    my $endDate = $response_page_data->{endDate};
                    $out .= "$i : $dutyRole; ";

                    # my $response_page_token = $response_page_data->{pageToken};
        }
    }

    return $out;
}

sub addOrUpdateBranchCode {
    
}

1;

