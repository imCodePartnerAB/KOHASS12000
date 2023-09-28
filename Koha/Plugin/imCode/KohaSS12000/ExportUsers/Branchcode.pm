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

# In your example:
# GET {{fullUrl}}/organisations/23fcdbfd-9a46-4a2e-a31f-f4aadf9fb38e
# HTTP/1.1 200 OK
# Server: nginx
# Date: Wed, 20 Sep 2023 13:14:09 GMT
# Content-Type: application/json
# Transfer-Encoding: chunked
# Connection: close
# Access-Control-Allow-Origin: *
# x-ist-ss12000-instance: hopeful_Prune
# x-ist-ss12000-status-time: 2023-09-20T12:37:44.517Z
# x-ist-ss12000-updated: 2023-09-20T12:37:44.517Z
# Content-Encoding: gzip

# {
#   "displayName": "Pettersbergsskolan",
#   "id": "23fcdbfd-9a46-4a2e-a31f-f4aadf9fb38e",
#   "meta": {
#     "created": "2023-09-04T03:39:48.904Z",
#     "modified": "2023-09-05T11:58:52.327Z"
#   },
#   "organisationType": "Skolenhet",
#   "municipalityCode": "1980",
#   "address": {
#     "type": "Besöksadress",
#     "streetAddress": "Pettersbergsgatan 39",
#     "locality": "VÄSTERÅS",
#     "postalCode": "721 87"
#   },
#   "organisationCode": "PBGR",
#   "organisationNumber": "2120002080",
#   "schoolUnitCode": "66142210",
#   "schoolTypes": [
#     "GR"
#   ],
#   "startDate": "2016-07-01",
#   "email": "ulrika2.eriksson@vasteras.se",
#   "phoneNumber": "021-39 08 20",
#   "parentOrganisation": {
#     "id": "a1bd3b4b-2bb4-4d10-8143-647c657ec4ca"
#   }
# }

# 4:15
# This is not the same as the test koha you are working towards though.
# 4:16
# But I am thinking that we want to map the "organisationCode": "PBGR" to PET

# Serge Tkachuk  4:34 PM
# organisationCode this Categorycode or Branchcode?

# Jacob Sandin  4:58 PM
# Branch


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

