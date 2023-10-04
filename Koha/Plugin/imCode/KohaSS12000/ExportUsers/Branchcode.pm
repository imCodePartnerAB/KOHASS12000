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
# $data_endpoint = organisations
# https://imcode.slack.com/archives/D04FC5N9C2W/p1695215674248579
# 
package Koha::Plugin::imCode::KohaSS12000::ExportUsers::Branchcode;

our $borrowers_table  = 'borrowers'; # Koha users table
our $categories_table = 'categories'; # Koha categories table
our $branches_table   = 'branches'; # Koha branches table
our $logs_table       = 'imcode_logs';
our $added_count      = 0; # to count added
our $updated_count    = 0; # to count updated

sub fetchBranchCode {
    my (
        $response_data,
        $api_limit,
        $debug_mode,
        $response_page_token,
        $data_hash
        ) = @_;

        my $dbh = C4::Context->dbh;

        my $j = 1;
        for my $i (1..$api_limit) {
            my $response_page_data = $response_data->{data}[$i-1];
            if ($response_page_data) { 
                        my $id = $response_page_data->{id};
                        my $displayName = $response_page_data->{displayName};
                        my $organisationType = $response_page_data->{organisationType};
                        my $municipalityCode = $response_page_data->{municipalityCode};

                        my $address = $response_page_data->{address}; # array: type, streetAddress, locality, postalCode
                        # warn "address: $address";
                        # "type": "Besöksadress",
                        # "type": "Postadress",
                        my $streetAddress = "";
                        my $locality = "";
                        my $postalCode = "";
                        my $type = "";
                        if (defined $address && ref $address eq 'HASH') {
                                $type = $address->{type};
                                # warn "fetchBranchCode type : $type";
                                if (defined $type && length($type) > 1 && $type =~ /Bes.?ksadress/) {
                                    if (defined $address->{streetAddress}) {               
                                        $streetAddress = ucfirst(lc($address->{streetAddress})); # field 'branchaddress1' in DB
                                    }
                                    if (defined $address->{locality}) {
                                        $locality = ucfirst(lc($address->{locality})); # field 'branchcity' ?
                                    }
                                    $postalCode = $address->{postalCode}; # field 'branchzip'
                                }
                        }
                        # warn "BranchCode streetAddress : $streetAddress";

                        my $organisationCode = $response_page_data->{organisationCode}; # field 'branchcode' in DB
                        my $organisationNumber = $response_page_data->{organisationNumber};
                        my $schoolUnitCode = $response_page_data->{schoolUnitCode};
                        my $schoolTypes = $response_page_data->{schoolTypes}; # object [] ???
                        my $email = $response_page_data->{email};
                        my $phoneNumber = $response_page_data->{phoneNumber};
                        my $parentOrganisation = $response_page_data->{parentOrganisation}; # "id":

                        if ($organisationCode) {
                            my $result = addOrUpdateBranchCode(
                                    uc($organisationCode),
                                    $displayName,
                                    $streetAddress,
                                    $postalCode,
                                    $locality,
                                    $phoneNumber,
                                    $email,

                                    $organisationType,
                                    $municipalityCode,
                                    $organisationNumber,
                                    $schoolUnitCode,
                                    $organisationCode
                                );
                        if ($result) { $j++; }                                
                        } else { $j++; }
            }
        }
            
        if ($j == $api_limit) {
                if ($debug_mode eq "No") { 
                    my $update_query = qq{
                        UPDATE $logs_table
                        SET is_processed = 1,
                            response = ?
                        WHERE data_hash = ?
                    };
                    my $update_response = "Added: $added_count, Updated: $updated_count";
                    my $sth_update = $dbh->prepare($update_query);
                    unless ($sth_update->execute($update_response, $data_hash)) {
                        die "An error occurred while executing the request: " . $sth_update->errstr;
                    }
                    $sth_update->finish();
                } elsif ($debug_mode eq "Yes") {
                    my $update_query = qq{
                        UPDATE $logs_table
                        SET is_processed = 1
                        WHERE data_hash = ?
                    };
                    my $sth_update = $dbh->prepare($update_query);
                    unless ($sth_update->execute($data_hash)) {
                        die "An error occurred while executing the request: " . $sth_update->errstr;
                    }
                    $sth_update->finish();
                }
        }

        if (!defined $response_page_token || $response_page_token eq "") {
            warn "EndLastPageFromAPI"; # last page from API, flag for bash script Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run.sh
        }

}

sub addOrUpdateBranchCode {
    my (
            $organisationCode,
            $displayName,
            $streetAddress,
            $postalCode,
            $locality,
            $phoneNumber,
            $email,

            $organisationType,
            $municipalityCode,
            $organisationNumber,
            $schoolUnitCode,
            $marcorgcode
        ) = @_;

    my $branchnotes = "$organisationType : $municipalityCode : $organisationNumber : $schoolUnitCode";
    my $dbh = C4::Context->dbh;

    ## Check if a branch with the specified branchcode already exists in the database
    my $select_query = qq{
        SELECT branchcode 
        FROM $branches_table 
        WHERE branchcode = ?
    };
    my $select_sth = $dbh->prepare($select_query);
    $select_sth->execute($organisationCode);
    my $existing_branche = $select_sth->fetchrow_hashref;

    if ($existing_branche) {
        # If the branch exists, update their data
        my $update_query = qq{
            UPDATE $branches_table 
            SET 
                branchname = ?, 
                branchaddress1 = ?, 
                branchzip = ?, 
                branchcity = ?, 
                branchphone = ?, 
                branchemail = ?,
                branchnotes = ?,
                marcorgcode = ?
            WHERE branchcode = ?
        };
        my $update_sth = $dbh->prepare($update_query);
        $update_sth->execute(
                $displayName,
                $streetAddress,
                $postalCode,
                $locality,
                $phoneNumber,
                $email,
                $branchnotes,
                $marcorgcode,
                $existing_branche->{'branchcode'}
            );

        $updated_count++;
    } else {
        # If the branch doesn't exist, insert their data
        my $insert_query = qq{
            INSERT INTO $branches_table  (
                branchcode, 
                branchname, 
                branchaddress1, 
                branchzip, 
                branchcity, 
                branchphone, 
                branchemail,
                branchnotes,
                marcorgcode
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        };
        my $insert_sth = $dbh->prepare($insert_query);
        $insert_sth->execute(
                $organisationCode,
                $displayName,
                $streetAddress,
                $postalCode,
                $locality,
                $phoneNumber,
                $email,
                $branchnotes,
                $marcorgcode
            );
            $added_count++;
    }
}


1;

