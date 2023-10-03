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
# $data_endpoint = persons
# 
# 
package Koha::Plugin::imCode::KohaSS12000::ExportUsers::Borrowers;

our $borrowers_table  = 'borrowers'; # Koha users table
our $categories_table = 'categories'; # Koha categories table
our $branches_table   = 'branches'; # Koha branches table
our $logs_table       = 'imcode_logs';
our $added_count      = 0; # to count added
our $updated_count    = 0; # to count updated

sub fetchBorrowers {
    my (
        $response_data,
        $api_limit,
        $debug_mode,
        $koha_default_categorycode, 
        $koha_default_branchcode,
        $cardnumberPlugin,
        $useridPlugin,
        $response_page_token,
        $data_hash
        ) = @_;

    # warn "Borrowers api_limit: $api_limit";

    my $dbh = C4::Context->dbh;

            my $j = 1;
            for my $i (1..$api_limit) {
                my $response_page_data = $response_data->{data}[$i-1];
                if ($response_page_data) {
                    my $id = $response_page_data->{id};
                    my $givenName = $response_page_data->{givenName};
                    my $familyName = $response_page_data->{familyName};
                    my $birthDate = $response_page_data->{birthDate};
                    my $sex = $response_page_data->{sex};
                    if (defined $sex) {
                        if ($sex eq "Man") {
                            $sex = "M";
                        } elsif ($sex eq "Kvinna") {
                            $sex = "F";
                        }
                    }

                    my $emails = $response_page_data->{emails}; # we get an array
                    my $email = "";
                    my $B_email = ""; # field B_email in DB
                    if (defined $emails && ref $emails eq 'ARRAY') {
                        foreach my $selectedEmail (@$emails) {
                            my $email_value = $selectedEmail->{value};
                            my $email_type  = $selectedEmail->{type};
                            if ($email_type eq "Privat") {
                                if (defined $email_value) {
                                    $email = lc($email_value);
                                }
                            } elsif ($email_type eq "Skola personal") {
                                if (defined $email_value) {
                                    $B_email = lc($email_value);
                                }
                            }
                        }
                    }

                    my $addresses = $response_page_data->{addresses}; # we get an array
                    my $streetAddress = "";
                    my $locality = "";
                    my $postalCode = "";
                    my $country = "";
                    my $countyCode = "";
                    my $municipalityCode = "";
                    my $realEstateDesignation = "";
                    my $type = "";
                    if (defined $addresses && ref $addresses eq 'ARRAY') {
                       foreach my $selectedAddresses (@$addresses) {
                            $type = $selectedAddresses->{type};
                            if (defined $type && length($type) > 1 && $type =~ /Folkbokf.?ring/) {
                                if (defined $selectedAddresses->{streetAddress}) {               
                                    $streetAddress = ucfirst(lc($selectedAddresses->{streetAddress})); # field 'addresses' in DB
                                }
                                if (defined $selectedAddresses->{locality}) {
                                    $locality = ucfirst(lc($selectedAddresses->{locality})); # field 'city' ?
                                }
                                $postalCode = $selectedAddresses->{postalCode}; # field 'zipcode'
                                $country = $selectedAddresses->{country}; # field 'country'
                                $countyCode = $selectedAddresses->{countyCode}; # now not use
                                $municipalityCode = $selectedAddresses->{municipalityCode}; # now not use
                                $realEstateDesignation = $selectedAddresses->{realEstateDesignation}; # now not use
                            }
                        }
                    }

                    my $phoneNumbers = $response_page_data->{phoneNumbers};
                    my $phone = "";
                    my $mobile_phone = "";
                    if (defined $phoneNumbers && ref $phoneNumbers eq 'ARRAY') {
                        foreach my $selectedPhone (@$phoneNumbers) {
                            my $phone_value = $selectedPhone->{value};
                            my $is_mobile = $selectedPhone->{mobile};
                            if ($is_mobile) {
                                $mobile_phone = $phone_value;
                            } else {
                                $phone = $phone_value;
                            }
                        }
                    }

                    my $cardnumber;
                    my $civicNo = $response_page_data->{civicNo};
                    if (defined $civicNo && ref $civicNo eq 'HASH') {
                            # $nationality = $civicNo->{nationality};
                            $cardnumber = $civicNo->{value}; 
                    }

                    my $externalIdentifier;
                    my $externalIdentifiers = $response_page_data->{externalIdentifiers};
                    if (defined $externalIdentifiers && ref $externalIdentifiers eq 'ARRAY') {
                        foreach my $selectedIdentifier (@$externalIdentifiers) {
                            $externalIdentifier = $selectedIdentifier->{value}; 
                            # warn $givenName. " - ".$externalIdentifier;
                        }
                    }

                    my $userid = $cardnumber;

                    if (!defined $email || $email eq "") { $email = undef; }

                    my $result = addOrUpdateBorrower(
                            $cardnumber, 
                            $familyName, 
                            $givenName, 
                            $birthDate, 
                            $email, 
                            $sex, 
                            $phone, 
                            $mobile_phone, 
                            $koha_default_categorycode, 
                            $koha_default_branchcode,
                            $streetAddress,
                            $locality,
                            $postalCode,
                            $country,
                            $B_email,
                            $userid,
                            $useridPlugin,
                            $cardnumberPlugin,
                            $externalIdentifier,
                        );
                    if ($result) { $j++; }
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

# Function to add or update user data in the borrowers table
sub addOrUpdateBorrower {
    my (
        $cardnumber, 
        $surname, 
        $firstname, 
        $birthdate, 
        $email, 
        $sex, 
        $phone, 
        $mobile_phone, 
        $categorycode, 
        $branchcode, 
        $streetAddress,
        $locality,
        $postalCode,
        $country,
        $B_email,
        $userid,
        $useridPlugin,
        $cardnumberPlugin,
        $externalIdentifier,
        ) = @_;

    # use utf8;

    my $dbh = C4::Context->dbh;
    
    my $newUserID;
    my $newCardnumber;

    if ($useridPlugin eq "civicNo" || $useridPlugin eq "externalIdentifier") {
        $newUserID = ($useridPlugin eq "civicNo") ? $userid : $externalIdentifier;
    }

    if ($cardnumberPlugin eq "civicNo" || $cardnumberPlugin eq "externalIdentifier") {
        $newCardnumber = ($cardnumberPlugin eq "civicNo") ? $cardnumber : $externalIdentifier;
    }

    ## Check if a user with the specified cardnumber already exists in the database
    my $select_query = qq{
        SELECT borrowernumber 
        FROM $borrowers_table 
        WHERE cardnumber = ? OR cardnumber = ?
    };
    my $select_sth = $dbh->prepare($select_query);
    $select_sth->execute($cardnumber, $externalIdentifier);
    my $existing_borrower = $select_sth->fetchrow_hashref;

    if ($existing_borrower) {
        # If the user exists, update their data
        my $update_query = qq{
            UPDATE $borrowers_table
            SET 
                dateofbirth = ?, 
                email = ?, 
                sex = ?, 
                phone = ?, 
                mobile = ?, 
                surname = ?, 
                firstname = ?,
                categorycode = ?,
                branchcode = ?,
                address = ?,
                city = ?,
                zipcode = ?,
                country = ?,
                B_email = ?,
                userid = ?,
                cardnumber = ?
            WHERE borrowernumber = ?
        };
        my $update_sth = $dbh->prepare($update_query);
        $update_sth->execute(
                $birthdate, 
                $email, 
                $sex, 
                $phone, 
                $mobile_phone, 
                $surname, 
                $firstname, 
                $categorycode, 
                $branchcode, 
                $streetAddress,
                $locality,
                $postalCode,
                $country,
                $B_email,
                $newUserID,
                $newCardnumber,
                $existing_borrower->{'borrowernumber'}
            );

        $updated_count++;
    } else {
        # If the user doesn't exist, insert their data
        my $insert_query = qq{
            INSERT INTO $borrowers_table (
                    cardnumber,
                    surname,
                    firstname,
                    dateofbirth,
                    email,
                    sex,
                    phone,
                    mobile,
                    categorycode,
                    branchcode,
                    address,
                    city,
                    zipcode,
                    country,
                    B_email,
                    userid
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        };
        my $insert_sth = $dbh->prepare($insert_query);
        $insert_sth->execute(
                $newCardnumber,
                $surname,
                $firstname,
                $birthdate,
                $email,
                $sex,
                $phone,
                $mobile_phone,
                $categorycode,
                $branchcode,
                $streetAddress,
                $locality,
                $postalCode,
                $country,
                $B_email,
                $newUserID
            );
            $added_count++;
    }
}


1;