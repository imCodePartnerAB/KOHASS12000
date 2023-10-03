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
# $data_endpoint = duties
# https://imcode.slack.com/archives/D04FC5N9C2W/p1695218323829439
# 
# So dutyRole should be mapable to categorycode.
# However if the person is not in duty, there has to be a default wich in this case is ELEV (Pupil) but it should be configurable.
# 
package Koha::Plugin::imCode::KohaSS12000::ExportUsers::Categorycode;

our $borrowers_table  = 'borrowers'; # Koha users table
our $categories_table = 'categories'; # Koha categories table
our $branches_table   = 'branches'; # Koha branches table
our $logs_table       = 'imcode_logs';
our $added_count      = 0; # to count added
our $updated_count    = 0; # to count updated

sub fetchCategoryCode {
    my (
        $response_data, 
        $api_limit, 
        $debug_mode, 
        $response_page_token,
        $data_hash  
        ) = @_;

        my $j = 1;
        for my $i (1..$api_limit) {
            my $response_page_data = $response_data->{data}[$i-1];
            if ($response_page_data) { 
                        my $id = $response_page_data->{id};
                        my $dutyRole = $response_page_data->{dutyRole}; # field 'categorycode' in DB
                        my $signature = $response_page_data->{signature};
                        my $startDate = $response_page_data->{startDate};
                        my $endDate = $response_page_data->{endDate};

            }
            
            my $result = addOrUpdateCategoryCode(
                    uc($dutyRole),
                    $signature,
                    $startDate,
                    $endDate
                    );
            if ($result) { $j++; } 
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

sub addOrUpdateCategoryCode {
    my (
        $dutyRole,
        $signature,
        $startDate,
        $endDate
        ) = @_;

    my $dbh = C4::Context->dbh;

    ## Check if a user with the specified cardnumber already exists in the database
    my $select_query = qq{
        SELECT categorycode 
        FROM $categories_table 
        WHERE categorycode = ?
    };
    my $select_sth = $dbh->prepare($select_query);
    $select_sth->execute($dutyRole);
    my $existing_category = $select_sth->fetchrow_hashref;

    if ($existing_category) {
        # If the branch exists, update their data
        my $update_query = qq{
            UPDATE $categories_table 
            SET 
                categorycode = ?
            WHERE categorycode = ?
        };
        my $update_sth = $dbh->prepare($update_query);
        $update_sth->execute(
                $dutyRole,
                $existing_branche->{'categorycode'}
            );

        $updated_count++;
    } else {
        # If the branch doesn't exist, insert their data
        my $insert_query = qq{
            INSERT INTO $categories_table  (
                categorycode
                )
            VALUES (?)
        };
        my $insert_sth = $dbh->prepare($insert_query);
        $insert_sth->execute(
                $dutyRole
            );
            $added_count++;
    }
}

sub addOrUpdateCategoryCode2 {

# INSERT INTO `categories` (
#     `categorycode`, `description`, `enrolmentperiod`, `enrolmentperioddate`, `upperagelimit`, `dateofbirthrequired`, 
#     `finetype`, `bulk`, `enrolmentfee`, `overduenoticerequired`, `issuelimit`, `reservefee`, `hidelostitems`, `category_type`, 
#     `BlockExpiredPatronOpacActions`, `default_privacy`, `checkprevcheckout`, `reset_password`, `change_password`, `min_password_length`, 
#     `require_strong_password`, `exclude_from_local_holds_priority`)
# VALUES
# 	('PERSONAL', 'Personal', 999, NULL, NULL, NULL, NULL, NULL, 0.000000, 1, NULL, 0.000000, 0, 'S', -1, 'default', 'no', NULL, NULL, NULL, NULL, NULL);



}

1;