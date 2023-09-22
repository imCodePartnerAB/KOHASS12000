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
package Koha::Plugin::imCode::KohaSS12000::ExportUsers;

use Modern::Perl;
use C4::Auth;
use C4::Matcher;

use strict;
use warnings;
use JSON qw(decode_json encode_json);

use Data::Dumper;

use LWP::UserAgent;
use HTTP::Request::Common;

use base qw(Koha::Plugins::Base);
use Digest::MD5;

use Cwd qw( abs_path cwd );
use Locale::Messages;
Locale::Messages->select_package('gettext_pp');

use Locale::Messages qw(:locale_h :libintl_h);
use POSIX qw(setlocale);
use Encode;

# set locale settings for gettext
my $self = new('Koha::Plugin::imCode::KohaSS12000::ExportUsers');
my $cgi  = $self->{'cgi'};

my $locale = C4::Languages::getlanguage($cgi);
$locale = substr( $locale, 0, 2 );
$ENV{'LANGUAGE'} = $locale;
setlocale Locale::Messages::LC_ALL(), '';
textdomain "com.imcode.exportusers";

my $locale_path = abs_path( $self->mbf_path('translations') );
bindtextdomain "com.imcode.exportusers" => $locale_path;

our $VERSION = "1.1";

our $metadata = {
    name            => getTranslation('Export Users from SS12000'),
    author          => 'imCode.com',
    date_authored   => '2023-08-08',
    date_updated    => '2023-09-18',
    minimum_version => '20.05.00.000',
    maximum_version => undef,
    version         => $VERSION,
    description     => getTranslation('This plugin implements export users from SS12000')
};

our $config_table     = 'imcode_config';
our $logs_table       = 'imcode_logs';
our $skey             = 'Uq9crAvPDNkkQcXAwsEHkjGwBwnSvDPC';  # Encryption key for ist_client_secret, change it if necessary
our $borrowers_table  = 'borrowers'; # Koha users table
our $categories_table = 'categories'; # Koha categories table
our $branches_table   = 'branches'; # Koha branches table
our $added_count      = 0; # to count added
our $updated_count    = 0; # to count updated
our $data_change_log  = 'imcode_data_change_log';

sub new {
    my ( $class, $args ) = @_;

    ## We need to add our metadata here so our base class can access it
    $args->{'metadata'} = $metadata;
    $args->{'metadata'}->{'class'} = $class;

    ## Here, we call the 'new' method for our base class
    ## This runs some additional magic and checking
    ## and returns our actual $self
    my $self = $class->SUPER::new($args);

    return $self;
}

sub install {
    my ( $self, $args ) = @_;

    my @installer_statements = (qq{CREATE TABLE IF NOT EXISTS $config_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL);},
    qq{CREATE TABLE IF NOT EXISTS $data_change_log (
        log_id INT AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(255),
        record_id INT,
        action VARCHAR(255),
        change_description TEXT,
        change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );},
    qq{CREATE TABLE IF NOT EXISTS $logs_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    page_token_next VARCHAR(255) DEFAULT NULL,
    response text COLLATE utf8mb4_unicode_ci,
    record_count int(11) DEFAULT NULL,
    is_processed tinyint(1) DEFAULT NULL,
    data_hash varchar(255) DEFAULT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP);},
    qq{INSERT INTO $config_table (name,value) VALUES ('ist_client_id','your_client_id');},
    qq{INSERT INTO $config_table (name,value) VALUES ('ist_client_secret','');},
    qq{INSERT INTO $config_table (name,value) VALUES ('ist_customer_id','your_customerId');},
    qq{INSERT INTO $config_table (name,value) VALUES ('ist_api_url','https://api.ist.com');},
    qq{INSERT INTO $config_table (name,value) VALUES ('ist_oauth_url','https://skolid.se/connect/token');},
    qq{INSERT INTO $config_table (name,value) VALUES ('koha_default_categorycode','');},
    qq{INSERT INTO $config_table (name,value) VALUES ('koha_default_branchcode','');},
    qq{INSERT INTO $config_table (name,value) VALUES ('debug_mode','No');},
    qq{INSERT INTO $config_table (name,value) VALUES ('api_limit','30');},
    qq{INSERT INTO $config_table (name,value) VALUES ('cardnumberPlugin','civicNo');},
    qq{INSERT INTO $config_table (name,value) VALUES ('useridPlugin','civicNo');},
    qq{INSERT INTO $config_table (name,value) VALUES ('logs_limit','3');},
    qq{INSERT INTO $config_table (name,value) VALUES ('update_on','INSERT');},
    );

    eval {
        for (@installer_statements) {
            my $sth = C4::Context->dbh->prepare($_);
            $sth->execute or die C4::Context->dbh->errstr;
        }
    };

    if ($@) {
        warn "Install Error: $@";
        return 0;
    }

    # LOG trigger in SQL
    my $trigger_sql = qq{
    DELIMITER //
    CREATE TRIGGER log_user_changes
    AFTER UPDATE ON $borrowers_table
    FOR EACH ROW
    BEGIN
        DECLARE change_description TEXT;
        
        IF NEW.dateofbirth != OLD.dateofbirth THEN
            SET change_description = CONCAT('field dateofbirth changed from "', OLD.dateofbirth, '" to "', NEW.dateofbirth, '"');
        END IF;

        IF NEW.phone != OLD.phone THEN
            SET change_description = CONCAT('field phone changed from "', OLD.phone, '" to "', NEW.phone, '"');
        END IF;

        IF NEW.mobile != OLD.mobile THEN
            SET change_description = CONCAT('field mobile changed from "', OLD.mobile, '" to "', NEW.mobile, '"');
        END IF;

        IF NEW.surname != OLD.surname THEN
            SET change_description = CONCAT('field surname changed from "', OLD.surname, '" to "', NEW.surname, '"');
        END IF;

        IF NEW.firstname != OLD.firstname THEN
            SET change_description = CONCAT('field firstname changed from "', OLD.firstname, '" to "', NEW.firstname, '"');
        END IF;

        IF NEW.categorycode != OLD.categorycode THEN
            SET change_description = CONCAT('field categorycode changed from "', OLD.categorycode, '" to "', NEW.categorycode, '"');
        END IF;

        IF NEW.branchcode != OLD.branchcode THEN
            SET change_description = CONCAT('field branchcode changed from "', OLD.branchcode, '" to "', NEW.branchcode, '"');
        END IF;

        IF NEW.address != OLD.address THEN
            SET change_description = CONCAT('field address changed from "', OLD.address, '" to "', NEW.address, '"');
        END IF;

        IF NEW.city != OLD.city THEN
            SET change_description = CONCAT('field city changed from "', OLD.city, '" to "', NEW.city, '"');
        END IF;

        IF NEW.zipcode != OLD.zipcode THEN
            SET change_description = CONCAT('field zipcode changed from "', OLD.zipcode, '" to "', NEW.zipcode, '"');
        END IF;

        IF NEW.country != OLD.country THEN
            SET change_description = CONCAT('field country changed from "', OLD.country, '" to "', NEW.country, '"');
        END IF;

        IF NEW.B_email != OLD.B_email THEN
            SET change_description = CONCAT('field B_email changed from "', OLD.B_email, '" to "', NEW.B_email, '"');
        END IF;

        IF NEW.userid != OLD.userid THEN
            SET change_description = CONCAT('field userid changed from "', OLD.userid, '" to "', NEW.userid, '"');
        END IF;

        IF NEW.cardnumber != OLD.cardnumber THEN
            SET change_description = CONCAT('field cardnumber changed from "', OLD.cardnumber, '" to "', NEW.cardnumber, '"');
        END IF;      

        IF NEW.sex != OLD.sex THEN
            SET change_description = CONCAT('field sex changed from "', OLD.sex, '" to "', NEW.sex, '"');
        END IF;
        
        IF NEW.email != OLD.email THEN
            SET change_description = CONCAT(change_description, '; Email changed from "', OLD.email, '" to "', NEW.email, '"');
        END IF;
        
        IF change_description IS NOT NULL THEN
            INSERT INTO $data_change_log (table_name, record_id, action, change_description)
            VALUES ('$borrowers_table', NEW.borrowernumber, 'update', change_description);
        END IF;
    END;
    //
    DELIMITER ;
    };

    eval {
        my $trigger_sth = C4::Context->dbh->prepare($trigger_sql);
        $trigger_sth->execute or die C4::Context->dbh->errstr;
    };

    if ($@) {
        warn "Install, CREATE TRIGGER log_user_changes, Error: $@";
        return 0;
    }


    return 1;
}

sub uninstall {
    my ( $self, $args ) = @_;

    my $dbh = C4::Context->dbh;

    my @tables_to_delete = ($config_table, $logs_table); 

    eval {
        foreach my $table (@tables_to_delete) {
            my $table_deletion_query = "DROP TABLE IF EXISTS $table";
            $dbh->do($table_deletion_query);
        }
    };

    if ($@) {
        warn "Error deleting table: $@";
        return 0;
    }

    return 1;
}

sub configure {
    my ($self, $args) = @_;

    my $cgi = $self->{'cgi'};
    my $dbh = C4::Context->dbh;

    my $op = $cgi->param('op') || '';

    my $missing_modules = 0;
    eval {
            require URI::Encode;
            URI::Encode->import(qw(uri_encode));
    };
    if ($@) {
        warn "Missing required module: URI::Encode qw(uri_encode) \n";
        $missing_modules = 1;
    }

    if ($missing_modules) {
        my $template = $self->get_template({ file => 'error.tt' });
        $template->param(
            error    => "missing_modules",
            language => C4::Languages::getlanguage($cgi) || 'en',
            mbf_path => abs_path( $self->mbf_path('translations') )
        );

        print $cgi->header(-type => 'text/html', -charset => 'utf-8');
        print $template->output();
        return 0;
    }

    my $template = $self->get_template({ file => 'config.tt' });

    if ($op eq 'save-config') {
        my $client_id     = $cgi->param('client_id');
        my $client_secret = xor_encrypt($cgi->param('client_secret'), $skey);
        my $customerId    = $cgi->param('customerId');
        my $api_url       = $cgi->param('api_url');
        my $oauth_url     = $cgi->param('oauth_url');
        my $debug_mode    = $cgi->param('debug_mode');
        my $api_limit     = int($cgi->param('api_limit'));
        my $koha_default_categorycode = $cgi->param('koha_default_categorycode');
        my $koha_default_branchcode   = $cgi->param('koha_default_branchcode');
        my $cardnumberPlugin    = $cgi->param('cardnumberPlugin');
        my $useridPlugin        = $cgi->param('useridPlugin');
        my $logs_limit          = int($cgi->param('logs_limit'));
        my $update_on           = $cgi->param('update_on');

        my $select_check_query = qq{
            SELECT name, value 
            FROM $config_table
            WHERE name in ('cardnumberPlugin','useridPlugin')
        };
        my $sth_select_check = $dbh->prepare($select_check_query);
        $sth_select_check->execute();
        my %config_values;
        while (my ($name, $value) = $sth_select_check->fetchrow_array) {
            $config_values{$name} = $value;
        }
        my $checkCardnumberPlugin = $config_values{'cardnumberPlugin'};
        my $checkUseridPlugin = $config_values{'useridPlugin'};

        if (
                defined $checkCardnumberPlugin && 
                defined $checkUseridPlugin && 
                ($checkCardnumberPlugin ne $cardnumberPlugin || $checkUseridPlugin ne $useridPlugin)
            ) {
                my $delete_query = qq{TRUNCATE $logs_table};
                my $sth_delete = $dbh->prepare($delete_query);
                eval {
                    if ($sth_delete->execute()) {
                        warn "Deleted old records from $logs_table. Configuration change \n";
                    } else {
                        die "Error deleting data from $logs_table: " . $dbh->errstr . "\n";
                    }
                };
                if ($@) {
                    warn "Database error: $@\n";
                }
        }

        my $update_query = qq{
            UPDATE $config_table
            SET value = CASE
                WHEN name = 'ist_client_id' THEN ?
                WHEN name = 'ist_client_secret' THEN ?
                WHEN name = 'ist_customer_id' THEN ?
                WHEN name = 'ist_api_url' THEN ?
                WHEN name = 'ist_oauth_url' THEN ?
                WHEN name = 'koha_default_categorycode' THEN ?
                WHEN name = 'koha_default_branchcode' THEN ?
                WHEN name = 'debug_mode' THEN ?
                WHEN name = 'api_limit' THEN ?
                WHEN name = 'cardnumberPlugin' THEN ?
                WHEN name = 'useridPlugin' THEN ?
                WHEN name = 'logs_limit' THEN ?
                WHEN name = 'update_on' THEN ?                
            END
            WHERE name IN (
                'ist_client_id', 
                'ist_client_secret', 
                'ist_customer_id', 
                'ist_api_url', 
                'ist_oauth_url',
                'koha_default_categorycode',
                'koha_default_branchcode',
                'debug_mode',
                'api_limit',
                'cardnumberPlugin',
                'useridPlugin',
                'logs_limit',
                'update_on',
                )
        };

        eval {
            $dbh->do(
                $update_query, 
                undef, 
                $client_id, 
                $client_secret, 
                $customerId, 
                $api_url, 
                $oauth_url, 
                $koha_default_categorycode, 
                $koha_default_branchcode,
                $debug_mode,
                $api_limit,
                $cardnumberPlugin,
                $useridPlugin,
                $logs_limit,
                $update_on,
                );
            $template->param(success => 'success');
        };

        if ($@) {
            warn "Error updating configuration: $@";
        }
    }

    my $select_query = qq{SELECT name, value FROM $config_table};
    my $config_data  = {};

    my $select_categorycode_query = qq{SELECT categorycode FROM $categories_table};
    my $select_branchcode_query = qq{SELECT branchcode FROM $branches_table};

    my @categories;
    my @branches;

    eval {
        my $sth_categorycode = $dbh->prepare($select_categorycode_query);
        $sth_categorycode->execute();
        while (my ($category) = $sth_categorycode->fetchrow_array) {
            push @categories, $category;
        }

        my $sth_branchcode = $dbh->prepare($select_branchcode_query);
        $sth_branchcode->execute();
        while (my ($branch) = $sth_branchcode->fetchrow_array) {
            push @branches, $branch;
        }
    };

    eval {
        my $sth = $dbh->prepare($select_query);
        $sth->execute();
        while (my ($name, $value) = $sth->fetchrow_array) {
            $config_data->{$name} = $value;
        }
    };

    if ($@) {
        warn "Error fetching configuration: $@";
    }

    $template->param(
        client_id     => $config_data->{ist_client_id} || '',
        client_secret => xor_encrypt($config_data->{ist_client_secret}, $skey) || '',
        customerId    => $config_data->{ist_customer_id} || '',
        api_url       => $config_data->{ist_api_url} || '',
        oauth_url     => $config_data->{ist_oauth_url} || '',
        categories    => \@categories,
        branches      => \@branches,
        debug_mode    => $config_data->{debug_mode} || '',
        api_limit     => int($config_data->{api_limit}) || 30,
        koha_default_categorycode => $config_data->{koha_default_categorycode} || '',
        koha_default_branchcode   => $config_data->{koha_default_branchcode} || '',
        cardnumberPlugin    => $config_data->{cardnumberPlugin} || 'civicNo',
        useridPlugin        => $config_data->{useridPlugin} || 'civicNo',
        logs_limit          => int($config_data->{logs_limit}) || 3,
        language            => C4::Languages::getlanguage($cgi) || 'en',
        mbf_path            => abs_path( $self->mbf_path('translations') ),
        update_on           => $config_data->{update_on} || '',
        );

    print $cgi->header(-type => 'text/html', -charset => 'utf-8');
    print $template->output();
}


sub xor_encrypt {
    # simple, but without additional libraries and will not be stored in the database in open form
    my ($string, $key) = @_;
    my $encrypted = '';
    for my $i (0 .. length($string) - 1) {
        my $byte = ord(substr($string, $i, 1)) ^ ord(substr($key, $i % length($key), 1));
        $encrypted .= pack("C", $byte);
    }
    return $encrypted;
}


sub tool {
    my ( $self, $args ) = @_;
    
    my $cgi      = $self->{'cgi'};
    my $template = $self->get_template( { file => 'tool.tt' } );

    my $op          = $cgi->param('op') || q{};

    # For pagination 
    my $page      = $cgi->param('page') || 1; # Current page
    my $per_page  = 20; # Number of entries per page
    my $start_row = ($page - 1) * $per_page;
    my $total_rows = 0;

    my $dbh = C4::Context->dbh; # Get a database connection object

    my $select_query = qq{SELECT name, value FROM $config_table WHERE name IN ('debug_mode')};
    my %config_values; 

    eval {
        my $sth = $dbh->prepare($select_query);
        $sth->execute();
        while (my ($name, $value) = $sth->fetchrow_array) {
            $config_values{$name} = $value;
        }
    };

    if ($@) {
        warn "Error fetching configuration values: $@";
    }

    my $debug_mode = $config_values{'debug_mode'} || '';

    if ($op eq 'show-updates') {
        my @updates;

        my $search = $cgi->param('search') || q{};

        my $select_query = qq{SELECT
                    l.log_id,
                    l.record_id,
                    l.change_description,
                    l.change_timestamp,
                    b.firstname,
                    b.surname
                FROM
                    $data_change_log l
                JOIN
                    $borrowers_table b ON l.record_id = b.borrowernumber ORDER BY l.log_id DESC LIMIT $per_page OFFSET $start_row};

        eval {
            my $sth = $dbh->prepare($select_query);
            $sth->execute();
                while (my $row = $sth->fetchrow_hashref()) {
                    push @updates, $row;
                }
            };

        if ($@) {
            warn "Error fetching, info about users data update: $@";
        }

        my $count_query = qq{SELECT COUNT(*) FROM $data_change_log};

        eval {
            my $sth = $dbh->prepare($count_query);
            $sth->execute();
            ($total_rows) = $sth->fetchrow_array();
        };

        my $total_pages = int(($total_rows + $per_page - 1) / $per_page);
        my $prev_page;
        if ($page > 1) {
            $prev_page = $page - 1;
        }
        my $next_page;
        if ($page < $total_pages) {
            $next_page = $page + 1;
        }
        # Pass the data to the template for display
        $template->param(
            updates => \@updates,
            prev_page => $prev_page,
            next_page => $next_page,
            total_pages => $total_pages,
            current_page => $page,
            search => $search,
        );
    }

    if ($op eq 'show-logs') {
        my @logs;

        # Execute a query on the database, selecting data from the $logs_table
        my $query = "SELECT * FROM $logs_table ORDER BY created_at DESC LIMIT $per_page OFFSET $start_row";
        
        eval {
            my $sth = $dbh->prepare($query);
            $sth->execute();

            # Fetch the data and insert it into the template
            while (my $row = $sth->fetchrow_hashref()) {
                push @logs, $row;
            }
        };

        if ($@) {
            warn "Error fetching data from $logs_table, details: $@";
        }

        my $count_query = qq{SELECT COUNT(*) FROM $logs_table};

        eval {
            my $sth = $dbh->prepare($count_query);
            $sth->execute();
            ($total_rows) = $sth->fetchrow_array();
        };

        my $total_pages = int(($total_rows + $per_page - 1) / $per_page);
        my $prev_page;
        if ($page > 1) {
            $prev_page = $page - 1;
        }
        my $next_page;
        if ($page < $total_pages) {
            $next_page = $page + 1;
        }

        # Pass the data to the template for display
        $template->param(
            logs => \@logs,
            debug_mode => $debug_mode || '',
            prev_page => $prev_page,
            next_page => $next_page,
            total_pages => $total_pages,
            current_page => $page,
        );
    }

    if ($op eq 'show-stat') {
        my @stats;

        my $select_query = qq{SELECT
            SUM(
                SUBSTRING_INDEX (SUBSTRING_INDEX (response, 'Added: ', -1), ',', 1)
            ) AS total_added,
            SUM(
                SUBSTRING_INDEX (
                SUBSTRING_INDEX (response, 'Updated: ', -1),
                ',',
                1
                )
            ) AS total_updated,
            DATE (created_at) AS date
            FROM
            $logs_table
            GROUP BY
            DATE (created_at)};

        eval {
            my $sth = $dbh->prepare($select_query);
            $sth->execute();
                while (my $row = $sth->fetchrow_hashref()) {
                    push @stats, $row;
                }
            };

        if ($@) {
            warn "Error fetching Statistics: $@";
        }

        # Pass the data to the template for display
        $template->param(
            stats => \@stats
        );
    }

    # $self->cronjob();

    $template->param(
            language => C4::Languages::getlanguage($cgi) || 'en',
            mbf_path => abs_path( $self->mbf_path('translations') ),
    );

    print $cgi->header( -type => 'text/html', -charset => 'utf-8' );
    print $template->output();
}


sub cronjob {
    # script for run in cron here cron/imcode_ss12000.pl
    my ($self) = @_;
    $self->fetchDataFromAPI();
    return;
}

sub fetchDataFromAPI {
    my ($self, $args) = @_;

    my $cgi = $self->{'cgi'};

    my $missing_modules = 0;
    eval {
            require URI::Encode;
            URI::Encode->import(qw(uri_encode));
    };
    if ($@) {
        warn "Missing required module: URI::Encode qw(uri_encode) \n";
        $missing_modules = 1;
    }

    if ($missing_modules) {
        return 0;
    }

    my $dbh = C4::Context->dbh;

    my $select_query = qq{SELECT name, value FROM $config_table};
    my $config_data  = {};

    my $insert_error_query = qq{
            INSERT INTO $logs_table (page_token_next, response)
            VALUES (?, ?)
    };

    eval {
        my $sth = $dbh->prepare($select_query);
        $sth->execute();
        while (my ($name, $value) = $sth->fetchrow_array) {
            $config_data->{$name} = $value;
        }
    };

    if ($@) {
        warn "Error fetching configuration: $@";
        # Insert the error message into the $logs_table
        my $sth_insert_error = $dbh->prepare($insert_error_query);
        $sth_insert_error->execute('Configuration Error', $@);
    }

    my $client_id     = $config_data->{ist_client_id} || '';
    my $client_secret = xor_encrypt($config_data->{ist_client_secret}, $skey) || '';
    my $customerId    = $config_data->{ist_customer_id} || '';
    my $ist_url       = $config_data->{ist_api_url} || '';
    my $oauth_url     = $config_data->{ist_oauth_url} || '';
    my $debug_mode    = $config_data->{debug_mode} || '';
    my $api_limit     = int($config_data->{api_limit}) || 30;
    my $koha_default_categorycode = $config_data->{koha_default_categorycode} || '';
    my $koha_default_branchcode   = $config_data->{koha_default_branchcode} || '';
    my $cardnumberPlugin    = $config_data->{cardnumberPlugin} || 'civicNo';
    my $useridPlugin        = $config_data->{useridPlugin} || 'civicNo';
    my $logs_limit          = int($config_data->{logs_limit}) || 3;
    my $update_on           = $config_data->{update_on} || 'INSERT';
    
    # Request to API IST
    my $ua = LWP::UserAgent->new;
    my $pageToken = '';
    my $api_url = "$ist_url/ss12000v2-api/source/$customerId/v2.0/persons?limit=$api_limit";

    # Setting headers for get access_token
    my $request = POST $oauth_url, [
        client_id     => $client_id,
        client_secret => $client_secret,
        grant_type    => 'client_credentials',
    ];

    my $oauth_response = $ua->request($request);

    if ($oauth_response->is_success) {
        my $oauth_content = decode_json($oauth_response->decoded_content);
        my $access_token = $oauth_content->{access_token};

        # Take pageToken= from the database and append it to $api_url
        my $select_tokens_query = qq{
            SELECT page_token_next
            FROM $logs_table
            WHERE is_processed = 1
            ORDER BY created_at DESC
            LIMIT 1
        };
        my $sth_select_tokens = $dbh->prepare($select_tokens_query);
        $sth_select_tokens->execute();

        my ($page_token_next) = $sth_select_tokens->fetchrow_array;

        if (defined $page_token_next) {
            # The result string is not empty, and you can use the values from the database.
            $api_url = $api_url."&pageToken=$page_token_next";
        } 

        # currently this only for test
        my $try_api_url = "$ist_url/ss12000v2-api/source/$customerId/v2.0/duties?limit=1";
        # limit, duties, pageToken
        my $try_result = getApiResponse($try_api_url, $access_token);
        # warn "try: ".Dumper($try_result);
        warn "try dutyRole: ".$try_result->{data}[0]{dutyRole};

        # Setting headers with an access token to connect to the API
        my $api_request = HTTP::Request->new(GET => $api_url);
        $api_request->header('Content-Type' => 'application/json');
        $api_request->header('Authorization' => "Bearer $access_token");
        my $api_response = $ua->request($api_request);

        if ($api_response->is_success) {
            my $api_content = $api_response->decoded_content;

            my $response_data = decode_json($api_content);
            my $response_page_token = $response_data->{pageToken};

            my $md5 = Digest::MD5->new;
            $md5->add($api_content);

            # Generate a hash for checking
            my $data_hash = $md5->hexdigest;

            # Check if a record with the same $data_hash and is_processed flag exists
            my $select_existing_query = qq{
                SELECT is_processed
                FROM $logs_table
                WHERE data_hash = ?
                ORDER BY created_at DESC
                LIMIT 1
            };

            my $sth_select = $dbh->prepare($select_existing_query);
            $sth_select->execute($data_hash);

            if (my ($is_processed) = $sth_select->fetchrow_array) {
                if ($is_processed) {
                    # Record with data_hash=$data_hash already processed
                    warn "Record with data_hash=$data_hash has already been processed.\n";
                } else {
                    # Record with data_hash=$data_hash exists but not processed
                    warn "Record with data_hash=$data_hash exists but has not been processed.\n";
                }
            } else {
                # Record with data_hash=$data_hash not found
                warn "Record with data_hash=$data_hash not found in the database.\n";

                # Perform INSERT of a new record
                my $insert_query = qq{
                    INSERT INTO $logs_table (page_token_next, response, record_count, data_hash)
                    VALUES (?, ?, ?, ?)
                };

                my $sth_insert = $dbh->prepare($insert_query);

                if ($debug_mode eq "No") { 
                    $api_content = "Debug Mode OFF"; 
                }

                eval {
                    if ($sth_insert->execute($response_page_token, $api_content, $api_limit, $data_hash)) {
                        warn "Data from the API successfully inserted into $logs_table.\n";
                    } else {
                        die "Error inserting data into $logs_table: " . $dbh->errstr . "\n";
                    }
                };

                if ($@) {
                    warn "Database error: $@\n";
                }
            }

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
                    if (defined $addresses && ref $addresses eq 'ARRAY') {
                       foreach my $selectedAddresses (@$addresses) {
                            # $type = $selectedAddresses->{type};             
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
                            $update_on
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

        } else {
            my $error_message = "Error from API: " . $api_response->status_line . "\n";
            warn $error_message;
            # Insert the error message into the $logs_table
            my $sth_insert_error = $dbh->prepare($insert_error_query);
            $sth_insert_error->execute('API Error', $error_message);
            if ($api_response->code == 410) {
                # "code": "NEW_DATA_RESTART_FROM_FIRST_PAGE"
                my $insert_query = qq{
                    INSERT INTO $logs_table (page_token_next, is_processed)
                    VALUES (?, ?)
                };
                my $sth_insert = $dbh->prepare($insert_query);
                eval {
                    if ($sth_insert->execute("", 1)) {
                        warn "Pagination error: There is new data since the previous page load and you need to restart from the first page.\n";
                    } else {
                        die "Error inserting data into $logs_table: " . $dbh->errstr . "\n";
                    }
                };

                my $delete_query = qq{
                    DELETE FROM $logs_table
                    WHERE created_at <= DATE_SUB(NOW(), INTERVAL $logs_limit DAY)
                };
                my $sth_delete = $dbh->prepare($delete_query);
                eval {
                    if ($sth_delete->execute()) {
                        print "Deleted old records from $logs_table. Interval $logs_limit day/s \n";
                    } else {
                        die "Error deleting data from $logs_table: " . $dbh->errstr . "\n";
                    }
                };

                if ($@) {
                    warn "Database error: $@\n";
                }
            }
        }
    } else {
        my $oauth_error_message = "Error get access_token: " . $oauth_response->status_line . "\n";
        warn $oauth_error_message;
        # Insert the OAuth error message into the $logs_table
        my $sth_insert_oauth_error = $dbh->prepare($insert_error_query);
        $sth_insert_oauth_error->execute('OAuth Error', $oauth_error_message);
    }

    return;
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
        $update_on
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
                address = ?,
                city = ?,
                zipcode = ?,
                country = ?,
                B_email = ?,
                userid = ?,
                cardnumber = ?
        };

        if ($update_on eq "UPDATE") {
            $update_query .= qq{,
                categorycode = ?,
                branchcode = ?
            };
        }

        $update_query .= qq{
            WHERE borrowernumber = ?
        };

        my $update_sth = $dbh->prepare($update_query);

        # $update_on == "UPDATE", categorycode and branchcode will be updated
        if ($update_on eq "UPDATE") {
            $update_sth->execute(
                $birthdate, 
                $email, 
                $sex, 
                $phone, 
                $mobile_phone, 
                $surname, 
                $firstname, 
                $streetAddress,
                $locality,
                $postalCode,
                $country,
                $B_email,
                $newUserID,
                $newCardnumber,
                $categorycode, 
                $branchcode,
                $existing_borrower->{'borrowernumber'}
            );
        } else {
            # $update_on == "INSERT", categorycode and branchcode will NOT BE updated
            $update_sth->execute(
                $birthdate, 
                $email, 
                $sex, 
                $phone, 
                $mobile_phone, 
                $surname, 
                $firstname, 
                $streetAddress,
                $locality,
                $postalCode,
                $country,
                $B_email,
                $newUserID,
                $newCardnumber,
                $existing_borrower->{'borrowernumber'}
            );
        }

        # my $update_query = qq{
        #     UPDATE $borrowers_table
        #     SET 
        #         dateofbirth = ?, 
        #         email = ?, 
        #         sex = ?, 
        #         phone = ?, 
        #         mobile = ?, 
        #         surname = ?, 
        #         firstname = ?,
        #         categorycode = ?,
        #         branchcode = ?,
        #         address = ?,
        #         city = ?,
        #         zipcode = ?,
        #         country = ?,
        #         B_email = ?,
        #         userid = ?,
        #         cardnumber = ?
        #     WHERE borrowernumber = ?
        # };
        # my $update_sth = $dbh->prepare($update_query);
        # $update_sth->execute(
        #         $birthdate, 
        #         $email, 
        #         $sex, 
        #         $phone, 
        #         $mobile_phone, 
        #         $surname, 
        #         $firstname, 
        #         $categorycode, 
        #         $branchcode, 
        #         $streetAddress,
        #         $locality,
        #         $postalCode,
        #         $country,
        #         $B_email,
        #         $newUserID,
        #         $newCardnumber,
        #         $existing_borrower->{'borrowernumber'}
        #     );

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

sub getTranslation {
    my ($string) = @_;
    return Encode::decode( 'UTF-8', gettext($string) );
}

sub getApiResponse {
    my (
        $api_url, 
        $access_token
        ) = @_;

    # Request to API IST
    my $ua = LWP::UserAgent->new;

    # Setting headers with an access token to connect to the API
    my $api_request = HTTP::Request->new(GET => $api_url);
    $api_request->header('Content-Type' => 'application/json');
    $api_request->header('Authorization' => "Bearer $access_token");
    my $api_response = $ua->request($api_request);

    if ($api_response->is_success) {
        my $api_content = $api_response->decoded_content;
        my $response_data = decode_json($api_content);
        return $response_data;  
    } else {
        return undef;  # if error return undef
    }

}

1;
