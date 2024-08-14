# Copyright (C) 2024 imCode, https://www.imcode.com, <info@imcode.com>
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

use utf8;
$ENV{PERL_UNICODE} = "AS";

binmode(STDOUT, ":utf8");
binmode(STDIN, ":utf8");

use Koha::Token;
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
use Fcntl qw(:flock);

our $config_table     = 'imcode_config';
our $logs_table       = 'imcode_logs';
our $skey             = 'Uq9crAvPDNkkQcXAwsEHkjGwBwnSvDPC';  # Encryption key for ist_client_secret, change it if necessary
our $borrowers_table  = 'borrowers'; # Koha users table
our $categories_table = 'categories'; # Koha categories table
our $branches_table   = 'branches'; # Koha branches table
our $data_change_log_table    = 'imcode_data_change_log';
our $categories_mapping_table = 'imcode_categories_mapping';
our $branches_mapping_table   = 'imcode_branches_mapping';
our $added_count      = 0; # to count added
our $updated_count    = 0; # to count updated

our $VERSION = "1.37";

our $metadata = {
    name            => getTranslation('Export Users from SS12000'),
    author          => 'imCode.com',
    date_authored   => '2023-08-08',
    date_updated    => '2024-08-14',
    minimum_version => '20.05',
    maximum_version => undef,
    version         => $VERSION,
    description     => getTranslation('This plugin implements export users from SS12000')
};

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

    my $dbh = C4::Context->dbh;

    $self->store_data( { plugin_version => $Koha::Plugin::imCode::KohaSS12000::ExportUsers::VERSION || '1.0' } );

    my @installer_statements = (
    qq{CREATE TABLE IF NOT EXISTS imcode_config (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        value VARCHAR(255) NOT NULL,
        UNIQUE KEY unique_name_value (name, value)
    );},
    qq{CREATE TABLE IF NOT EXISTS imcode_data_change_log (
        log_id INT AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(255),
        record_id INT,
        action VARCHAR(255),
        change_description TEXT,
        change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );},
    qq{CREATE TABLE IF NOT EXISTS imcode_categories_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        categorycode VARCHAR(10) NOT NULL,
        dutyRole VARCHAR(120) NOT NULL,
        not_import tinyint(1) DEFAULT NULL,
        UNIQUE KEY unique_branchcode_organisationCode (categorycode, dutyRole)
    );},
    qq{CREATE TABLE IF NOT EXISTS imcode_branches_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        branchcode VARCHAR(10) NOT NULL,
        organisationCode VARCHAR(120) NOT NULL,
        UNIQUE KEY unique_branchcode_organisationCode (branchcode, organisationCode)
    );},        
    qq{CREATE TABLE IF NOT EXISTS imcode_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        page_token_next text COLLATE utf8mb4_unicode_ci,
        response text COLLATE utf8mb4_unicode_ci,
        record_count int(11) DEFAULT NULL,
        is_processed tinyint(1) DEFAULT NULL,
        data_endpoint varchar(255) DEFAULT NULL,
        data_hash varchar(255) DEFAULT NULL,
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
    );},
    qq{INSERT INTO imcode_config (name,value) VALUES ('ist_client_id','your_client_id');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('ist_client_secret','');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('ist_customer_id','your_customerId');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('ist_api_url','https://api.ist.com');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('ist_oauth_url','https://skolid.se/connect/token');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('koha_default_categorycode','');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('koha_default_branchcode','');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('debug_mode','No');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('api_limit','30');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('cardnumberPlugin','civicNo');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('useridPlugin','civicNo');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('logs_limit','3');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('excluding_dutyRole_empty','No');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('excluding_enrolments_empty','No');},
    qq{INSERT INTO imcode_categories_mapping (categorycode,dutyRole) VALUES ('SKOLA','Lärare');},
    qq{INSERT INTO imcode_categories_mapping (categorycode,dutyRole) VALUES ('PERSONAL','Kurator');},
    qq{INSERT INTO imcode_categories_mapping (categorycode,dutyRole) VALUES ('SKOLA','Rektor');},
    qq{INSERT INTO imcode_categories_mapping (categorycode,dutyRole) VALUES ('PERSONAL','Lärarassistent');},
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
    CREATE TRIGGER log_user_changes
    AFTER UPDATE ON borrowers
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
            INSERT INTO imcode_data_change_log (table_name, record_id, action, change_description)
            VALUES ('borrowers', NEW.borrowernumber, 'update', change_description);
        END IF;
    END;
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

    my @tables_to_delete = ($config_table, $logs_table, $data_change_log_table, $categories_mapping_table, $branches_mapping_table); 

    eval {
        foreach my $table (@tables_to_delete) {
            my $table_deletion_query = "DROP TABLE IF EXISTS $table";
            $dbh->do($table_deletion_query);
        }
        my $table_deletion_query = "DROP TRIGGER IF EXISTS log_user_changes";
        $dbh->do($table_deletion_query);
    };

    if ($@) {
        warn "Error deleting table: $@";
        return 0;
    }

    return 1;
}

sub insertConfigValue {
    my ($dbh, $name, $value) = @_;

    my $check_query = qq{
        SELECT COUNT(*) FROM `imcode_config` WHERE `name` = ?
    };

    my $existing_records = $dbh->selectrow_array($check_query, undef, $name);

    if (!$existing_records) {
        my $insert_query = qq{
            INSERT INTO `imcode_config` (`name`, `value`)
            VALUES (?, ?)
        };

        eval { $dbh->do($insert_query, undef, $name, $value) };

        if ($@) {
            warn "Error while inserting config value: $@";
        }
    } else {
        # warn "Config value with name '$name' already exists";
    }
}


sub configure {
    my ($self, $args) = @_;

    my $dbh = C4::Context->dbh;

    my $cgi = $self->{'cgi'};

    my $op = $cgi->param('op') || '';

    # update for version 1.32 
    insertConfigValue($dbh, 'excluding_dutyRole_empty', 'No');
    insertConfigValue($dbh, 'excluding_enrolments_empty', 'No');

    my $select_query = qq{SELECT name, value FROM $config_table};
    my $config_data  = {};

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

    if ($op eq 'cud-save-config') {
        my $client_id     = $cgi->param('client_id');
        # client_secret
        my $client_secret = $cgi->param('client_secret');
        if (defined $client_secret && length($client_secret) > 0) {
            $client_secret = xor_encrypt($client_secret, $skey);
        } else {
            my $sth = $dbh->prepare($select_query);
            $sth->execute();
            while (my ($name, $value) = $sth->fetchrow_array) {
                $config_data->{$name} = $value;
            }
            $client_secret = $config_data->{ist_client_secret};
        }
        # /client_secret
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

        my $new_organisationCode_mapping = $cgi->param('new_organisationCode_mapping');
        my $new_branch_mapping           = $cgi->param('new_branch_mapping');

        my $new_categories_mapping       = $cgi->param('new_categories_mapping');
        my $new_dutyRole_mapping         = $cgi->param('new_dutyRole_mapping');

        my @category_mapping_del = $cgi->multi_param('category_mapping_del[]');
        my @branch_mapping_del   = $cgi->multi_param('branch_mapping_del[]');
        my @category_mapping_not_import = $cgi->multi_param('category_mapping_not_import[]');

        my $excluding_dutyRole_empty = $cgi->param('excluding_dutyRole_empty');
        my $excluding_enrolments_empty = $cgi->param('excluding_enrolments_empty');

        # update not_import
        my $update_category_query = qq{UPDATE $categories_mapping_table SET not_import = NULL};
        $dbh->do($update_category_query);
        if (@category_mapping_not_import) {
            my $update_category_query = qq{
                UPDATE
                $categories_mapping_table
                SET not_import = 1
                WHERE id = ?
            };
            foreach my $category_id (@category_mapping_not_import) {
                next if $category_id == 0; # skip ID = 0
                $dbh->do($update_category_query, undef, $category_id);
            }
        }

        # delete
        if (@category_mapping_del) {
            my $delete_category_query = qq{
                DELETE 
                FROM $categories_mapping_table 
                WHERE id = ? 
            };

            foreach my $category_id (@category_mapping_del) {
                next if $category_id == 0; # skip ID = 0
                $dbh->do($delete_category_query, undef, $category_id);
            }
        }
        if (@branch_mapping_del) {
            my $delete_branch_query = qq{
                DELETE 
                FROM $branches_mapping_table 
                WHERE id = ? 
            };

            foreach my $branch_id (@branch_mapping_del) {
                next if $branch_id == 0; # skip ID = 0
                $dbh->do($delete_branch_query, undef, $branch_id);
            }
        }
        # /delete

        if ($new_branch_mapping && $new_organisationCode_mapping) {
            # my $check_mapping_query = qq{
            #     SELECT organisationCode 
            #     FROM $branches_mapping_table 
            #     WHERE organisationCode = ? 
            #     AND branchcode = ?
            # };            
            my $check_mapping_query = qq{
                SELECT organisationCode 
                FROM $branches_mapping_table 
                WHERE organisationCode = ? 
            };
            my $sth_check_mapping = $dbh->prepare($check_mapping_query);
            # $sth_check_mapping->execute($new_organisationCode_mapping, $new_branch_mapping);
            $sth_check_mapping->execute($new_organisationCode_mapping);

            if (!$sth_check_mapping->fetchrow_array()) {
                my $insert_mapping_query = qq{
                    INSERT INTO $branches_mapping_table (organisationCode, branchcode) VALUES (?, ?)
                };
                my $sth_insert_mapping = $dbh->prepare($insert_mapping_query);
                $sth_insert_mapping->execute($new_organisationCode_mapping, $new_branch_mapping);
            }
            $sth_check_mapping->finish();
        }

        if ($new_categories_mapping && $new_dutyRole_mapping) {
            # my $check_mapping_query = qq{
            #     SELECT categorycode 
            #     FROM $categories_mapping_table 
            #     WHERE categorycode = ? 
            #     AND dutyRole = ?
            # };
            my $check_mapping_query = qq{
                SELECT categorycode 
                FROM $categories_mapping_table 
                WHERE categorycode = ? 
            };
            my $sth_check_mapping = $dbh->prepare($check_mapping_query);
            # $sth_check_mapping->execute($new_categories_mapping, $new_dutyRole_mapping);
            $sth_check_mapping->execute($new_dutyRole_mapping);

            if (!$sth_check_mapping->fetchrow_array()) {
                my $insert_mapping_query = qq{
                    INSERT INTO $categories_mapping_table (categorycode, dutyRole) VALUES (?, ?)
                };
                my $sth_insert_mapping = $dbh->prepare($insert_mapping_query);
                $sth_insert_mapping->execute($new_categories_mapping, $new_dutyRole_mapping);
            }
            $sth_check_mapping->finish();
        }

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
                WHEN name = 'excluding_dutyRole_empty' THEN ?
                WHEN name = 'excluding_enrolments_empty' THEN ?
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
                'excluding_dutyRole_empty',
                'excluding_enrolments_empty'
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
                $excluding_dutyRole_empty,
                $excluding_enrolments_empty
                );
            $template->param(success => 'success');
        };

        if ($@) {
            warn "Error updating configuration: $@";
        }
    }


    # update for version 1.31
    # Check for 'not_import' field
    my $add_column_query = qq{
        ALTER TABLE $categories_mapping_table
        ADD COLUMN IF NOT EXISTS not_import TINYINT(1) DEFAULT NULL
    };
    eval { $dbh->do($add_column_query) };

    if ($@) {
        warn "Error while adding column: $@";
    }

    my $select_categorycode_query = qq{SELECT categorycode FROM $categories_table};
    my $select_branchcode_query = qq{SELECT branchcode FROM $branches_table};

    my @categories;
    my @branches;

    my $select_categories_mapping_query = qq{SELECT id, categorycode, dutyRole, not_import FROM $categories_mapping_table};
    my $select_branches_mapping_query = qq{SELECT id, branchcode, organisationCode FROM $branches_mapping_table};

    my @categories_mapping;
    my @branches_mapping;

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

        my $sth_categories_mapping = $dbh->prepare($select_categories_mapping_query);
        $sth_categories_mapping->execute();
        while (my ($id, $category, $dutyRole, $not_import) = $sth_categories_mapping->fetchrow_array) {
            push @categories_mapping, { id => $id, categorycode => $category, dutyRole => $dutyRole, not_import => $not_import };
        }

        my $sth_branches_mapping = $dbh->prepare($select_branches_mapping_query);
        $sth_branches_mapping->execute();
        while (my ($id, $branch, $organisationCode) = $sth_branches_mapping->fetchrow_array) {
            push @branches_mapping, { id => $id, branchcode => $branch, organisationCode => $organisationCode };
        }

        my $sth = $dbh->prepare($select_query);
        $sth->execute();
        while (my ($name, $value) = $sth->fetchrow_array) {
            $config_data->{$name} = $value;
        }
    };

    if ($@) {
        warn "Error fetching configuration: $@";
    }

    my $session_id = $cgi->cookie('CGISESSID');
    unless ($session_id) {
        warn "Session ID not found";
        return;
    }
    my $tokenizer = Koha::Token->new;
    my $csrf_token = $tokenizer->generate_csrf({ session_id => $session_id });

    $template->param(
        client_id     => $config_data->{ist_client_id} || '',
        client_secret => xor_encrypt($config_data->{ist_client_secret}, $skey) || '',
        customerId    => $config_data->{ist_customer_id} || '',
        api_url       => $config_data->{ist_api_url} || '',
        oauth_url     => $config_data->{ist_oauth_url} || '',
        categories    => \@categories,
        branches      => \@branches,
        categories_mapping   => \@categories_mapping,
        branches_mapping     => \@branches_mapping,        
        debug_mode    => $config_data->{debug_mode} || '',
        api_limit     => int($config_data->{api_limit}) || 30,
        koha_default_categorycode => $config_data->{koha_default_categorycode} || '',
        koha_default_branchcode   => $config_data->{koha_default_branchcode} || '',
        cardnumberPlugin    => $config_data->{cardnumberPlugin} || 'civicNo',
        useridPlugin        => $config_data->{useridPlugin} || 'civicNo',
        logs_limit          => int($config_data->{logs_limit}) || 3,
        language            => C4::Languages::getlanguage($cgi) || 'en',
        mbf_path            => abs_path( $self->mbf_path('translations') ),
        verify_config       => verify_categorycode_and_branchcode(),
        excluding_enrolments_empty => $config_data->{excluding_enrolments_empty} || 'No',
        excluding_dutyRole_empty => $config_data->{excluding_dutyRole_empty} || 'No',
        csrf_token => $csrf_token
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
    
    my $dbh = C4::Context->dbh;

    my $cgi      = $self->{'cgi'};
    my $template = $self->get_template( { file => 'tool.tt' } );

    my $session_id = $cgi->cookie('CGISESSID');
    unless ($session_id) {
        warn "Session ID not found";
        return;
    }
    my $tokenizer = Koha::Token->new;
    my $csrf_token = $tokenizer->generate_csrf({ session_id => $session_id });

    my $op          = $cgi->param('op') || q{};

    # For pagination 
    my $page      = $cgi->param('page') || 1; # Current page
    my $per_page  = 20; # Number of entries per page
    my $start_row = ($page - 1) * $per_page;
    my $total_rows = 0;

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

    if ($op eq 'cud-show-updates') {
        my @updates;

        my $search = $cgi->param('search') || q{};
        $search = substr($search, 0, 50); # Limit to 50 characters for searching
        my @search_terms = split(/\s+/, $search);

        my $select_query = qq{
            SELECT
                l.log_id,
                l.record_id,
                l.change_description,
                l.change_timestamp,
                b.firstname,
                b.surname
            FROM
                $data_change_log_table l
            JOIN
                $borrowers_table b ON l.record_id = b.borrowernumber
            WHERE
                1=1
        };

        # Add search terms for each word
        if ($search) {
            my @search_conditions;
            foreach my $term (@search_terms) {
                push @search_conditions, qq{
                    (
                        l.change_description LIKE ? OR
                        l.change_timestamp LIKE ? OR
                        b.firstname LIKE ? OR
                        b.surname LIKE ?
                    )
                };
            }
            my $search_condition = join(" AND ", @search_conditions);
            $select_query .= " AND ($search_condition)";
        }

        $select_query .= qq{ ORDER BY l.log_id DESC LIMIT $per_page OFFSET $start_row};

        eval {
            my $sth = $dbh->prepare($select_query);
            if ($search) {
                my @search_params = map { '%' . $_ . '%' } @search_terms;
                $sth->execute(@search_params, @search_params, @search_params, @search_params);
            } else {
                $sth->execute();
            }
            while (my $row = $sth->fetchrow_hashref()) {
                push @updates, $row;
            }
        };

        if ($@) {
            warn "Error fetching, info about users data update: $@";
        }

        my $count_query = qq{
            SELECT COUNT(*)
            FROM
                $data_change_log_table l
            JOIN
                $borrowers_table b ON l.record_id = b.borrowernumber
            WHERE
                1=1
        };

        # Add search conditions for the number of records
        if ($search) {
            my @search_conditions;
            foreach my $term (@search_terms) {
                push @search_conditions, qq{
                    (
                        l.change_description LIKE ? OR
                        l.change_timestamp LIKE ? OR
                        b.firstname LIKE ? OR
                        b.surname LIKE ?
                    )
                };
            }
            my $search_condition = join(" AND ", @search_conditions);
            $count_query .= " AND ($search_condition)";
        }

        eval {
            my $sth = $dbh->prepare($count_query);
            if ($search) {
                my @search_params = map { '%' . $_ . '%' } @search_terms;
                $sth->execute(@search_params, @search_params, @search_params, @search_params);
            } else {
                $sth->execute();
            }
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

    if ($op eq 'cud-show-logs') {
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

    if ($op eq 'cud-show-stat') {
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
            csrf_token => $csrf_token,
    );

    print $cgi->header( -type => 'text/html', -charset => 'utf-8' );
    print $template->output();
}


sub cronjob {
    # script for run in cron here cron/imcode_ss12000.pl
    my ($self, $data_endpoint) = @_;
    $self->fetchDataFromAPI($data_endpoint);
    return;
}

sub fetchDataFromAPI {
    my ($self, $data_endpoint) = @_;

    my $dbh = C4::Context->dbh;

    if (verify_categorycode_and_branchcode() eq "No") {
        warn "WARNING: branches mapping and/or categories mapping not configured correctly";
        die "ErrorVerifyCategorycodeBranchcode";
    } 

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
    my $excluding_enrolments_empty = $config_data->{excluding_enrolments_empty} || 'No';
    my $excluding_dutyRole_empty = $config_data->{excluding_dutyRole_empty} || 'No';
    
    # Request to API IST
    my $ua = LWP::UserAgent->new;
    my $pageToken = '';
    # my $data_endpoint = "persons";
    my $api_url = "$ist_url/ss12000v2-api/source/$customerId/v2.0/$data_endpoint?limit=$api_limit";
    my $api_url_base = "$ist_url/ss12000v2-api/source/$customerId/v2.0/";

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
            SELECT id, page_token_next
            FROM $logs_table
            WHERE is_processed = 1
            AND data_endpoint = ?
            ORDER BY created_at DESC
            LIMIT 1
        };
        my $sth_select_tokens = $dbh->prepare($select_tokens_query);
        $sth_select_tokens->execute($data_endpoint);

        my ($data_id, $page_token_next) = $sth_select_tokens->fetchrow_array;

        if (defined $page_token_next) {
            # The result string is not empty, and you can use the values from the database.
            $api_url = $api_url."&pageToken=$page_token_next";
        } 

        # api_url = "$ist_url/ss12000v2-api/source/$customerId/v2.0/$data_endpoint?limit=$api_limit";

        my $response_data = getApiResponse($api_url, $access_token);

        my $response = $response_data; 
        if ($debug_mode eq "No") { 
            $response = "Debug Mode OFF"; 
        } 

        eval {
            $response_data = decode_json($response_data);
        };
        if ($@) {
            my $update_query = qq{
                UPDATE $logs_table
                SET is_processed = 1,
                    page_token_next = ?
                WHERE id = ?
            };
            my $sth_update = $dbh->prepare($update_query);
            unless ($sth_update->execute("", $data_id)) {
                die "An error occurred while executing the request: " . $sth_update->errstr;
            }
            $sth_update->finish();
            die "Error when decoding JSON: $@";
        }

        my $response_page_token = $response_data->{pageToken};

        my $md5 = Digest::MD5->new;
        $md5->add($response_data);
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
                    INSERT INTO $logs_table (page_token_next, response, record_count, data_hash, data_endpoint)
                    VALUES (?, ?, ?, ?, ?)
                };

                my $sth_insert = $dbh->prepare($insert_query);

                eval {
                    if ($sth_insert->execute($response_page_token, $response, $api_limit, $data_hash, $data_endpoint)) {
                        warn "Data from the API successfully inserted into $logs_table.\n";
                    } else {
                        die "Error inserting data into $logs_table: " . $dbh->errstr . "\n";
                    }
                };

                if ($@) {
                    warn "Database error: $@\n";
                }
        }

        my $select_categories_mapping_query = qq{SELECT id, categorycode, dutyRole, not_import FROM $categories_mapping_table};
        my $select_branches_mapping_query = qq{SELECT id, branchcode, organisationCode FROM $branches_mapping_table};

        my @categories_mapping;
        my @branches_mapping;

        eval {
            my $sth_categories_mapping = $dbh->prepare($select_categories_mapping_query);
            $sth_categories_mapping->execute();
            while (my ($id, $category, $dutyRole, $not_import) = $sth_categories_mapping->fetchrow_array) {
                push @categories_mapping, { id => $id, categorycode => $category, dutyRole => $dutyRole, not_import =>$not_import };
            }

            my $sth_branches_mapping = $dbh->prepare($select_branches_mapping_query);
            $sth_branches_mapping->execute();
            while (my ($id, $branch, $organisationCode) = $sth_branches_mapping->fetchrow_array) {
                push @branches_mapping, { id => $id, branchcode => $branch, organisationCode => $organisationCode };
            }
        };

        if ($@) {
            warn "Error fetching categories_mapping/branches_mapping: $@";
        }

        if ($response_data && $data_endpoint eq "persons") {
                fetchBorrowers(
                    $response_data, 
                    $api_limit, 
                    $debug_mode, 
                    $koha_default_categorycode, 
                    $koha_default_branchcode,
                    $cardnumberPlugin,
                    $useridPlugin,
                    $response_page_token,
                    $data_hash,
                    $access_token,
                    $api_url_base,
                    $excluding_enrolments_empty,
                    $excluding_dutyRole_empty,                    
                    \@categories_mapping,
                    \@branches_mapping
                );

            if (!defined $response_page_token || $response_page_token eq "") {
                    die "EndLastPageFromAPI"; # last page from API, flag for bash script Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run.sh
            }                

        }        
        else {
            my $error_message = "Error from API: " . $response->status_line . "\n";
            warn $error_message;
            # Insert the error message into the $logs_table
            my $sth_insert_error = $dbh->prepare($insert_error_query);
            $sth_insert_error->execute('API Error', $error_message);
            # if an API error, add this entry to the logs_table as well - to start a new playthrough cycle
            # if ($response->code == 410) {
                # "code": "NEW_DATA_RESTART_FROM_FIRST_PAGE"
                my $insert_query = qq{
                    INSERT INTO $logs_table (page_token_next, is_processed, data_endpoint)
                    VALUES (?, ?, ?)
                };
                my $sth_insert = $dbh->prepare($insert_query);
                eval {
                    if ($sth_insert->execute("", 1, $data_endpoint)) {
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
            # }
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
        return $api_content;
    } else {
        return undef;  # if error return undef
    }

}


sub verify_categorycode_and_branchcode {
    my $dbh = C4::Context->dbh;

    $dbh->do("SET NAMES utf8mb4");
    $dbh->do("SET CHARACTER SET utf8mb4");

    my $select_categories_mapping_query = qq{SELECT categorycode FROM $categories_mapping_table};
    my $select_branches_mapping_query = qq{SELECT branchcode FROM $branches_mapping_table};

    my $categories_mapping_exists = $dbh->selectall_arrayref($select_categories_mapping_query);
    my $branches_mapping_exists = $dbh->selectall_arrayref($select_branches_mapping_query);

    if (@$categories_mapping_exists && @$branches_mapping_exists) {
        my $select_categorycode_query = qq{SELECT categorycode FROM $categories_table};
        my $select_branchcode_query = qq{SELECT branchcode FROM $branches_table};

        my $categorycode_exists = $dbh->selectall_arrayref($select_categorycode_query);
        my $branchcode_exists = $dbh->selectall_arrayref($select_branchcode_query);

        my $categorycode_in_categories_table_query = qq{
            SELECT categorycode FROM $categories_table
            WHERE categorycode COLLATE utf8mb4_swedish_ci IN (SELECT categorycode FROM $categories_mapping_table)
        };

        my $categorycode_in_categories_table = $dbh->selectall_arrayref($categorycode_in_categories_table_query);

        my $branchcode_in_branches_table_query = qq{
            SELECT branchcode FROM $branches_table
            WHERE branchcode COLLATE utf8mb4_swedish_ci IN (SELECT branchcode FROM $branches_mapping_table)
        };

        my $branchcode_in_branches_table = $dbh->selectall_arrayref($branchcode_in_branches_table_query);

        if (@$categorycode_exists && @$branchcode_exists && @$categorycode_in_categories_table && @$branchcode_in_branches_table) {
            return "Ok";
        } else {
            return "No";
        }
    } else {
        return "No";
    }
}

sub read_state {
    my $filename = shift;
    open my $fh, '<', $filename or die "Cannot open $filename: $!";
    flock($fh, LOCK_SH) or die "Cannot lock $filename: $!";
    my %state;
    while (my $line = <$fh>) {
        chomp $line;
        my ($key, $value) = split /:/, $line, 2;
        $state{$key} = $value;
    }
    close $fh;
    return \%state;
}

sub write_state {
    my ($filename, $state) = @_;
    open my $fh, '>', $filename or die "Cannot open $filename: $!";
    flock($fh, LOCK_EX) or die "Cannot lock $filename: $!";
    foreach my $key (keys %$state) {
        print $fh "$key:$state->{$key}\n";
    }
    close $fh;
}


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
            $data_hash,
            $access_token,
            $api_url_base,
            $excluding_enrolments_empty,
            $excluding_dutyRole_empty,            
            $categories_mapping_ref,
            $branches_mapping_ref
        ) = @_;

    my @categories_mapping = @$categories_mapping_ref;
    my @branches_mapping = @$branches_mapping_ref;

    my $dbh = C4::Context->dbh;

    my $state_file = '/tmp/imcode-state.inf';
    unless (-e $state_file) {
                    open my $fh, '>', $state_file or die "Cannot create $state_file: $!";
                    flock($fh, LOCK_EX) or die "Cannot lock $state_file: $!";
                    print $fh "";
                    close $fh;
    }

    my $state = read_state($state_file);
    my $parse_page = $state->{parse_page} // 1;
    my $total_borrowers = $state->{total_borrowers} // 0;
    my $total_enrolments = $state->{total_enrolments} // 0;
    my $total_dutyRole = $state->{total_dutyRole} // 0;
    my $total_dutyRole_empty = $state->{total_dutyRole_empty} // 0;
    my $total_enrolments_empty = $state->{total_enrolments_empty} // 0;
    my $total_intersecting_dutyRole_enrolments_empty = $state->{total_intersecting_dutyRole_enrolments_empty} // 0;
    my $total_dutyRoleName = $state->{total_dutyRoleName} // '';
    my $total_enrolmentsName = $state->{total_enrolmentsName} // '';

            my $j = 0;
            for my $i (1..$api_limit) {

                my $koha_categorycode = $koha_default_categorycode;
                my $koha_branchcode = $koha_default_branchcode;
                my $not_import = 0;

                my $response_page_data = $response_data->{data}[$i-1];
                if ($response_page_data) {
                    # Update state
                    if ($debug_mode eq "Yes") { $total_borrowers++; }

                    my $id = $response_page_data->{id};

                    # start search "groupType": "Klass"
                    my $person_groupMemberships_api_url = $api_url_base."persons/".$id."?expand=groupMemberships";
                    my $response_data_groupMemberships;
                    eval {
                        $response_data_groupMemberships = decode_json(getApiResponse($person_groupMemberships_api_url, $access_token));
                    };

                    use DateTime;
                    my $dt = DateTime->now;
                    my $today = $dt->ymd;

                    my $klass_displayName;
                    foreach my $groupMembership (@{$response_data_groupMemberships ->{_embedded}->{groupMemberships}}) {
                        my $group = $groupMembership->{group};
                        # "groupType" & "endDate" 
                        # 2024-05-24 added "startDate" for case like - to_date: 2025-06-30 from_date: 2024-07-01
                        if ($group->{groupType} eq "Klass" && $group->{endDate} gt $today && $group->{startDate} lt $today) {
                            $klass_displayName = $group->{displayName};
                            # warn "klass_displayName: ".$klass_displayName.", to date: ".$group->{endDate}." from date: ".$group->{startDate};
                            last; 
                        }
                    }
                    # end search "groupType": "Klass"

                    # my $api_url_base = "$ist_url/ss12000v2-api/source/$customerId/v2.0/";
                    # dutyRole @categories_mapping
                    my $person_api_url = $api_url_base."duties?person=".$id;
                    my $response_data_person;
                    my $duty_role;

                    # warn "access_token: ".$access_token;
                    if ($debug_mode eq "Yes") { print STDERR "\nUSER START DEBUG\n"; }

                    eval {
                        $response_data_person = decode_json(getApiResponse($person_api_url, $access_token));
                    };

                    if (
                            $response_data_person && 
                            ref($response_data_person) eq 'HASH' && 
                            $response_data_person->{data} && 
                            ref($response_data_person->{data}) eq 'ARRAY' && 
                            @{$response_data_person->{data}}
                        ) {
                        $duty_role = $response_data_person->{data}[0]->{dutyRole};
                        # utf8::decode($duty_role); # utf8
                    } 

                    # warn "response_data_person: ".Dumper($response_data_person);

                    if ($duty_role) {
                        if ($debug_mode eq "Yes") { 
                            print STDERR "duty_role: ".$duty_role."\n"; 
                            if ($total_dutyRoleName eq '') {
                                $total_dutyRoleName = $duty_role;
                            } else {
                                $total_dutyRoleName = $total_dutyRoleName . ", " . $duty_role;
                            }
                            $total_dutyRole++;
                        }
                        foreach my $category_mapping (@categories_mapping) {
                            if ($category_mapping->{dutyRole} && $category_mapping->{dutyRole} eq $duty_role) {
                                $koha_categorycode = $category_mapping->{categorycode};
                                $not_import = $category_mapping->{not_import} || 0;
                                if ($debug_mode eq "Yes") { 
                                    print STDERR "not_import from mysql base, category_mapping: ".$not_import."\n";
                                }
                                last; 
                            }
                        }
                    } else {

                        if ($excluding_dutyRole_empty eq "Yes") {
                                $not_import = 1;
                                if ($debug_mode eq "Yes") { 
                                    $total_dutyRole_empty++;
                                    print STDERR "duty_role is empty, not import data\n";
                                    # warn "duty_role is empty, not import data";
                                }                            
                        }

                    }
                    # /dutyRole

                    # organisationCode @branches_mapping
                    my $enrolments = $response_page_data->{enrolments}; 
                    my $enroledAtId = "";

                    if (defined $enrolments && ref $enrolments eq 'ARRAY') {
                        foreach my $enrolment (@$enrolments) {
                            my $enroledAt = $enrolment->{enroledAt}; 
                            if (defined $enroledAt && ref $enroledAt eq 'HASH') {
                                $enroledAtId = $enroledAt->{id}; 
                            }
                        }
                    }

                    if ($enroledAtId) {
                        if ($debug_mode eq "Yes") { 
                                $total_enrolments++; 
                                # warn "organisations user url: ".$person_api_url;
                                print STDERR "enroledAtId: ".$enroledAtId."\n";
                        }
                        
                        my $person_api_url = $api_url_base."organisations/".$enroledAtId;
                        my $organisationCode;

                        eval {
                            $response_data_person = decode_json(getApiResponse($person_api_url, $access_token));
                        };

                        # warn "organisations response_data_person: ".Dumper($response_data_person);

                        if (defined $response_data_person && ref($response_data_person) eq 'HASH' && defined $response_data_person->{organisationCode}) {
                            $organisationCode = $response_data_person->{organisationCode};
                        }

                        if ($organisationCode) {
                            if ($debug_mode eq "Yes") { 
                                print STDERR "organisationCode: $organisationCode\n"; 
                                if ($total_enrolmentsName eq '') {
                                    $total_enrolmentsName = $organisationCode;
                                } else {
                                    $total_enrolmentsName = $total_enrolmentsName . ", " . $organisationCode;
                                }
                            }
                            # warn "organisationCode: $organisationCode";
                            # warn "Number of elements in \@branches_mapping: " . scalar(@branches_mapping);
                            foreach my $branch_mapping (@branches_mapping) {
                                # warn "Checking branch_mapping: ". Dumper($branch_mapping);
                                if ($branch_mapping->{organisationCode} && $branch_mapping->{organisationCode} eq $organisationCode) {
                                    $koha_branchcode = $branch_mapping->{branchcode};
                                    last; 
                                }
                            }
                            # warn "koha_branchcode set to: ".$koha_branchcode;
                            # warn "koha_default_branchcode is : ".$koha_default_branchcode;
                        }
                        # /organisationCode
                    } else {

                        if ($excluding_enrolments_empty eq "Yes") {
                                $not_import = 1;
                                if ($debug_mode eq "Yes") { 
                                    # warn "enrolments is empty, not import data";
                                    print STDERR "enrolments is empty, not import data\n";
                                    $total_enrolments_empty++;
                                }
                        }

                    }

                    if ($not_import == 1 && $debug_mode eq "Yes") { $total_intersecting_dutyRole_enrolments_empty++; }

                    if ($debug_mode eq "Yes") { print STDERR "USER END DEBUG\n"; }

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
                                    $B_email = lc($email_value);
                                }
                            } elsif ($email_type eq "Skola personal") {
                                if (defined $email_value) {
                                    $email = lc($email_value);
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
                        }
                    }

                    my $userid = $cardnumber;

                    if (!defined $email || $email eq "") { $email = undef; }
                    # warn "not_import, must be !=1, now is: ".$not_import;
                    if ($not_import != 1) {
                        addOrUpdateBorrower(
                                $cardnumber,
                                $familyName,
                                $givenName,
                                $birthDate,
                                $email,
                                $sex,
                                $phone,
                                $mobile_phone,
                                $koha_categorycode,
                                $koha_branchcode,
                                $streetAddress,
                                $locality,
                                $postalCode,
                                $country,
                                $B_email,
                                $userid,
                                $useridPlugin,
                                $cardnumberPlugin,
                                $externalIdentifier,
                                $klass_displayName,
                            );
                    }
                    $j++;
                } 
            }

            if ($debug_mode eq "Yes") {
                # Update state
                $state->{parse_page} = $parse_page + 1;
                $state->{total_borrowers} = $total_borrowers;
                $state->{total_enrolments} = $total_enrolments;
                $state->{total_dutyRole} = $total_dutyRole;
                $state->{total_dutyRole_empty} = $total_dutyRole_empty;
                $state->{total_enrolments_empty} = $total_enrolments_empty;
                $state->{total_intersecting_dutyRole_enrolments_empty} = $total_intersecting_dutyRole_enrolments_empty;
                $state->{total_dutyRoleName} = $total_dutyRoleName;
                $state->{total_enrolmentsName} = $total_enrolmentsName;
                write_state($state_file, $state);
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
        $klass_displayName,
        ) = @_;
    
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
        WHERE cardnumber = ? OR cardnumber = ? OR userid = ?
    };
    my $select_sth = $dbh->prepare($select_query);
    $select_sth->execute($cardnumber, $externalIdentifier, $newUserID);
    my $existing_borrower = $select_sth->fetchrow_hashref;
    my $borrowernumber;

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
        $borrowernumber = $existing_borrower->{'borrowernumber'};
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
            $borrowernumber = $dbh->last_insert_id(undef, undef, $borrowers_table, undef);
    }

    if ($klass_displayName) {
        # add to borrower_attributes
        # warn "borrowernumber: $borrowernumber, klass_displayName: $klass_displayName";
        my $code = 'CL';
        my $attribute = $klass_displayName;

        # Check if an entry exists in borrower_attribute_types
        my $check_types_query = qq{
            SELECT 1 FROM borrower_attribute_types WHERE code = ?
        };
        my $check_types_sth = $dbh->prepare($check_types_query);
        $check_types_sth->execute($code);
        my ($exists) = $check_types_sth->fetchrow_array();

        unless ($exists) {
            # If there is no record, insert it
            my $insert_types_query = qq{
                INSERT INTO borrower_attribute_types (code, description, repeatable, unique_id, opac_display, opac_editable, staff_searchable, authorised_value_category, display_checkout, category_code, class, keep_for_pseudonymization, mandatory)
                VALUES ('CL', 'Klass', 0, 0, 0, 0, 0, '', 0, NULL, '', 0, 0)
            };
            my $insert_types_sth = $dbh->prepare($insert_types_query);
            $insert_types_sth->execute();
        }

        # Check if the record exists
        my $check_query = qq{
                SELECT attribute FROM borrower_attributes 
                WHERE borrowernumber = ? AND code = ?
                };
        my $check_sth = $dbh->prepare($check_query);
        $check_sth->execute($borrowernumber, $code);
        my $existing_attribute = $check_sth->fetchrow_array();

        if (defined $existing_attribute) {
                # If the record exists but the attribute is different, update it
                if ($existing_attribute ne $attribute) {
                    my $update_query = qq{
                        UPDATE borrower_attributes
                        SET attribute = ?
                        WHERE borrowernumber = ? AND code = ?
                    };
                    my $update_sth = $dbh->prepare($update_query);
                    $update_sth->execute($attribute, $borrowernumber, $code);
                }
        } else {
            # If there is no record, insert a new one
            my $insert_query = qq{
                        INSERT INTO borrower_attributes (borrowernumber, code, attribute)
                        VALUES (?, ?, ?)
                    };
            my $insert_sth = $dbh->prepare($insert_query);
            $insert_sth->execute($borrowernumber, $code, $attribute);
        }
        # end borrower_attributes
    }
}

1;
