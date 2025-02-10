# Copyright (C) 2024 imCode, https://www.imcode.com, <info@imcode.com>
#
# This is a plugin for Koha
# It exports user data from the API in SS12000 format to your Koha database
#
# Category: Koha, https://koha-community.org 
# Plugin:   imCode::KohaSS12000::ExportUsers
# Author:   Serge Tkachuk, https://github.com/fly304625, <tkachuk.serge@gmail.com>
# Author:   Jacob Sandin, https://github.com/JacobSandin, <jacob@imcode.com>
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
use C4::Context;
use File::Spec;
use Fcntl ':flock';  # Import LOCK_EX for file locking
use Time::Local;     # For time calculations

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
use POSIX qw(strftime); # To format date/time
use Encode;
use URI::Escape qw(uri_escape);


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
our $processed_count  = 0; # to count processed

our $VERSION = "1.62";

our $metadata = {
    name            => getTranslation('Export Users from SS12000'),
    author          => 'imCode.com',
    date_authored   => '2023-08-08',
    date_updated    => '2025-02-10',
    minimum_version => '20.05',
    maximum_version => undef,
    version         => $VERSION,
    description     => getTranslation('This plugin implements export users from SS12000')
};

# our $update_date = $metadata->{date_updated};
# $update_date =~ s/-//g;
our $update_date = $metadata->{date_updated} =~ s/-//gr;

our $version_info = "$update_date v$VERSION";

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


sub get_log_file {
    my $log_config_dir = C4::Context->config("logdir"); 
    return File::Spec->catfile($log_config_dir, 'imcode-export-users.log');
}

# Function to log messages
# Example usage:
# log_message('Yes', 'Here is the message to log');
sub log_message {
    my ($debug_mode, $message) = @_;

    # If debug mode is "Yes"
    if ($debug_mode eq 'Yes') {
        my $my_log_file = get_log_file();

        # Check if the file exists, if not - create it
        unless (-e $my_log_file) {
            eval {
                open my $fh, '>', $my_log_file 
                    or die "Cannot create $my_log_file: $!";
                flock($fh, LOCK_EX) 
                    or die "Cannot lock $my_log_file: $!";
                print $fh "";  # Create an empty file
                close $fh;
            };
            if ($@) {
                warn "Error creating log file: $@";
                return;
            }
        }

        # Open the file for appending data
        eval {
            open my $fh, '>>', $my_log_file 
                or die "Cannot open $my_log_file: $!";
            flock($fh, LOCK_EX) 
                or die "Cannot lock $my_log_file: $!";

            # Get the current date and time
            my $timestamp = strftime "%Y-%m-%d %H:%M:%S", localtime;

            # Write the message to the log file with a timestamp
            print $fh "$timestamp - $message\n";

            # Close the file
            close $fh;
        };
        if ($@) {
            warn "Error writing to log file: $@";
            return;
        }
    }
}

sub install {
    my ( $self, $args ) = @_;

    my $dbh = C4::Context->dbh;

    $self->store_data( { installed_version => $VERSION } );
    $self->store_data( { plugin_version => $Koha::Plugin::imCode::KohaSS12000::ExportUsers::VERSION || '1.0' } );

    log_message("Yes", "Starting installation process");
    log_message("Yes", "Storing initial version: $VERSION");

    my $stored_version = $self->retrieve_data('installed_version');
    log_message("Yes", "Verified stored version: " . ($stored_version || 'none'));

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
        added_count INT DEFAULT 0,
        updated_count INT DEFAULT 0,
        processed_count INT DEFAULT 0,
        iteration_number INT DEFAULT 0,
        organisation_code VARCHAR(255) DEFAULT NULL,
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
    qq{INSERT INTO imcode_config (name,value) VALUES ('archived_limit','0');},
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
        log_message("Yes", "Install Error: $@");
        return 0;
    }

    # LOG trigger in SQL
    my $trigger_sql = qq{
    CREATE TRIGGER log_user_changes
    AFTER UPDATE ON borrowers
    FOR EACH ROW
    BEGIN
        DECLARE change_description TEXT DEFAULT '';

        IF NEW.dateofbirth != OLD.dateofbirth THEN
            SET change_description = CONCAT(change_description, 'field dateofbirth changed from "', OLD.dateofbirth, '" to "', NEW.dateofbirth, '"; ');
        END IF;

        IF NEW.phone != OLD.phone THEN
            SET change_description = CONCAT(change_description, 'field phone changed from "', OLD.phone, '" to "', NEW.phone, '"; ');
        END IF;

        IF NEW.mobile != OLD.mobile THEN
            SET change_description = CONCAT(change_description, 'field mobile changed from "', OLD.mobile, '" to "', NEW.mobile, '"; ');
        END IF;

        IF NEW.surname != OLD.surname THEN
            SET change_description = CONCAT(change_description, 'field surname changed from "', OLD.surname, '" to "', NEW.surname, '"; ');
        END IF;

        IF NEW.firstname != OLD.firstname THEN
            SET change_description = CONCAT(change_description, 'field firstname changed from "', OLD.firstname, '" to "', NEW.firstname, '"; ');
        END IF;

        IF NEW.categorycode != OLD.categorycode THEN
            SET change_description = CONCAT(change_description, 'field categorycode changed from "', OLD.categorycode, '" to "', NEW.categorycode, '"; ');
        END IF;

        IF NEW.branchcode != OLD.branchcode THEN
            SET change_description = CONCAT(change_description, 'field branchcode changed from "', OLD.branchcode, '" to "', NEW.branchcode, '"; ');
        END IF;

        IF NEW.address != OLD.address THEN
            SET change_description = CONCAT(change_description, 'field address changed from "', OLD.address, '" to "', NEW.address, '"; ');
        END IF;

        IF NEW.city != OLD.city THEN
            SET change_description = CONCAT(change_description, 'field city changed from "', OLD.city, '" to "', NEW.city, '"; ');
        END IF;

        IF NEW.zipcode != OLD.zipcode THEN
            SET change_description = CONCAT(change_description, 'field zipcode changed from "', OLD.zipcode, '" to "', NEW.zipcode, '"; ');
        END IF;

        IF NEW.country != OLD.country THEN
            SET change_description = CONCAT(change_description, 'field country changed from "', OLD.country, '" to "', NEW.country, '"; ');
        END IF;

        IF NEW.B_email != OLD.B_email THEN
            SET change_description = CONCAT(change_description, 'field B_email changed from "', OLD.B_email, '" to "', NEW.B_email, '"; ');
        END IF;

        IF NEW.userid != OLD.userid THEN
            SET change_description = CONCAT(change_description, 'field userid changed from "', OLD.userid, '" to "', NEW.userid, '"; ');
        END IF;

        IF NEW.cardnumber != OLD.cardnumber THEN
            SET change_description = CONCAT(change_description, 'field cardnumber changed from "', OLD.cardnumber, '" to "', NEW.cardnumber, '"; ');
        END IF;

        IF NEW.sex != OLD.sex THEN
            SET change_description = CONCAT(change_description, 'field sex changed from "', OLD.sex, '" to "', NEW.sex, '"; ');
        END IF;

        IF NEW.email != OLD.email THEN
            SET change_description = CONCAT(change_description, 'field email changed from "', OLD.email, '" to "', NEW.email, '"; ');
        END IF;

        IF change_description != '' THEN
            INSERT INTO imcode_data_change_log (table_name, record_id, action, change_description)
            VALUES ('borrowers', NEW.borrowernumber, 'update', TRIM(TRAILING '; ' FROM change_description));
        END IF;
    END;
    };

    eval {
        my $trigger_sth = C4::Context->dbh->prepare($trigger_sql);
        $trigger_sth->execute or die C4::Context->dbh->errstr;
    };

    if ($@) {
        warn "Install, CREATE TRIGGER log_user_changes, Error: $@";
        log_message("Yes", "Install, CREATE TRIGGER log_user_changes, Error: $@");
        return 0;
    }

    return 1;
}

sub upgrade {
    my ( $self, $args ) = @_;
    
    my $dbh = C4::Context->dbh;
    my $success = 1;

    # Ensure $VERSION is defined
    our $VERSION = $VERSION || '1.0';
    log_message("Yes", "Starting upgrade process for plugin version $VERSION");

    # Check if this is a new installation
    my $installed_version = $self->retrieve_data('installed_version') || '0';
    my $is_new_install = ($installed_version eq '0');

    log_message("Yes", "Is new install: " . ($is_new_install ? "Yes" : "No"));
    log_message("Yes", "Installed version: $installed_version");

    # Add new columns to imcode_logs table
    my $alter_table_sql = q{
        ALTER TABLE imcode_logs
        ADD COLUMN IF NOT EXISTS added_count INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS updated_count INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS processed_count INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS iteration_number INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS organisation_code VARCHAR(255) DEFAULT NULL
    };

    eval {
        $dbh->do($alter_table_sql) or die "Failed to alter table: " . $dbh->errstr;
        log_message("Yes", "Table imcode_logs altered successfully");
    };
    if ($@) {
        warn "Error altering table: $@";
        $success = 0;
    }

    # Always attempt to drop the trigger first
    my $drop_trigger_sql = q{
        DROP TRIGGER IF EXISTS log_user_changes
    };

    eval {
        $dbh->do($drop_trigger_sql) or die "Failed to drop trigger: " . $dbh->errstr;
        log_message("Yes", "Existing trigger dropped successfully (if it existed)");
    };
    if ($@) {
        log_message("Yes", "Error dropping trigger: $@");
        $success = 0;
    }

    # Create the new trigger
    my $create_trigger_sql = q{
    CREATE TRIGGER log_user_changes
    AFTER UPDATE ON borrowers
    FOR EACH ROW
    BEGIN
        DECLARE change_description TEXT DEFAULT '';

        IF NEW.dateofbirth != OLD.dateofbirth THEN
            SET change_description = CONCAT(change_description, 'field dateofbirth changed from "', OLD.dateofbirth, '" to "', NEW.dateofbirth, '"; ');
        END IF;

        IF NEW.phone != OLD.phone THEN
            SET change_description = CONCAT(change_description, 'field phone changed from "', OLD.phone, '" to "', NEW.phone, '"; ');
        END IF;

        IF NEW.mobile != OLD.mobile THEN
            SET change_description = CONCAT(change_description, 'field mobile changed from "', OLD.mobile, '" to "', NEW.mobile, '"; ');
        END IF;

        IF NEW.surname != OLD.surname THEN
            SET change_description = CONCAT(change_description, 'field surname changed from "', OLD.surname, '" to "', NEW.surname, '"; ');
        END IF;

        IF NEW.firstname != OLD.firstname THEN
            SET change_description = CONCAT(change_description, 'field firstname changed from "', OLD.firstname, '" to "', NEW.firstname, '"; ');
        END IF;

        IF NEW.categorycode != OLD.categorycode THEN
            SET change_description = CONCAT(change_description, 'field categorycode changed from "', OLD.categorycode, '" to "', NEW.categorycode, '"; ');
        END IF;

        IF NEW.branchcode != OLD.branchcode THEN
            SET change_description = CONCAT(change_description, 'field branchcode changed from "', OLD.branchcode, '" to "', NEW.branchcode, '"; ');
        END IF;

        IF NEW.address != OLD.address THEN
            SET change_description = CONCAT(change_description, 'field address changed from "', OLD.address, '" to "', NEW.address, '"; ');
        END IF;

        IF NEW.city != OLD.city THEN
            SET change_description = CONCAT(change_description, 'field city changed from "', OLD.city, '" to "', NEW.city, '"; ');
        END IF;

        IF NEW.zipcode != OLD.zipcode THEN
            SET change_description = CONCAT(change_description, 'field zipcode changed from "', OLD.zipcode, '" to "', NEW.zipcode, '"; ');
        END IF;

        IF NEW.country != OLD.country THEN
            SET change_description = CONCAT(change_description, 'field country changed from "', OLD.country, '" to "', NEW.country, '"; ');
        END IF;

        IF NEW.B_email != OLD.B_email THEN
            SET change_description = CONCAT(change_description, 'field B_email changed from "', OLD.B_email, '" to "', NEW.B_email, '"; ');
        END IF;

        IF NEW.userid != OLD.userid THEN
            SET change_description = CONCAT(change_description, 'field userid changed from "', OLD.userid, '" to "', NEW.userid, '"; ');
        END IF;

        IF NEW.cardnumber != OLD.cardnumber THEN
            SET change_description = CONCAT(change_description, 'field cardnumber changed from "', OLD.cardnumber, '" to "', NEW.cardnumber, '"; ');
        END IF;

        IF NEW.sex != OLD.sex THEN
            SET change_description = CONCAT(change_description, 'field sex changed from "', OLD.sex, '" to "', NEW.sex, '"; ');
        END IF;

        IF NEW.email != OLD.email THEN
            SET change_description = CONCAT(change_description, 'field email changed from "', OLD.email, '" to "', NEW.email, '"; ');
        END IF;

        IF change_description != '' THEN
            INSERT INTO imcode_data_change_log (table_name, record_id, action, change_description)
            VALUES ('borrowers', NEW.borrowernumber, 'update', TRIM(TRAILING '; ' FROM change_description));
        END IF;
    END
    };

    eval {
        $dbh->do($create_trigger_sql) or die "Failed to create trigger: " . $dbh->errstr;
        log_message("Yes", "New trigger created successfully");
    };
    if ($@) {
        log_message("Yes", "Error creating trigger: $@");
        $success = 0;
    }

    # Verify the trigger was created
    my $verify_trigger_sql = q{
        SELECT TRIGGER_NAME FROM information_schema.triggers 
        WHERE trigger_schema = DATABASE()
        AND trigger_name = 'log_user_changes'
    };
    my $verify_sth = $dbh->prepare($verify_trigger_sql);
    $verify_sth->execute;
    my ($verified_trigger) = $verify_sth->fetchrow_array;

    if ($verified_trigger) {
        log_message("Yes", "Trigger verified: log_user_changes exists in the database");
    } else {
        log_message("Yes", "Error: Trigger log_user_changes not found in the database after creation attempt");
        $success = 0;
    }

    # Log the upgrade or installation result
    if ($success) {
        if ($is_new_install) {
            log_message("Yes", "Plugin installed successfully (version $VERSION)");
        } else {
            log_message("Yes", "Plugin upgraded successfully to version $VERSION");
        }
        # Store the new version
        $self->store_data({ installed_version => $VERSION });
        $self->store_data({ plugin_version => $VERSION });
    } else {
        if ($is_new_install) {
            log_message("Yes", "Plugin installation failed (version $VERSION)");
        } else {
            log_message("Yes", "Plugin upgrade to version $VERSION failed");
        }
    }

    return $success;
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
            log_message("Yes", "Error while inserting config value: $@");
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
    insertConfigValue($dbh, 'archived_limit', '0');

    my $select_query = qq{SELECT name, value FROM $config_table};
    my $config_data  = {};

    my $missing_modules = 0;
    eval {
            require URI::Encode;
            URI::Encode->import(qw(uri_encode));
    };
    if ($@) {
        warn "Missing required module: URI::Encode qw(uri_encode) \n";
        log_message("Yes", "Missing required module: URI::Encode qw(uri_encode)");
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

    # Pass success or error message to the template
    $template->param(success => $self->{'success'}) if $self->{'success'};
    $template->param(error => $self->{'error'}) if $self->{'error'};
    my $count_log_query = "SELECT COUNT(*) FROM imcode_logs WHERE DATE(created_at) = CURDATE() AND is_processed = 1";
    my ($log_count) = $dbh->selectrow_array($count_log_query);
    $template->param(log_count => $log_count);

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
        my $archived_limit      = int($cgi->param('archived_limit'));

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
            my $check_mapping_query = qq{
                SELECT organisationCode 
                FROM $branches_mapping_table 
                WHERE organisationCode = ? 
            };
            my $sth_check_mapping = $dbh->prepare($check_mapping_query);
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
            my $check_mapping_query = qq{
                SELECT categorycode 
                FROM $categories_mapping_table 
                WHERE categorycode = ? 
            };
            my $sth_check_mapping = $dbh->prepare($check_mapping_query);
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
                my $update_query = qq{
                    UPDATE $logs_table 
                    SET is_processed = 0,
                        page_token_next = NULL
                    WHERE DATE(created_at) = CURDATE()
                };

                my $sth_update = $dbh->prepare($update_query);
                eval {
                    if ($sth_update->execute()) {
                        # warn "Updated records in $logs_table for current date. Configuration change \n";
                        log_message("Yes", "Updated records in $logs_table for current date. Configuration change");
                        sleep(1); # sleep for 1 second 
                    } else {
                        log_message("Yes", "Error updating data in $logs_table: " . $dbh->errstr);
                        die "Error updating data in $logs_table: " . $dbh->errstr . "\n";
                    }
                };
                if ($@) {
                    log_message("Yes", "Database error: $@");
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
                WHEN name = 'archived_limit' THEN ?
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
                'archived_limit',
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
                $archived_limit,
                $excluding_dutyRole_empty,
                $excluding_enrolments_empty
                );
            $template->param(success => 'success');
        };

        if ($@) {
            log_message("Yes","Error updating configuration: $@");
            warn "Error updating configuration: $@";
        }
    }
    elsif ($op eq 'cud-clearlog-config') {
         # my $clean_query = qq{
         #     TRUNCATE TABLE imcode_logs
         # };
            my $clean_query = qq{
                UPDATE imcode_logs 
                SET is_processed = 0, updated_count = 0, added_count = 0, 
                    page_token_next = NULL
                WHERE DATE(created_at) = CURDATE()
            };
        eval { $dbh->do($clean_query) };

        if ($@) {
            log_message("Yes", "Error while run clean_query: $@");
            warn "Error while run clean_query: $@";
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
        log_message("Yes", "Error while adding column: $@");
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
        log_message("Yes", "Error fetching configuration: $@");
        warn "Error fetching configuration: $@";
    }

    my $session_id = $cgi->cookie('CGISESSID');
    unless ($session_id) {
        log_message("Yes", "Session ID not found");
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
        archived_limit      => int($config_data->{archived_limit}) || 0,
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
        log_message("Yes", "Session ID not found");
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
        log_message("Yes","Error fetching configuration values: $@");
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
            log_message("Yes", "Error fetching, info about users data update: $@");
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
            log_message("Yes", "Error fetching data from $logs_table, details: $@");
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
            log_message("Yes", "Error fetching Statistics: $@");
            warn "Error fetching Statistics: $@";
        }

        # Pass the data to the template for display
        $template->param(
            stats => \@stats
        );
    }

    $template->param(
            language => C4::Languages::getlanguage($cgi) || 'en',
            mbf_path => abs_path( $self->mbf_path('translations') ),
            csrf_token => $csrf_token,
    );

    print $cgi->header( -type => 'text/html', -charset => 'utf-8' );
    print $template->output();
}


sub cronjob {
    my ($self, $data_endpoint) = @_;
    
    my $dbh = C4::Context->dbh;

    # First, check if full processing was already completed today
    my $check_completed_query = qq{
        SELECT COUNT(*) 
        FROM $logs_table 
        WHERE DATE(created_at) = CURDATE()
        AND data_endpoint = ?
        AND page_token_next IS NULL 
        AND is_processed = 1
        GROUP BY organisation_code
    };
    
    my $sth_check = $dbh->prepare($check_completed_query);
    $sth_check->execute($data_endpoint);
    my $completed_orgs = $sth_check->fetchall_arrayref();
    
    # Get total number of organizations that need processing
    my $total_orgs_query = qq{
        SELECT COUNT(DISTINCT organisationCode) 
        FROM $branches_mapping_table 
        WHERE organisationCode IS NOT NULL 
        AND organisationCode != ''
    };
    
    my ($total_orgs) = $dbh->selectrow_array($total_orgs_query);
    
    # If no organizations configured, treat as single process
    if ($total_orgs == 0) {
        $total_orgs = 1;
    }
    
    # If number of completed organizations equals total organizations, exit
    if (scalar(@$completed_orgs) >= $total_orgs) {
        log_message("Yes", "Full processing cycle already completed today for all organizations");
        print "EndLastPageFromAPI\n";
        return 0;
    }

    # Check if mapping table has any records
    my $check_mapping_exists = qq{
        SELECT COUNT(*) FROM $branches_mapping_table
    };
    my ($mapping_exists) = $dbh->selectrow_array($check_mapping_exists);
    
    # Get current date
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
    my $today = sprintf("%04d-%02d-%02d", $year + 1900, $mon + 1, $mday);
    
    # Clean old logs
    my $config_data = $self->get_config_data();
    my $logs_limit = int($config_data->{logs_limit}) || 3;
    
    my $cleanup_query = qq{
        DELETE FROM $logs_table
        WHERE created_at < DATE_SUB(CURDATE(), INTERVAL ? DAY)
    };
    $dbh->do($cleanup_query, undef, $logs_limit);

    # Clean archived userid/cardnumber
    my $archived_limit = int($config_data->{archived_limit}) || 0;

    if ($archived_limit > 0) {
        my $cleanup_query = qq{
            DELETE FROM borrowers
            WHERE 
                (cardnumber LIKE 'ARCHIVED_%' OR userid LIKE 'ARCHIVED_%')
                AND updated_on < DATE_SUB(NOW(), INTERVAL ? DAY)
        };
        my $rows_deleted = int($dbh->do($cleanup_query, undef, $archived_limit));
        if ($rows_deleted) {
            log_message("Yes", "Deleted $rows_deleted archived users older than $archived_limit days");
        }
    }
    
    if ($mapping_exists > 0) {
        # Process with organization filtering
        my $select_branches_query = qq{
            SELECT DISTINCT bm.organisationCode 
            FROM $branches_mapping_table bm
            WHERE bm.organisationCode IS NOT NULL 
            AND bm.organisationCode != ''
            AND bm.organisationCode NOT IN (
                SELECT DISTINCT l.organisation_code 
                FROM $logs_table l
                WHERE DATE(l.created_at) = CURDATE()
                AND l.page_token_next IS NULL 
                AND l.data_endpoint = ?
                AND l.is_processed = 1
            )
        };
        
        my $sth = $dbh->prepare($select_branches_query);
        $sth->execute($data_endpoint);
        
        my @organisation_codes;
        while (my ($org_code) = $sth->fetchrow_array) {
            push @organisation_codes, $org_code;
        }
        
        if (@organisation_codes) {
            log_message('Yes', 'Found ' . scalar(@organisation_codes) . ' unprocessed organisation codes');
            
            foreach my $org_code (@organisation_codes) {
                # Check if this specific organisation is already completed
                my $check_org_query = qq{
                    SELECT 1 FROM $logs_table 
                    WHERE DATE(created_at) = CURDATE()
                    AND organisation_code = ?
                    AND page_token_next IS NULL
                    AND data_endpoint = ?
                    AND is_processed = 1
                };
                
                my ($org_completed) = $dbh->selectrow_array($check_org_query, undef, $org_code, $data_endpoint);
                if ($org_completed) {
                    log_message('Yes', "Organisation $org_code already processed today, skipping");
                    next;
                }
                
                log_message('Yes', "Processing organisation code: $org_code");
                
                my $org_id = $self->get_organisation_id($org_code, $config_data);
                
                if ($org_id) {
                    log_message('Yes', "Got organisation ID: $org_id for code: $org_code");
                    
                    my $filter_params = {
                        'relationship.organisation' => $org_id,
                        'relationship.startDate.onOrBefore' => $today,
                        'relationship.endDate.onOrAfter' => $today,
                        'relationship.entity.type' => 'enrolment'
                    };
                    
                    eval {
                        my $result = $self->fetchDataFromAPI($data_endpoint, $filter_params, $org_code);
                        if ($result == 0) {
                            log_message('Yes', "Processing completed for organisation $org_code");
                        }
                    };
                    
                    if ($@) {
                        if ($@ =~ /EndLastPageFromAPI/) {
                            log_message('Yes', "Reached last page for organisation $org_code");
                        } elsif ($@ =~ /ErrorVerifyCategorycodeBranchcode/) {
                            log_message('Yes', "Configuration error for organisation $org_code");
                            print "ErrorVerifyCategorycodeBranchcode\n";
                            die $@;
                        } else {
                            log_message('Yes', "Error processing organisation $org_code: $@");
                        }
                    }
                } else {
                    log_message('Yes', "Could not get ID for organisation code: $org_code");
                }
            }
            
            # Check if all organisations are now processed
            my $check_all_completed = qq{
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT organisationCode 
                    FROM $branches_mapping_table 
                    WHERE organisationCode IS NOT NULL 
                    AND organisationCode != ''
                    AND organisationCode NOT IN (
                        SELECT DISTINCT organisation_code 
                        FROM $logs_table 
                        WHERE DATE(created_at) = CURDATE()
                        AND page_token_next IS NULL 
                        AND data_endpoint = ?
                        AND is_processed = 1
                    )
                ) AS remaining_orgs
            };
            
            my ($remaining_count) = $dbh->selectrow_array($check_all_completed, undef, $data_endpoint);
            if ($remaining_count == 0) {
                log_message('Yes', "All organisations processed successfully");

                # Get and log final statistics
                my $stats_query = qq{
                    SELECT 
                        COUNT(DISTINCT organisation_code) as orgs_processed,
                        SUM(added_count) as total_added,
                        SUM(updated_count) as total_updated,
                        SUM(processed_count) as total_processed
                    FROM $logs_table 
                    WHERE data_endpoint = ?
                    AND DATE(created_at) = CURDATE()
                };
                
                my $sth_stats = $dbh->prepare($stats_query);
                $sth_stats->execute($data_endpoint);
                my $stats = $sth_stats->fetchrow_hashref;
                
                if ($stats) {
                    log_message('Yes', sprintf(
                        "Daily statistics - Organisations processed: %d, Added: %d, Updated: %d, Total processed: %d",
                        $stats->{orgs_processed} || 0,
                        $stats->{total_added} || 0,
                        $stats->{total_updated} || 0,
                        $stats->{total_processed} || 0
                    ));      
                }

                print "EndLastPageFromAPI\n";
            }
        } else {
            log_message('Yes', "No organizations left to process today");
            return 0;
        }
    } else {
        # Process without organization filtering
        # First check if already processed today
        my $check_processed = qq{
            SELECT COUNT(*) 
            FROM $logs_table 
            WHERE DATE(created_at) = CURDATE()
            AND data_endpoint = ?
            AND page_token_next IS NULL 
            AND is_processed = 1
            AND organisation_code = 'NO_ORG'
        };
        
        my ($already_processed) = $dbh->selectrow_array($check_processed, undef, $data_endpoint);
        
        if ($already_processed) {
            log_message('Yes', 'Processing already completed today for non-organization mode');
            return 0;
        }
        
        log_message('Yes', 'No organisation mappings found, processing all data without filtering');
        
        my $filter_params = {
            'relationship.startDate.onOrBefore' => $today,
            'relationship.endDate.onOrAfter' => $today,
            'relationship.entity.type' => 'enrolment'
        };
        
        my $config_data = $self->get_config_data();
        log_message('Yes', 'Got config data');
        
        my $ua = LWP::UserAgent->new;
        my $token = $self->get_api_token($config_data, $ua);
        
        if (!$token) {
            log_message('Yes', 'Failed to get API token');
            return 0;
        }
        log_message('Yes', 'Got API token successfully');
        
        eval {
            log_message('Yes', "Starting fetchDataFromAPI for endpoint: $data_endpoint");
            my $result = $self->fetchDataFromAPI($data_endpoint, $filter_params, 'NO_ORG');
            log_message('Yes', "fetchDataFromAPI result: " . ($result // 'undef'));
            
            if (defined $result && $result == 0) {
                log_message('Yes', "Processing completed without organisation filtering");
                print "EndLastPageFromAPI\n";
                return 0;
            }
        };
        
        if ($@) {
            log_message('Yes', "Caught error: $@");
            if ($@ =~ /EndLastPageFromAPI/) {
                print "EndLastPageFromAPI\n";
                log_message('Yes', "Processing completed without organisation filtering");
                return 0;
            } elsif ($@ =~ /ErrorVerifyCategorycodeBranchcode/) {
                print "ErrorVerifyCategorycodeBranchcode\n";
                log_message('Yes', "Configuration error detected");
                die $@;
            } else {
                log_message('Yes', "Unknown error during processing: $@");
                die $@;
            }
        }
    }
    
    return 1;
}


# Helper function to get configuration data
sub get_config_data {
    my ($self) = @_;
    my $dbh = C4::Context->dbh;
    
    my $select_query = qq{SELECT name, value FROM $config_table};
    my $config_data = {};
    
    eval {
        my $sth = $dbh->prepare($select_query);
        $sth->execute();
        while (my ($name, $value) = $sth->fetchrow_array) {
            $config_data->{$name} = $value;
        }
    };
    
    return $config_data;
}

# Helper function to get organisation ID by code
sub get_organisation_id {
    my ($self, $org_code, $config_data) = @_;
    
    # Get API access token first
    my $ua = LWP::UserAgent->new;
    my $token = $self->get_api_token($config_data, $ua);
    return unless $token;
    
    # Build URL for organisations endpoint
    my $ist_url = $config_data->{ist_api_url} || '';
    my $customerId = $config_data->{ist_customer_id} || '';
    my $org_url = "$ist_url/ss12000v2-api/source/$customerId/v2.0/organisations?organisationCode=$org_code";
    
    # Make request to get organisation details
    my $request = HTTP::Request->new(
        'GET',
        $org_url,
        [
            'Accept' => 'application/json',
            'Authorization' => "Bearer $token"
        ]
    );
    
    my $response = $ua->request($request);
    if ($response->is_success) {
        my $org_data = decode_json($response->content);
        if ($org_data && $org_data->{data} && @{$org_data->{data}}) {
            return $org_data->{data}[0]->{id};
        }
    }
    
    log_message("Yes", "Failed to get organisation ID for code $org_code: " . $response->status_line);
    warn "Failed to get organisation ID for code $org_code: " . $response->status_line;
    return;
}

# Helper function to get API token
sub get_api_token {
    my ($self, $config_data, $ua) = @_;
    
    my $client_id = $config_data->{ist_client_id} || '';
    my $client_secret = xor_encrypt($config_data->{ist_client_secret}, $skey) || '';
    my $oauth_url = $config_data->{ist_oauth_url} || '';
    
    my $token_request = POST $oauth_url, [
        client_id => $client_id,
        client_secret => $client_secret,
        grant_type => 'client_credentials'
    ];
    
    my $token_response = $ua->request($token_request);
    if ($token_response->is_success) {
        my $token_data = decode_json($token_response->content);
        return $token_data->{access_token};
    }

    log_message("Yes","Failed to get API token: " . $token_response->status_line);
    warn "Failed to get API token: " . $token_response->status_line;
    return;
}


sub fetchDataFromAPI {
    my ($self, $data_endpoint, $filter_params, $current_org_code) = @_;

    my $dbh = C4::Context->dbh;
    my $response_page_token;     

    # Reset counters at start of processing
    our $added_count = 0;
    our $updated_count = 0;
    our $processed_count = 0;

    if (verify_categorycode_and_branchcode() eq "No") {
        warn "WARNING: branches mapping and/or categories mapping not configured correctly";
        die "ErrorVerifyCategorycodeBranchcode";
    } 

    my $cgi = $self->{'cgi'};

    my $select_query = qq{SELECT name, value FROM $config_table};
    my $config_data  = {};

    my $insert_error_query = qq{
            INSERT INTO $logs_table (page_token_next, response, organisation_code)
            VALUES (?, ?, ?)
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
        my $sth_insert_error = $dbh->prepare($insert_error_query);
        $sth_insert_error->execute('Configuration Error', $@, $current_org_code);
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
    my $archived_limit      = int($config_data->{archived_limit}) || 0;
    my $excluding_enrolments_empty = $config_data->{excluding_enrolments_empty} || 'No';
    my $excluding_dutyRole_empty = $config_data->{excluding_dutyRole_empty} || 'No';
    
    if ($debug_mode eq "Yes") { 
        log_message($debug_mode, "Starting processing for endpoint: $data_endpoint, organisation: $current_org_code");
        log_message($debug_mode, "Filter params: " . Dumper($filter_params)) if $filter_params;
    }

    my $ua = LWP::UserAgent->new;
    my $api_url = "$ist_url/ss12000v2-api/source/$customerId/v2.0/$data_endpoint?limit=$api_limit";
    my $api_url_base = "$ist_url/ss12000v2-api/source/$customerId/v2.0/";

    if ($filter_params && ref($filter_params) eq 'HASH') {
        my $encoded_params = join '&', map {
            uri_escape($_) . '=' . uri_escape($filter_params->{$_})
        } keys %$filter_params;
        $api_url .= "&$encoded_params" if $encoded_params;
    }

    my $request = POST $oauth_url, [
        client_id     => $client_id,
        client_secret => $client_secret,
        grant_type    => 'client_credentials',
    ];

    my $oauth_response = $ua->request($request);

    if ($oauth_response->is_success) {
        my $oauth_content = decode_json($oauth_response->decoded_content);
        my $access_token = $oauth_content->{access_token};

        # Get the latest token for current organisation
        my $select_tokens_query = qq{
            SELECT id, page_token_next
            FROM $logs_table
            WHERE is_processed = 1
            AND data_endpoint = ?
            AND organisation_code = ?
            AND DATE(created_at) = CURDATE()
            AND page_token_next IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
        };
        
        my $sth_select_tokens = $dbh->prepare($select_tokens_query);
        $sth_select_tokens->execute($data_endpoint, $current_org_code);
        my ($data_id, $page_token_next) = $sth_select_tokens->fetchrow_array;

        if (defined $page_token_next) {
            $api_url = $api_url."&pageToken=$page_token_next";
        } 

        my $response_data = getApiResponse($api_url, $access_token);
        my $response = $debug_mode eq "Yes" ? $response_data : "Debug Mode OFF";

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

        $response_page_token = $response_data->{pageToken};

        my $md5 = Digest::MD5->new;
        $md5->add(encode_json($response_data));
        my $data_hash = $md5->hexdigest;

        my $select_existing_query = qq{
            SELECT is_processed
            FROM $logs_table
            WHERE data_hash = ?
            AND organisation_code = ?
            ORDER BY created_at DESC
            LIMIT 1
        };

        my $sth_select = $dbh->prepare($select_existing_query);
        $sth_select->execute($data_hash, $current_org_code);

        if (my ($is_processed) = $sth_select->fetchrow_array) {
            warn "Record with data_hash=$data_hash for organisation $current_org_code already exists (processed=$is_processed)";
        } else {
            my $insert_query = qq{
                INSERT INTO $logs_table (
                    page_token_next, 
                    response, 
                    record_count, 
                    data_hash, 
                    data_endpoint,
                    organisation_code
                )
                VALUES (?, ?, ?, ?, ?, ?)
            };

            my $sth_insert = $dbh->prepare($insert_query);
            eval {
                if ($sth_insert->execute(
                    $response_page_token, 
                    $response, 
                    $api_limit, 
                    $data_hash, 
                    $data_endpoint,
                    $current_org_code
                )) {
                    # warn "Data from API successfully inserted into $logs_table";
                    log_message($debug_mode, "Data from API successfully inserted into $logs_table ");
                } else {
                    die "Error inserting data into $logs_table: " . $dbh->errstr;
                }
            };
            if ($@) {
                warn "Database error: $@";
            }
        }

        # Get mappings
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
            warn "Error fetching mappings: $@";
        }

        # Get current iteration number
        my $select_iteration = qq{
            SELECT COALESCE(MAX(iteration_number), 0) + 1 
            FROM $logs_table 
            WHERE data_endpoint = ?
            AND organisation_code = ?
            AND DATE(created_at) = CURDATE()
        };
        my ($iteration_number) = $dbh->selectrow_array($select_iteration, undef, $data_endpoint, $current_org_code);

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
                my $update_query = qq{
                    UPDATE $logs_table
                    SET is_processed = 1,
                        page_token_next = NULL,
                        response = ?,
                        added_count = ?,
                        updated_count = ?,
                        processed_count = ?,
                        iteration_number = ?
                    WHERE data_hash = ?
                };
                
                # Get organization-specific totals
                my $org_stats_query = qq{
                    SELECT
                        SUM(processed_count) as org_processed
                    FROM $logs_table
                    WHERE organisation_code = ?
                    AND DATE(created_at) = CURDATE()
                    AND data_endpoint = 'persons'
                };
                my $sth_org_stats = $dbh->prepare($org_stats_query);
                $sth_org_stats->execute($current_org_code);
                my ($org_processed) = $sth_org_stats->fetchrow_array();

                my $completion_message = sprintf(
                    "Processing completed for %s. Organization statistics, total processed: %d",
                    $current_org_code,
                    $org_processed || 0
                );

                log_message('Yes', $completion_message);

                my $sth_update = $dbh->prepare($update_query);
                $sth_update->execute(
                    $completion_message,
                    $added_count,
                    $updated_count,
                    $processed_count,
                    $iteration_number,
                    $data_hash
                );

                die "EndLastPageFromAPI";
            }

            # Update the current record with counts
            my $update_query = qq{
                UPDATE $logs_table
                SET is_processed = 1,
                    added_count = ?,
                    updated_count = ?,
                    processed_count = ?,
                    iteration_number = ?
                WHERE data_hash = ?
            };

            my $sth_update = $dbh->prepare($update_query);
            $sth_update->execute(
                $added_count,
                $updated_count,
                $processed_count,
                $iteration_number,
                $data_hash
            );
        }
    } else {
        my $oauth_error_message = "Error get access_token: " . $oauth_response->status_line;
        warn $oauth_error_message;
        my $sth_insert_oauth_error = $dbh->prepare($insert_error_query);
        $sth_insert_oauth_error->execute('OAuth Error', $oauth_error_message, $current_org_code);
        return 0;
    }

    return 1;
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

    log_message($debug_mode, 'data from mysql, categories_mapping: '.Dumper(\@categories_mapping));
    log_message($debug_mode, 'data from mysql, branches_mapping: '.Dumper(\@branches_mapping));

    my $dbh = C4::Context->dbh;

    my $select_iteration = qq{
        SELECT COALESCE(MAX(iteration_number), 0) + 1 
        FROM $logs_table 
        WHERE data_endpoint = 'persons'
    };
    my ($iteration_number) = $dbh->selectrow_array($select_iteration);

            my $j = 0;
            for my $i (1..$api_limit) {

                my $koha_categorycode = $koha_default_categorycode;
                my $koha_branchcode = $koha_default_branchcode;
                my $not_import = 0;

                my $response_page_data = $response_data->{data}[$i-1];
                if ($response_page_data) {

                    log_message($debug_mode, 'STARTED DEBUGGING THE CURRENT USER');
                    log_message($debug_mode, 'koha_default_categorycode: '.$koha_default_categorycode);
                    log_message($debug_mode, 'koha_default_branchcode: '.$koha_default_branchcode);

                    my $id = $response_page_data->{id};
                    log_message($debug_mode, 'api response_page_data->{id}: '.$id);

                    # start search "groupType": "Klass"
                    my $person_groupMemberships_api_url = $api_url_base."persons/".$id."?expand=groupMemberships";
                    log_message($debug_mode, 'person_groupMemberships_api_url: '.$person_groupMemberships_api_url);
                    my $response_data_groupMemberships;
                    eval {
                        $response_data_groupMemberships = decode_json(getApiResponse($person_groupMemberships_api_url, $access_token));
                        log_message($debug_mode, 'response_data_groupMemberships: '.Dumper($response_data_groupMemberships));
                    };

                    use DateTime;
                    my $dt = DateTime->now;
                    my $today = $dt->ymd;

                    my $klass_displayName;
                    log_message($debug_mode, '::groupMembership (trying to get Klass) BEGIN');
                    foreach my $groupMembership (@{$response_data_groupMemberships ->{_embedded}->{groupMemberships}}) {
                        my $group = $groupMembership->{group};
                        log_message($debug_mode, 'groupMembership->{group}: '.Dumper($group));
                        # "groupType" & "endDate" 
                        # 2024-05-24 added "startDate" for case like - to_date: 2025-06-30 from_date: 2024-07-01
                        if ($group->{groupType} eq "Klass" && $group->{endDate} gt $today && $group->{startDate} lt $today) {
                            $klass_displayName = $group->{displayName};
                            log_message($debug_mode, 'klass_displayName: '.$klass_displayName);
                            log_message($debug_mode, 'group->{endDate}: '.$group->{endDate});
                            log_message($debug_mode, 'group->{startDate}: '.$group->{startDate});
                            last; 
                        }
                    }
                    log_message($debug_mode, '::groupMembership END');
                    # end search "groupType": "Klass"

                    # my $api_url_base = "$ist_url/ss12000v2-api/source/$customerId/v2.0/";
                    # dutyRole @categories_mapping
                    my $person_api_url = $api_url_base."duties?person=".$id;
                    log_message($debug_mode, 'person_api_url: '.$person_api_url);
                    my $response_data_person;
                    my $duty_role;


                    eval {
                        $response_data_person = decode_json(getApiResponse($person_api_url, $access_token));
                        log_message($debug_mode, 'response_data_person: '.Dumper($response_data_person));
                    };

                    if (
                            $response_data_person && 
                            ref($response_data_person) eq 'HASH' && 
                            $response_data_person->{data} && 
                            ref($response_data_person->{data}) eq 'ARRAY' && 
                            @{$response_data_person->{data}}
                        ) {
                        $duty_role = $response_data_person->{data}[0]->{dutyRole};
                    } 


                    log_message($debug_mode, '::duty_role BEGIN');
                    if ($duty_role) {
                        log_message($debug_mode, 'duty_role: '.$duty_role);
                        log_message($debug_mode, "Checking categories_mapping, in categories_mapping we have: ". Dumper(@categories_mapping));
                        foreach my $category_mapping (@categories_mapping) {
                            if ($category_mapping->{dutyRole} && $category_mapping->{dutyRole} eq $duty_role) {
                                $koha_categorycode = $category_mapping->{categorycode};
                                $not_import = $category_mapping->{not_import} || 0;
                                log_message($debug_mode, 'Geted not_import flag from mysql base, category_mapping import set to: '.($not_import ? 'no' : 'yes'));
                                last; 
                            } 
                            log_message($debug_mode, 'koha_categorycode settled to: '.$koha_categorycode);
                            log_message($debug_mode, 'not_import flag settled to: '.($not_import ? 'no' : 'yes'));
                        }
                    } else {
                        if ($excluding_dutyRole_empty eq "Yes") {
                                $not_import = 1;
                                log_message($debug_mode, 'Duty_role is empty, not import data, excluding_dutyRole_empty in config settled to Yes');
                        }
                    }
                    log_message($debug_mode, '::duty_role END');
                    # /dutyRole

                    # organisationCode @branches_mapping
                    my $enrolments = $response_page_data->{enrolments}; 
                    log_message($debug_mode, 'enrolments: '.Dumper($enrolments));
                    my $enroledAtId = "";

                    log_message($debug_mode, '::organisationCode BEGIN');

                    if (defined $enrolments && ref $enrolments eq 'ARRAY') {
                        foreach my $enrolment (@$enrolments) {
                            my $enroledAt = $enrolment->{enroledAt}; 
                            if (defined $enroledAt && ref $enroledAt eq 'HASH') {
                                $enroledAtId = $enroledAt->{id}; 
                            }
                        }
                    }

                    if ($enroledAtId) {
                        log_message($debug_mode, 'enroledAtId: '.$enroledAtId);
                        
                        my $person_api_url = $api_url_base."organisations/".$enroledAtId;
                        log_message($debug_mode, 'person_api_url: '.$person_api_url);
                        my $organisationCode;

                        eval {
                            $response_data_person = decode_json(getApiResponse($person_api_url, $access_token));
                            log_message($debug_mode, 'response_data_person: '.Dumper($response_data_person));
                        };

                        if (defined $response_data_person && ref($response_data_person) eq 'HASH' && defined $response_data_person->{organisationCode}) {
                            $organisationCode = $response_data_person->{organisationCode};
                        }

                        if ($organisationCode) {
                            log_message($debug_mode, 'organisationCode: '.$organisationCode);
                            log_message($debug_mode, "Checking branches_mapping, in branches_mapping we have: ". Dumper(@branches_mapping));
                            foreach my $branch_mapping (@branches_mapping) {
                                if ($branch_mapping->{organisationCode} && $branch_mapping->{organisationCode} eq $organisationCode) {
                                    $koha_branchcode = $branch_mapping->{branchcode};
                                    log_message($debug_mode, 'Checking branch_mapping, koha_branchcode: '.$koha_branchcode);
                                    last; 
                                } 
                            }
                            log_message($debug_mode, 'koha_branchcode settled to: '.$koha_branchcode);
                        }
                        # /organisationCode
                    } else {

                        if ($excluding_enrolments_empty eq "Yes") {
                                $not_import = 1;
                                log_message($debug_mode, 'Enrolments is empty, not import data, excluding_Enrolments_empty in config settled to Yes');
                        }

                    }
                    log_message($debug_mode, '::organisationCode END');

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
                    log_message($debug_mode, 'givenName: '.$givenName);
                    log_message($debug_mode, 'familyName: '.$familyName);
                    log_message($debug_mode, 'birthDate: '.$birthDate);
                    log_message($debug_mode, 'sex: '.$sex);

                    my $emails = $response_page_data->{emails}; # we get an array
                    my $email = "";
                    my $B_email = ""; # field B_email in DB

                    # ver 1.521:
                    if (defined $emails && ref $emails eq 'ARRAY') {
                        my $found_private = 0;
                        my $first_non_private;
  
                        foreach my $selectedEmail (@$emails) {
                            next unless defined $selectedEmail->{value};
                            
                            if ($selectedEmail->{type} eq "Privat") {
                                $B_email = lc($selectedEmail->{value});
                                $found_private = 1;
                            } elsif (!defined $first_non_private) {
                                $first_non_private = lc($selectedEmail->{value});
                            }
                        }

                        if (defined $first_non_private) {
                            $email = $first_non_private;
                        }
                    }

                    log_message($debug_mode, 'Geted emails from api: '.Dumper($emails));
                    log_message($debug_mode, 'email: '.$email);
                    log_message($debug_mode, 'B_email: '.$B_email);

                    my $addresses = $response_page_data->{addresses}; # we get an array
                    log_message($debug_mode, 'Geted addresses from api: '.Dumper($addresses));
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
                    log_message($debug_mode, 'type: '.$type);
                    log_message($debug_mode, 'streetAddress: '.$streetAddress);
                    log_message($debug_mode, 'locality: '.$locality);
                    log_message($debug_mode, 'postalCode: '.$postalCode);
                    log_message($debug_mode, 'country: '.$country);
                    log_message($debug_mode, 'countyCode: '.$countyCode);
                    log_message($debug_mode, 'municipalityCode: '.$municipalityCode);
                    log_message($debug_mode, 'realEstateDesignation: '.$realEstateDesignation);

                    my $phoneNumbers = $response_page_data->{phoneNumbers};
                    log_message($debug_mode, 'Geted phoneNumbers from api: '.Dumper($phoneNumbers));
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
                    log_message($debug_mode, 'phone: '.$phone);
                    log_message($debug_mode, 'mobile_phone: '.$mobile_phone);

                    my $cardnumber;
                    my $civicNo = $response_page_data->{civicNo};
                    if (defined $civicNo && ref $civicNo eq 'HASH') {
                            # $nationality = $civicNo->{nationality};
                            $cardnumber = $civicNo->{value}; 
                    }
                    log_message($debug_mode, 'cardnumber: '.$cardnumber);
                    log_message($debug_mode, 'civicNo: '.Dumper($civicNo));

                    my $externalIdentifier;
                    my $externalIdentifiers = $response_page_data->{externalIdentifiers};
                    if (defined $externalIdentifiers && ref $externalIdentifiers eq 'ARRAY') {
                        foreach my $selectedIdentifier (@$externalIdentifiers) {
                            $externalIdentifier = $selectedIdentifier->{value}; 
                        }
                    }
                    log_message($debug_mode, 'externalIdentifier: '.$externalIdentifier);

                    my $userid = $cardnumber;
                    log_message($debug_mode, 'userid: '.$userid);

                    if (!defined $email || $email eq "") { $email = undef; }
                    # warn "not_import, must be !=1, now is: ".$not_import;
                    log_message($debug_mode, 'addOrUpdateBorrower data from api to our Koha: '.($not_import ? 'no' : 'yes'));
                    $processed_count++; 

                    if ($not_import != 1) {
                        log_message($debug_mode, 'addOrUpdateBorrower, cardnumber: '.$cardnumber);
                        log_message($debug_mode, 'addOrUpdateBorrower, familyName: '.(defined $familyName ? $familyName : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, givenName: '.(defined $givenName ? $givenName : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, birthDate: '.(defined $birthDate ? $birthDate : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, email: '.(defined $email ? $email : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, sex: '.(defined $sex ? $sex : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, phone: '.(defined $phone ? $phone : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, mobile_phone: '.(defined $mobile_phone ? $mobile_phone : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, koha_categorycode: '.$koha_categorycode);
                        log_message($debug_mode, 'addOrUpdateBorrower, koha_branchcode: '.$koha_branchcode);
                        log_message($debug_mode, 'addOrUpdateBorrower, streetAddress: '.(defined $streetAddress ? $streetAddress : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, locality: '.(defined $locality ? $locality : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, postalCode: '.(defined $postalCode ? $postalCode : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, country: '.(defined $country ? $country : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, B_email: '.(defined $B_email ? $B_email : ''));
                        log_message($debug_mode, 'addOrUpdateBorrower, userid: '.$userid);
                        log_message($debug_mode, 'addOrUpdateBorrower, useridPlugin: '.$useridPlugin);
                        log_message($debug_mode, 'addOrUpdateBorrower, cardnumberPlugin: '.$cardnumberPlugin);
                        log_message($debug_mode, 'addOrUpdateBorrower, externalIdentifier: '.$externalIdentifier);
                        log_message($debug_mode, 'addOrUpdateBorrower, klass_displayName: '.(defined $klass_displayName ? $klass_displayName : ''));

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
                    log_message($debug_mode, 'ENDED DEBUGGING THE CURRENT USER');
                    log_message($debug_mode, ' ');
                } 
                # here
            }

            if ($j >0) {
                if ($debug_mode eq "No") { 
                    my $update_query = qq{
                        UPDATE $logs_table
                        SET is_processed = 1,
                            response = ?,
                            added_count = ?,
                            updated_count = ?,
                            processed_count = ?,
                            iteration_number = ?
                        WHERE data_hash = ?
                    };
                    my $update_response = "Added: $added_count, Updated: $updated_count, Total: $processed_count";
                    my $sth_update = $dbh->prepare($update_query);
                    
                    eval {
                        $sth_update->execute(
                            $update_response,
                            $added_count,
                            $updated_count,
                            $processed_count,
                            $iteration_number,
                            $data_hash
                        );
                        
                        # Логуємо завершення ітерації незалежно від debug_mode
                        my $message = "Iteration $iteration_number completed. Added: $added_count, Updated: $updated_count, Total processed: $processed_count";
                        log_message('Yes', $message);
                    };
                    
                    if ($@) {
                        warn "Error updating statistics: $@";
                    }

                    $sth_update->finish();
                } elsif ($debug_mode eq "Yes") {
                    my $update_query = qq{
                        UPDATE $logs_table
                        SET is_processed = 1,
                            added_count = ?,
                            updated_count = ?,
                            processed_count = ?,
                            iteration_number = ?
                        WHERE data_hash = ?
                    };
                    my $sth_update = $dbh->prepare($update_query);
                    unless ($sth_update->execute(
                            $added_count,
                            $updated_count,
                            $processed_count,
                            $iteration_number,
                            $data_hash
                        )) {
                        die "An error occurred while executing the request: " . $sth_update->errstr;
                    }
                    $sth_update->finish();
                }

            }
}

sub has_changes {
    my ($old_data, $new_values) = @_;
    
    my %fields_to_compare = (
        'dateofbirth' => $new_values->{birthdate},
        'email' => $new_values->{email},
        'sex' => $new_values->{sex},
        'phone' => $new_values->{phone},
        'mobile' => $new_values->{mobile_phone},
        'surname' => $new_values->{surname},
        'firstname' => $new_values->{firstname},
        'categorycode' => $new_values->{categorycode},
        'branchcode' => $new_values->{branchcode},
        'address' => $new_values->{streetAddress},
        'city' => $new_values->{locality},
        'zipcode' => $new_values->{postalCode},
        'country' => $new_values->{country},
        'B_email' => $new_values->{B_email},
        'userid' => $new_values->{newUserID},
        'cardnumber' => $new_values->{newCardnumber}
    );
    
    my @changed_fields;
    
    while (my ($field, $new_value) = each %fields_to_compare) {
        my $old_value = $old_data->{$field};
        $old_value = '' if !defined $old_value;
        $new_value = '' if !defined $new_value;
        
        if ($old_value ne $new_value) {
            push @changed_fields, {
                field => $field,
                old_value => $old_value,
                new_value => $new_value
            };
        }
    }
    
    return @changed_fields;
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

    # Determine the new userid based on plugin settings
    if ($useridPlugin eq "civicNo" || $useridPlugin eq "externalIdentifier") {
        $newUserID = ($useridPlugin eq "civicNo") ? $userid : $externalIdentifier;
    }

    # Determine the new cardnumber based on plugin settings
    if ($cardnumberPlugin eq "civicNo" || $cardnumberPlugin eq "externalIdentifier") {
        $newCardnumber = ($cardnumberPlugin eq "civicNo") ? $cardnumber : $externalIdentifier;
    }

    # Find all duplicates with comprehensive information about their usage
    my $find_duplicates_query = qq{
        SELECT b.*, 
            dateofbirth, email, sex, phone, mobile, 
            surname, firstname, categorycode, branchcode,
            address, city, zipcode, country, B_email,
            userid, cardnumber, opacnote,
            (SELECT COUNT(*) FROM issues i WHERE i.borrowernumber = b.borrowernumber) as issues_count,
            (SELECT COUNT(*) FROM old_issues oi WHERE oi.borrowernumber = b.borrowernumber) as old_issues_count,
            (SELECT COUNT(*) FROM reserves r WHERE r.borrowernumber = b.borrowernumber) as reserves_count,
            (SELECT COUNT(*) FROM borrower_attributes ba WHERE ba.borrowernumber = b.borrowernumber) as attributes_count,
            (SELECT COUNT(*) FROM accountlines al WHERE al.borrowernumber = b.borrowernumber) as accountlines_count,
            (SELECT COUNT(*) FROM message_queue mq WHERE mq.borrowernumber = b.borrowernumber) as messages_count
        FROM $borrowers_table b
        WHERE userid = ? OR cardnumber = ? OR userid = ? OR cardnumber = ?
        ORDER BY
            (issues_count + old_issues_count + reserves_count + attributes_count + accountlines_count + messages_count) DESC,
            updated_on DESC
    };
    my $borrowernumber;
    my $find_sth = $dbh->prepare($find_duplicates_query);
    $find_sth->execute($userid, $cardnumber, $newUserID, $newCardnumber);
    
    my @duplicates = ();
    while (my $row = $find_sth->fetchrow_hashref) {
        push @duplicates, $row;
    }
    
    my $existing_borrower;
    
    if (@duplicates > 1) {
        # Select the record with the most activity as the main record
        my $main_record = shift @duplicates;
        
        # Log detailed information about the main record
        log_message('Yes', sprintf(
            "Selected main record borrowernumber: %d (issues: %d, old_issues: %d, reserves: %d, attributes: %d, accountlines: %d, messages: %d)",
            $main_record->{borrowernumber},
            $main_record->{issues_count},
            $main_record->{old_issues_count},
            $main_record->{reserves_count},
            $main_record->{attributes_count},
            $main_record->{accountlines_count},
            $main_record->{messages_count}
        ));
        
        # Process and merge each duplicate record
        for my $duplicate (@duplicates) {
            # Log information about the duplicate being processed
            log_message('Yes', sprintf(
                "Processing duplicate borrowernumber: %d (issues: %d, old_issues: %d, reserves: %d, attributes: %d, accountlines: %d, messages: %d)",
                $duplicate->{borrowernumber},
                $duplicate->{issues_count},
                $duplicate->{old_issues_count},
                $duplicate->{reserves_count},
                $duplicate->{attributes_count},
                $duplicate->{accountlines_count},
                $duplicate->{messages_count}
            ));
            
            # List of tables that need to be updated with the new borrowernumber
            my @tables_to_update = (
                'issues',              # Current checkouts
                'old_issues',          # Checkout history
                'reserves',            # Current holds
                'old_reserves',        # Hold history
                'borrower_attributes', # Additional borrower information
                'accountlines',        # Financial transactions
                'message_queue',       # Messages
                'statistics',          # Usage statistics (very big db table)
                'borrower_files',      # Attached files
                'borrower_debarments', # Borrower restrictions
                'borrower_modifications', # Modification requests
                'club_enrollments',     # Club memberships
                'illrequests',          # Interlibrary loan requests
                'tags_all',             # User tags
                'reviews'               # User reviews
                # 'pending_offline_operations', # Offline operations (cardnumber?)
                # 'search_history',     # Search history (userid?)
                # 'suggestions',        # Purchase suggestions (suggestedby? managedby? acceptedby? rejectedby? lastmodificationby?)
                # 'patron_lists',       # List memberships (borrowernumber = owner)
                # 'virtualshelves',     # List ownerships (borrowernumber = owner)
            );
            
            # Update each table to point to the main record
            foreach my $table (@tables_to_update) {
                my $update_query = qq{
                    UPDATE $table 
                    SET borrowernumber = ? 
                    WHERE borrowernumber = ?
                };
                eval {
                    my $update_sth = $dbh->prepare($update_query);
                    $update_sth->execute($main_record->{borrowernumber}, $duplicate->{borrowernumber});
                    my $rows_affected = $update_sth->rows;
                    if ($rows_affected > 0) {
                        log_message('Yes', "Updated $rows_affected records in table $table");
                    }
                };
                if ($@) {
                    log_message('Yes', "Error updating $table: $@");
                }
            }
            
            # Delete the duplicate record only after successful data transfer
            # Instead of deleting, mark the duplicate record as archived
            eval {
                my $archive_query = qq{
                    UPDATE $borrowers_table 
                    SET
                        userid = CONCAT('ARCHIVED_', userid, '_', borrowernumber),
                        cardnumber = CONCAT('ARCHIVED_', cardnumber, '_', borrowernumber),
                        flags = -1,  # Special flag to mark as archived
                        dateexpiry = NOW(),  # Expire the card
                        gonenoaddress = 1,   # Mark as invalid address
                        lost = 1,            # Mark as lost card
                        debarredcomment = CONCAT('Updated by SS12000: plugin ', '$version_info', '. Merged with borrowernumber: ', ?, ' at ', NOW()),
                        opacnote = CONCAT('Updated by SS12000: plugin ', '$version_info', '. Merged with borrowernumber: ', ?, ' at ', NOW())
                    WHERE borrowernumber = ?
                };
                my $archive_sth = $dbh->prepare($archive_query);
                $archive_sth->execute(
                    $main_record->{borrowernumber}, 
                    $main_record->{borrowernumber}, 
                    $duplicate->{borrowernumber}
                );
                log_message('Yes', "Archived duplicate borrowernumber: " . $duplicate->{borrowernumber} . 
                                " (merged with: " . $main_record->{borrowernumber} . ")");
            };
            if ($@) {
                log_message('Yes', "Error archiving duplicate: $@");
            }
        }
        
        # Use the main record for further updates
        $existing_borrower = $main_record;
        
    } elsif (@duplicates == 1) {
        # If only one record exists, use it
        $existing_borrower = $duplicates[0];
    }


    if ($existing_borrower) {
        my @changes = has_changes($existing_borrower, {
            birthdate => $birthdate,
            email => $email,
            sex => $sex,
            phone => $phone,
            mobile_phone => $mobile_phone,
            surname => $surname,
            firstname => $firstname,
            categorycode => $categorycode,
            branchcode => $branchcode,
            streetAddress => $streetAddress,
            locality => $locality,
            postalCode => $postalCode,
            country => $country,
            B_email => $B_email,
            newUserID => $newUserID,
            newCardnumber => $newCardnumber
        });
        
        if (@changes) {
            foreach my $change (@changes) {
                log_message('Yes', sprintf(
                    "Field %s changed from '%s' to '%s'",
                    $change->{field},
                    $change->{old_value},
                    $change->{new_value}
                ));
            }
            
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
                    cardnumber = ?,
                    opacnote = CASE
                        WHEN opacnote IS NULL OR opacnote = ''
                            THEN CONCAT('Updated by SS12000: plugin ', '$version_info', ' at ', NOW(), ' Fields changed: ', ?)
                        WHEN opacnote LIKE '%Updated by SS12000: plugin%'
                            THEN CONCAT(
                                SUBSTRING_INDEX(opacnote, 'Updated by SS12000: plugin', 1),
                                'Updated by SS12000: plugin ', '$version_info', ' at ', NOW(), ' Fields changed: ', ?
                            )
                        WHEN opacnote LIKE '%Added by SS12000: plugin%'
                            THEN CONCAT(
                                REPLACE(
                                    SUBSTRING_INDEX(opacnote, 'Added by SS12000: plugin', 1),
                                    'Added by', 'Updated by'
                                ),
                                'Updated by SS12000: plugin ', '$version_info', ' at ', NOW(), ' Fields changed: ', ?
                            )
                        ELSE CONCAT(
                            opacnote,
                            '\nUpdated by SS12000: plugin ', '$version_info', ' at ', NOW(), ' Fields changed: ', ?
                        )
                    END,
                    updated_on = NOW()
                WHERE borrowernumber = ?
            };
            
            my $update_sth = $dbh->prepare($update_query);
            eval {
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
                    join(', ', map { $_->{field} } @changes),
                    join(', ', map { $_->{field} } @changes),
                    join(', ', map { $_->{field} } @changes),
                    join(', ', map { $_->{field} } @changes),
                    $existing_borrower->{'borrowernumber'}
                );
            };
            if ($@) {
                log_message('Yes', "Error updating user: $@");
            } else {
                $updated_count++;
                $borrowernumber = $existing_borrower->{'borrowernumber'};
                log_message('Yes', "Successfully updated borrower: " . $existing_borrower->{'borrowernumber'});
            }
        } else {
            log_message('Yes', "No changes detected for borrower: " . $existing_borrower->{'borrowernumber'});
        }
    } else {
        # Insert a new borrower record if no existing record was found
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
                userid,
                dateenrolled,
                dateexpiry,
                updated_on,
                opacnote
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                CURDATE(), 
                DATE_ADD(CURDATE(), INTERVAL 1 YEAR), 
                NOW(),
                CONCAT('Added by SS12000: plugin ', '$version_info', ' at ', NOW())
            )
        };
        my $insert_sth = $dbh->prepare($insert_query);
        eval {
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
        };
        if ($@) {
            log_message('Yes', "Error inserting user: $@");
        } else {
            $added_count++;
            $borrowernumber = $dbh->last_insert_id(undef, undef, $borrowers_table, undef);
        }
    }

    # Process class attribute if provided
    if ($borrowernumber && $klass_displayName) {
        my $code = 'CL';
        my $attribute = $klass_displayName;

        # Check if entry exists in borrower_attribute_types
        my $check_types_query = qq{
            SELECT 1 FROM borrower_attribute_types WHERE code = ?
        };
        my $check_types_sth = $dbh->prepare($check_types_query);
        $check_types_sth->execute($code);
        my ($exists) = $check_types_sth->fetchrow_array();

        # Create attribute type if it doesn't exist
        unless ($exists) {
            my $insert_types_query = qq{
                INSERT INTO borrower_attribute_types (
                    code, 
                    description, 
                    repeatable, 
                    unique_id, 
                    opac_display, 
                    opac_editable, 
                    staff_searchable, 
                    authorised_value_category, 
                    display_checkout, 
                    category_code, 
                    class, 
                    keep_for_pseudonymization, 
                    mandatory
                )
                VALUES (
                    'CL', 'Klass', 0, 0, 0, 0, 0, '', 0, NULL, '', 0, 0
                )
            };
            my $insert_types_sth = $dbh->prepare($insert_types_query);
            eval {
                $insert_types_sth->execute();
            };
            if ($@) {
                log_message('Yes', "Error inserting into borrower_attribute_types: $@");
            }
        }

        # Check if attribute record exists
        my $check_query = qq{
            SELECT attribute FROM borrower_attributes 
            WHERE borrowernumber = ? AND code = ?
        };
        my $check_sth = $dbh->prepare($check_query);
        $check_sth->execute($borrowernumber, $code);
        my $existing_attribute = $check_sth->fetchrow_array();

        if (defined $existing_attribute) {
            # Update existing attribute if value is different
            if ($existing_attribute ne $attribute) {
                my $update_query = qq{
                    UPDATE borrower_attributes
                    SET attribute = ?
                    WHERE borrowernumber = ? AND code = ?
                };
                my $update_sth = $dbh->prepare($update_query);
                eval {
                    $update_sth->execute($attribute, $borrowernumber, $code);
                };
                if ($@) {
                    log_message('Yes', "Error updating borrower_attributes: $@");
                }
            }
        } else {
            # Insert new attribute
            my $insert_query = qq{
                INSERT INTO borrower_attributes (borrowernumber, code, attribute)
                VALUES (?, ?, ?)
            };
            my $insert_sth = $dbh->prepare($insert_query);
            eval {
                $insert_sth->execute($borrowernumber, $code, $attribute);
            };
            if ($@) {
                log_message('Yes', "Error inserting into borrower_attributes: $@");
            }
        }
    }

    # return $borrowernumber;
}


1;
