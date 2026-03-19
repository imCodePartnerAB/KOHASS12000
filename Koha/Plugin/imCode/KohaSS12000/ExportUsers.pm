# Copyright (C) 2024-2026 imCode, https://www.imcode.com, <info@imcode.com>
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

use File::Basename;

use Fcntl qw(:flock SEEK_END O_WRONLY O_CREAT O_EXCL);
use Time::Piece;

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

our $VERSION = "1.91";

our $metadata = {
    name            => getTranslation('Export Users from SS12000'),
    author          => 'imCode.com',
    date_authored   => '2023-08-08',
    date_updated    => '2026-03-12',
    minimum_version => '20.05',
    maximum_version => undef,
    version         => $VERSION,
    description     => getTranslation('This plugin implements export users from SS12000')
};

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

    if ($debug_mode eq 'Yes') {
        my $my_log_file = get_log_file();

        unless (-e $my_log_file) {
            eval {
                open my $fh, '>', $my_log_file 
                    or die "Cannot create $my_log_file: $!";
                flock($fh, LOCK_EX) 
                    or die "Cannot lock $my_log_file: $!";
                print $fh "";
                close $fh;
            };
            if ($@) {
                warn "Error creating log file: $@";
                return;
            }
        }

        eval {
            open my $fh, '>>', $my_log_file 
                or die "Cannot open $my_log_file: $!";
            flock($fh, LOCK_EX) 
                or die "Cannot lock $my_log_file: $!";

            my $timestamp = strftime "%Y-%m-%d %H:%M:%S", localtime;
            print $fh "$timestamp - $message\n";
            close $fh;
        };
        if ($@) {
            warn "Error writing to log file: $@";
            return;
        }
    }
}

sub check_session {
    my ($self, $is_cron) = @_;
    
    return 1 if $is_cron; # Skipping the check for cron jobs
    
    my $cgi = $self->{'cgi'};
    my $session_id = $cgi->cookie('CGISESSID');
    
    unless ($session_id) {
        log_message("Yes", "Session ID not found");
        return;
    }
    
    my $session = C4::Auth::get_session($session_id);
    unless ($session) {
        log_message("Yes", "Invalid session");
        return;
    }
    
    my $userid = $session->param('id');
    unless ($userid) {
        log_message("Yes", "User not logged in");
        return;
    }
    
    my $tokenizer = Koha::Token->new;
    my $csrf_token = $tokenizer->generate_csrf({ session_id => $session_id });
    
    return {
        session_id => $session_id,
        csrf_token => $csrf_token,
        userid     => $userid
    };
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
    qq{INSERT INTO imcode_config (name,value) VALUES ('ignore_cancelled_flag','No');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('use_default_for_unmapped','No');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('dateexpiry_fallback','keep');},
    qq{INSERT INTO imcode_config (name,value) VALUES ('dateexpiry_months','12');},
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

    my $trigger_sql = qq{
    CREATE TRIGGER log_user_changes
    AFTER UPDATE ON borrowers
    FOR EACH ROW
    BEGIN
        DECLARE change_description TEXT DEFAULT '';

        IF NOT (NEW.dateofbirth <=> OLD.dateofbirth) THEN
            SET change_description = CONCAT(change_description, 'field dateofbirth changed from "', OLD.dateofbirth, '" to "', NEW.dateofbirth, '"; ');
        END IF;

        IF NOT (NEW.phone <=> OLD.phone) THEN
            SET change_description = CONCAT(change_description, 'field phone changed from "', OLD.phone, '" to "', NEW.phone, '"; ');
        END IF;

        IF NOT (NEW.mobile <=> OLD.mobile) THEN
            SET change_description = CONCAT(change_description, 'field mobile changed from "', OLD.mobile, '" to "', NEW.mobile, '"; ');
        END IF;

        IF NOT (NEW.surname <=> OLD.surname) THEN
            SET change_description = CONCAT(change_description, 'field surname changed from "', OLD.surname, '" to "', NEW.surname, '"; ');
        END IF;

        IF NOT (NEW.firstname <=> OLD.firstname) THEN
            SET change_description = CONCAT(change_description, 'field firstname changed from "', OLD.firstname, '" to "', NEW.firstname, '"; ');
        END IF;

        IF NOT (NEW.categorycode <=> OLD.categorycode) THEN
            SET change_description = CONCAT(change_description, 'field categorycode changed from "', OLD.categorycode, '" to "', NEW.categorycode, '"; ');
        END IF;

        IF NOT (NEW.branchcode <=> OLD.branchcode) THEN
            SET change_description = CONCAT(change_description, 'field branchcode changed from "', OLD.branchcode, '" to "', NEW.branchcode, '"; ');
        END IF;

        IF NOT (NEW.address <=> OLD.address) THEN
            SET change_description = CONCAT(change_description, 'field address changed from "', OLD.address, '" to "', NEW.address, '"; ');
        END IF;

        IF NOT (NEW.city <=> OLD.city) THEN
            SET change_description = CONCAT(change_description, 'field city changed from "', OLD.city, '" to "', NEW.city, '"; ');
        END IF;

        IF NOT (NEW.zipcode <=> OLD.zipcode) THEN
            SET change_description = CONCAT(change_description, 'field zipcode changed from "', OLD.zipcode, '" to "', NEW.zipcode, '"; ');
        END IF;

        IF NOT (NEW.country <=> OLD.country) THEN
            SET change_description = CONCAT(change_description, 'field country changed from "', OLD.country, '" to "', NEW.country, '"; ');
        END IF;

        IF NOT (NEW.B_email <=> OLD.B_email) THEN
            SET change_description = CONCAT(change_description, 'field B_email changed from "', OLD.B_email, '" to "', NEW.B_email, '"; ');
        END IF;

        IF NOT (NEW.userid <=> OLD.userid) THEN
            SET change_description = CONCAT(change_description, 'field userid changed from "', OLD.userid, '" to "', NEW.userid, '"; ');
        END IF;

        IF NOT (NEW.cardnumber <=> OLD.cardnumber) THEN
            SET change_description = CONCAT(change_description, 'field cardnumber changed from "', OLD.cardnumber, '" to "', NEW.cardnumber, '"; ');
        END IF;

        IF NOT (NEW.sex <=> OLD.sex) THEN
            SET change_description = CONCAT(change_description, 'field sex changed from "', OLD.sex, '" to "', NEW.sex, '"; ');
        END IF;

        IF NOT (NEW.email <=> OLD.email) THEN
            SET change_description = CONCAT(change_description, 'field email changed from "', OLD.email, '" to "', NEW.email, '"; ');
        END IF;

        IF NOT (NEW.dateexpiry <=> OLD.dateexpiry) THEN
            SET change_description = CONCAT(change_description, 'field dateexpiry changed from "', OLD.dateexpiry, '" to "', NEW.dateexpiry, '"; ');
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

    our $VERSION = $VERSION || '1.0';
    log_message("Yes", "Starting upgrade process for plugin version $VERSION");

    my $installed_version = $self->retrieve_data('installed_version') || '0';
    my $is_new_install = ($installed_version eq '0');

    log_message("Yes", "Is new install: " . ($is_new_install ? "Yes" : "No"));
    log_message("Yes", "Installed version: $installed_version");

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

    my $create_trigger_sql = q{
    CREATE TRIGGER log_user_changes
    AFTER UPDATE ON borrowers
    FOR EACH ROW
    BEGIN
        DECLARE change_description TEXT DEFAULT '';

        IF NOT (NEW.dateofbirth <=> OLD.dateofbirth) THEN
            SET change_description = CONCAT(change_description, 'field dateofbirth changed from "', OLD.dateofbirth, '" to "', NEW.dateofbirth, '"; ');
        END IF;

        IF NOT (NEW.phone <=> OLD.phone) THEN
            SET change_description = CONCAT(change_description, 'field phone changed from "', OLD.phone, '" to "', NEW.phone, '"; ');
        END IF;

        IF NOT (NEW.mobile <=> OLD.mobile) THEN
            SET change_description = CONCAT(change_description, 'field mobile changed from "', OLD.mobile, '" to "', NEW.mobile, '"; ');
        END IF;

        IF NOT (NEW.surname <=> OLD.surname) THEN
            SET change_description = CONCAT(change_description, 'field surname changed from "', OLD.surname, '" to "', NEW.surname, '"; ');
        END IF;

        IF NOT (NEW.firstname <=> OLD.firstname) THEN
            SET change_description = CONCAT(change_description, 'field firstname changed from "', OLD.firstname, '" to "', NEW.firstname, '"; ');
        END IF;

        IF NOT (NEW.categorycode <=> OLD.categorycode) THEN
            SET change_description = CONCAT(change_description, 'field categorycode changed from "', OLD.categorycode, '" to "', NEW.categorycode, '"; ');
        END IF;

        IF NOT (NEW.branchcode <=> OLD.branchcode) THEN
            SET change_description = CONCAT(change_description, 'field branchcode changed from "', OLD.branchcode, '" to "', NEW.branchcode, '"; ');
        END IF;

        IF NOT (NEW.address <=> OLD.address) THEN
            SET change_description = CONCAT(change_description, 'field address changed from "', OLD.address, '" to "', NEW.address, '"; ');
        END IF;

        IF NOT (NEW.city <=> OLD.city) THEN
            SET change_description = CONCAT(change_description, 'field city changed from "', OLD.city, '" to "', NEW.city, '"; ');
        END IF;

        IF NOT (NEW.zipcode <=> OLD.zipcode) THEN
            SET change_description = CONCAT(change_description, 'field zipcode changed from "', OLD.zipcode, '" to "', NEW.zipcode, '"; ');
        END IF;

        IF NOT (NEW.country <=> OLD.country) THEN
            SET change_description = CONCAT(change_description, 'field country changed from "', OLD.country, '" to "', NEW.country, '"; ');
        END IF;

        IF NOT (NEW.B_email <=> OLD.B_email) THEN
            SET change_description = CONCAT(change_description, 'field B_email changed from "', OLD.B_email, '" to "', NEW.B_email, '"; ');
        END IF;

        IF NOT (NEW.userid <=> OLD.userid) THEN
            SET change_description = CONCAT(change_description, 'field userid changed from "', OLD.userid, '" to "', NEW.userid, '"; ');
        END IF;

        IF NOT (NEW.cardnumber <=> OLD.cardnumber) THEN
            SET change_description = CONCAT(change_description, 'field cardnumber changed from "', OLD.cardnumber, '" to "', NEW.cardnumber, '"; ');
        END IF;

        IF NOT (NEW.sex <=> OLD.sex) THEN
            SET change_description = CONCAT(change_description, 'field sex changed from "', OLD.sex, '" to "', NEW.sex, '"; ');
        END IF;

        IF NOT (NEW.email <=> OLD.email) THEN
            SET change_description = CONCAT(change_description, 'field email changed from "', OLD.email, '" to "', NEW.email, '"; ');
        END IF;

        IF NOT (NEW.dateexpiry <=> OLD.dateexpiry) THEN
            SET change_description = CONCAT(change_description, 'field dateexpiry changed from "', OLD.dateexpiry, '" to "', NEW.dateexpiry, '"; ');
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

    if ($success) {
        if ($is_new_install) {
            log_message("Yes", "Plugin installed successfully (version $VERSION)");
        } else {
            log_message("Yes", "Plugin upgraded successfully to version $VERSION");
        }
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
    }
}

sub recreate_trigger {
    my ($dbh) = @_;

    eval {
        $dbh->do(q{DROP TRIGGER IF EXISTS log_user_changes});
    };
    if ($@) {
        log_message("Yes", "recreate_trigger: error dropping trigger: $@");
        return 0;
    }

    my $create_trigger_sql = q{
    CREATE TRIGGER log_user_changes
    AFTER UPDATE ON borrowers
    FOR EACH ROW
    BEGIN
        DECLARE change_description TEXT DEFAULT '';

        IF NOT (NEW.dateofbirth <=> OLD.dateofbirth) THEN
            SET change_description = CONCAT(change_description, 'field dateofbirth changed from "', OLD.dateofbirth, '" to "', NEW.dateofbirth, '"; ');
        END IF;

        IF NOT (NEW.phone <=> OLD.phone) THEN
            SET change_description = CONCAT(change_description, 'field phone changed from "', OLD.phone, '" to "', NEW.phone, '"; ');
        END IF;

        IF NOT (NEW.mobile <=> OLD.mobile) THEN
            SET change_description = CONCAT(change_description, 'field mobile changed from "', OLD.mobile, '" to "', NEW.mobile, '"; ');
        END IF;

        IF NOT (NEW.surname <=> OLD.surname) THEN
            SET change_description = CONCAT(change_description, 'field surname changed from "', OLD.surname, '" to "', NEW.surname, '"; ');
        END IF;

        IF NOT (NEW.firstname <=> OLD.firstname) THEN
            SET change_description = CONCAT(change_description, 'field firstname changed from "', OLD.firstname, '" to "', NEW.firstname, '"; ');
        END IF;

        IF NOT (NEW.categorycode <=> OLD.categorycode) THEN
            SET change_description = CONCAT(change_description, 'field categorycode changed from "', OLD.categorycode, '" to "', NEW.categorycode, '"; ');
        END IF;

        IF NOT (NEW.branchcode <=> OLD.branchcode) THEN
            SET change_description = CONCAT(change_description, 'field branchcode changed from "', OLD.branchcode, '" to "', NEW.branchcode, '"; ');
        END IF;

        IF NOT (NEW.address <=> OLD.address) THEN
            SET change_description = CONCAT(change_description, 'field address changed from "', OLD.address, '" to "', NEW.address, '"; ');
        END IF;

        IF NOT (NEW.city <=> OLD.city) THEN
            SET change_description = CONCAT(change_description, 'field city changed from "', OLD.city, '" to "', NEW.city, '"; ');
        END IF;

        IF NOT (NEW.zipcode <=> OLD.zipcode) THEN
            SET change_description = CONCAT(change_description, 'field zipcode changed from "', OLD.zipcode, '" to "', NEW.zipcode, '"; ');
        END IF;

        IF NOT (NEW.country <=> OLD.country) THEN
            SET change_description = CONCAT(change_description, 'field country changed from "', OLD.country, '" to "', NEW.country, '"; ');
        END IF;

        IF NOT (NEW.B_email <=> OLD.B_email) THEN
            SET change_description = CONCAT(change_description, 'field B_email changed from "', OLD.B_email, '" to "', NEW.B_email, '"; ');
        END IF;

        IF NOT (NEW.userid <=> OLD.userid) THEN
            SET change_description = CONCAT(change_description, 'field userid changed from "', OLD.userid, '" to "', NEW.userid, '"; ');
        END IF;

        IF NOT (NEW.cardnumber <=> OLD.cardnumber) THEN
            SET change_description = CONCAT(change_description, 'field cardnumber changed from "', OLD.cardnumber, '" to "', NEW.cardnumber, '"; ');
        END IF;

        IF NOT (NEW.sex <=> OLD.sex) THEN
            SET change_description = CONCAT(change_description, 'field sex changed from "', OLD.sex, '" to "', NEW.sex, '"; ');
        END IF;

        IF NOT (NEW.email <=> OLD.email) THEN
            SET change_description = CONCAT(change_description, 'field email changed from "', OLD.email, '" to "', NEW.email, '"; ');
        END IF;

        IF NOT (NEW.dateexpiry <=> OLD.dateexpiry) THEN
            SET change_description = CONCAT(change_description, 'field dateexpiry changed from "', OLD.dateexpiry, '" to "', NEW.dateexpiry, '"; ');
        END IF;

        IF change_description != '' THEN
            INSERT INTO imcode_data_change_log (table_name, record_id, action, change_description)
            VALUES ('borrowers', NEW.borrowernumber, 'update', TRIM(TRAILING '; ' FROM change_description));
        END IF;
    END
    };

    eval {
        $dbh->do($create_trigger_sql);
    };
    if ($@) {
        log_message("Yes", "recreate_trigger: error creating trigger: $@");
        return 0;
    }

    log_message("Yes", "recreate_trigger: trigger log_user_changes successfully recreated");
    return 1;
}

sub configure {
    my ($self, $args) = @_;
    my $cgi = $self->{'cgi'};

    my $session_data = $self->check_session(0);
    unless ($session_data) {
        print $cgi->redirect("/cgi-bin/koha/mainpage.pl");
        return;
    }

    my $dbh = C4::Context->dbh;
    my $op = $cgi->param('op') || '';

    my ($trigger_version) = $dbh->selectrow_array(
        "SELECT value FROM $config_table WHERE name = 'trigger_version'"
    );
    if (!$trigger_version || $trigger_version ne $VERSION) {
        recreate_trigger($dbh);
        $dbh->do(
            "INSERT INTO $config_table (name, value) VALUES ('trigger_version', ?)
             ON DUPLICATE KEY UPDATE value = ?",
            undef, $VERSION, $VERSION
        );
        log_message("Yes", "Trigger recreated for plugin version $VERSION");
    }

    insertConfigValue($dbh, 'excluding_dutyRole_empty', 'No');
    insertConfigValue($dbh, 'excluding_enrolments_empty', 'No');
    insertConfigValue($dbh, 'ignore_cancelled_flag', 'No');
    insertConfigValue($dbh, 'use_default_for_unmapped', 'No');
    insertConfigValue($dbh, 'archived_limit', '0');
    insertConfigValue($dbh, 'dateexpiry_fallback', 'keep');
    insertConfigValue($dbh, 'dateexpiry_months', '12');

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

    $template->param(success => $self->{'success'}) if $self->{'success'};
    $template->param(error => $self->{'error'}) if $self->{'error'};
    my $count_log_query = "SELECT COUNT(*) FROM imcode_logs WHERE DATE(created_at) = CURDATE() AND is_processed = 1";
    my ($log_count) = $dbh->selectrow_array($count_log_query);
    $template->param(log_count => $log_count);

    if ($op eq 'cud-save-config') {
        my $client_id     = $cgi->param('client_id');
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
        my $ignore_cancelled_flag = $cgi->param('ignore_cancelled_flag');
        my $use_default_for_unmapped = $cgi->param('use_default_for_unmapped');
        my $dateexpiry_fallback  = $cgi->param('dateexpiry_fallback') || 'keep';
        my $dateexpiry_months    = int($cgi->param('dateexpiry_months') || 12);

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
                next if $category_id == 0;
                $dbh->do($update_category_query, undef, $category_id);
            }
        }

        if (@category_mapping_del) {
            my $delete_category_query = qq{
                DELETE 
                FROM $categories_mapping_table 
                WHERE id = ? 
            };

            foreach my $category_id (@category_mapping_del) {
                next if $category_id == 0;
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
                next if $branch_id == 0;
                $dbh->do($delete_branch_query, undef, $branch_id);
            }
        }

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
                        log_message("Yes", "Updated records in $logs_table for current date.");                        
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
                WHEN name = 'ignore_cancelled_flag' THEN ?
                WHEN name = 'use_default_for_unmapped' THEN ?
                WHEN name = 'dateexpiry_fallback' THEN ?
                WHEN name = 'dateexpiry_months' THEN ?
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
                'excluding_enrolments_empty',
                'ignore_cancelled_flag',
                'use_default_for_unmapped',
                'dateexpiry_fallback',
                'dateexpiry_months'
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
                $excluding_enrolments_empty,
                $ignore_cancelled_flag,
                $use_default_for_unmapped,
                $dateexpiry_fallback,
                $dateexpiry_months
                );
            $template->param(success => 'success');
        };

        if ($@) {
            log_message("Yes","Error updating configuration: $@");
            warn "Error updating configuration: $@";
        }
    }
    elsif ($op eq 'cud-clearlog-config') {
        my $clean_query = qq{
                UPDATE imcode_logs 
                SET is_processed = 0, updated_count = 0, added_count = 0, 
                    page_token_next = 'RESET'
                WHERE DATE(created_at) = CURDATE()
            };
        eval { $dbh->do($clean_query) };

        if ($@) {
            log_message("Yes", "Error while run clean_query: $@");
            warn "Error while run clean_query: $@";
        }
        log_message("Yes", "Updated records in $logs_table for current date. Configuration change");
        $template->param(log_count => 0);

        my $status_file = get_status_file();
        unlink $status_file if -e $status_file;
        unlink get_lock_file() if -e get_lock_file();
    }

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
        ignore_cancelled_flag => $config_data->{ignore_cancelled_flag} || 'No',
        use_default_for_unmapped => $config_data->{use_default_for_unmapped} || 'No',
        dateexpiry_fallback  => $config_data->{dateexpiry_fallback} || 'keep',
        dateexpiry_months    => int($config_data->{dateexpiry_months} || 12),
        csrf_token => $session_data->{csrf_token},
        );

    print $cgi->header(-type => 'text/html', -charset => 'utf-8');
    print $template->output();
}


sub xor_encrypt {
    # Simple symmetric XOR encryption — avoids external crypto dependencies
    # and ensures the secret is never stored in plaintext in the database
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
    
    my $session_data = $self->check_session(0);
    unless ($session_data) {
        print $cgi->redirect("/cgi-bin/koha/mainpage.pl");
        return;
    }

    my $dbh = C4::Context->dbh;
    my $template = $self->get_template( { file => 'tool.tt' } );
    my $op          = $cgi->param('op') || q{};

    my $page      = $cgi->param('page') || 1;
    my $per_page  = 20;
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

    if ($op eq 'cud-clearlog') {
        my $clean_query = qq{
                UPDATE imcode_logs 
                SET is_processed = 0, updated_count = 0, added_count = 0, 
                    page_token_next = 'RESET'
                WHERE DATE(created_at) = CURDATE()
            };
        eval { $dbh->do($clean_query) };

        my $status_file = get_status_file();
        unlink $status_file if -e $status_file;
        unlink get_lock_file() if -e get_lock_file();

        if ($@) {
            log_message("Yes", "Error while run clean_query: $@");
            warn "Error while run clean_query: $@";
        }
        log_message("Yes", "Updated records in $logs_table for current date. Configuration change");
        $template->param(log_count => 0);
    }    

    if ($op eq 'cud-get-status') {
        print $cgi->header('application/json');
        my $status = $self->read_status();
        
        if ($status->{pid}) {
            unless (kill(0, $status->{pid})) {
                # PID is gone — check DB to distinguish normal completion from a crash.
                # When the child process finishes all pages, it updates status to
                # 'completed' before exit. If status is still 'running' here, the
                # process died unexpectedly only if no completed records exist today.
                my ($completed_today) = $dbh->selectrow_array(qq{
                    SELECT COUNT(*)
                    FROM $logs_table
                    WHERE DATE(created_at) = CURDATE()
                    AND data_endpoint = 'persons'
                    AND page_token_next IS NULL
                    AND is_processed = 1
                });

                if ($completed_today > 0 || $status->{status} eq 'completed' || $status->{status} eq 'page_completed') {
                    # Process finished normally — status file just not yet cleaned up
                    $status->{status} = 'completed' unless $status->{status} eq 'page_completed';
                    $status->{locked} = 0;
                    $status->{pid}    = undef;
                } elsif ($status->{status} eq 'running') {
                    # Genuinely crashed — no completed records and status still running
                    $status->{status} = 'error';
                    $status->{locked} = 0;
                    $status->{pid}    = undef;
                    push @{$status->{messages}}, {
                        time  => time(),
                        text  => "Process died unexpectedly",
                        error => 1
                    };
                }
                $self->save_status($status);
            }
        }
        
        # Clean up stale status files older than 1 day
        if ($status->{status} ne 'running' && 
            $status->{last_update} && 
            time() - $status->{last_update} > 86400) {
            
            $status = {
                locked      => 0,
                pid         => undef,
                started_at  => undef,
                status      => 'idle',
                last_update => time(),
                messages    => []
            };
            $self->save_status($status);
        }
        
        print JSON::encode_json($status);
        exit;
    }
    
    elsif ($op eq 'cud-force-unlock') {
        print $cgi->header('application/json');
        
        my $session_data = $self->check_session(0);
        unless ($session_data && $session_data->{permissions}->{plugins}) {
            print JSON::encode_json({
                status  => 'error',
                message => 'Permission denied'
            });
            exit;
        }
        
        my $status = $self->read_status();
        
        if ($status->{started_at} && time() - $status->{started_at} < 300) {
            print JSON::encode_json({
                status  => 'error',
                message => 'Process is still young, wait at least 5 minutes before forcing unlock'
            });
            exit;
        }
        
        if ($status->{pid} && kill(0, $status->{pid})) {
            kill 'TERM', $status->{pid};
            sleep 2;
            
            if (kill(0, $status->{pid})) {
                kill 'KILL', $status->{pid};
            }
        }
        
        $status = {
            locked      => 0,
            pid         => undef,
            started_at  => undef,
            status      => 'idle',
            last_update => time(),
            messages    => [
                {
                    time  => time(),
                    text  => "Process forcefully unlocked by user",
                    error => 0
                }
            ]
        };
        
        $self->save_status($status);
        $self->release_lock();
        
        log_message("Yes", "Export process forcefully unlocked by user");
        
        print JSON::encode_json({
            status  => 'success',
            message => 'Process unlocked successfully'
        });
        exit;
    }

    if ($op eq 'cud-get-log') {
        print $cgi->header('application/json');
        print JSON::encode_json($self->get_log_contents());
        exit;
    }    

    if ($op eq 'cud-run-export') {
        if ($self->is_process_running()) {
            $template->param(
                process_started => {
                    status  => 'already_running',
                    message => 'Export process is already running'
                }
            );
        }
        elsif ($cgi->param('start_export')) {
            my $result = $self->start_export_process();
            $template->param(
                process_started => $result
            );
        }
        
        $template->param(
            run_export => 1,
        );
    }

    if ($op eq 'cud-show-updates') {
        my @updates;

        my $search = $cgi->param('search') || q{};
        $search = substr($search, 0, 50);
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
        $template->param(
            updates     => \@updates,
            prev_page   => $prev_page,
            next_page   => $next_page,
            total_pages => $total_pages,
            current_page => $page,
            search      => $search,
        );
    }

    if ($op eq 'cud-show-logs') {
        my @logs;

        my $query = "SELECT * FROM $logs_table ORDER BY created_at DESC LIMIT $per_page OFFSET $start_row";
        
        eval {
            my $sth = $dbh->prepare($query);
            $sth->execute();

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

        $template->param(
            logs        => \@logs,
            debug_mode  => $debug_mode || '',
            prev_page   => $prev_page,
            next_page   => $next_page,
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

        $template->param(
            stats => \@stats
        );
    }

    my $count_log_query = "SELECT COUNT(*) FROM imcode_logs WHERE DATE(created_at) = CURDATE() AND is_processed = 1";
    my ($log_count) = $dbh->selectrow_array($count_log_query);
    $template->param(log_count => $log_count);

    $template->param(
            language   => C4::Languages::getlanguage($cgi) || 'en',
            mbf_path   => abs_path( $self->mbf_path('translations') ),
            csrf_token => $session_data->{csrf_token},
    );

    print $cgi->header( -type => 'text/html', -charset => 'utf-8' );
    print $template->output();
}

sub cronjob {
    my ($self, $data_endpoint, $is_web) = @_;
    
    log_message("Yes", "Starting cronjob with plugin version: $VERSION ($version_info)");

    # Acquire shared lock — prevents simultaneous cron + HTTP execution.
    # Returns undef on success, or hashref describing the existing lock owner.
    my $existing_lock = $self->acquire_lock('cron');
    if ($existing_lock) {
        my $msg = "Another process already running (source: $existing_lock->{source}, pid: $existing_lock->{pid}), skipping";
        log_message("Yes", $msg);
        if ($is_web) {
            return "ProcessAlreadyRunning";
        } else {
            print "ProcessAlreadyRunning\n";
            return 0;
        }
    }

    my $result = eval {
        $self->_cronjob_inner($data_endpoint, $is_web);
    };
    my $err = $@;

    $self->release_lock();

    if ($err) {
        if ($err =~ /EndLastPageFromAPI/) {
            return "EndLastPageFromAPI" if $is_web;
            print "EndLastPageFromAPI\n";
            return 0;
        }
        die $err;
    }

    return $result;
}

# Internal implementation — called only from cronjob() after lock is acquired
sub _cronjob_inner {
    my ($self, $data_endpoint, $is_web) = @_;

    my $dbh = C4::Context->dbh;

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
    
    my $total_orgs_query = qq{
        SELECT COUNT(DISTINCT organisationCode) 
        FROM $branches_mapping_table 
        WHERE organisationCode IS NOT NULL 
        AND organisationCode != ''
    };
    
    my ($total_orgs) = $dbh->selectrow_array($total_orgs_query);
    
    if ($total_orgs == 0) {
        $total_orgs = 1;
    }
    
    if (scalar(@$completed_orgs) >= $total_orgs) {
        log_message("Yes", "Full processing cycle already completed today for all organizations");
        return "EndLastPageFromAPI" if $is_web;
        print "EndLastPageFromAPI\n";
        return 0;
    }

    my $check_mapping_exists = qq{
        SELECT COUNT(*) FROM $branches_mapping_table
    };
    my ($mapping_exists) = $dbh->selectrow_array($check_mapping_exists);
    
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
    my $today = sprintf("%04d-%02d-%02d", $year + 1900, $mon + 1, $mday);
    
    my $config_data = $self->get_config_data();
    my $logs_limit = int($config_data->{logs_limit}) || 3;
    
    my $cleanup_query = qq{
        DELETE FROM $logs_table
        WHERE created_at < DATE_SUB(CURDATE(), INTERVAL ? DAY)
    };
    $dbh->do($cleanup_query, undef, $logs_limit);

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
                        'relationship.organisation'           => $org_id,
                        'relationship.startDate.onOrBefore'  => $today,
                        'relationship.endDate.onOrAfter'     => $today,
                        'relationship.entity.type'           => 'enrolment'
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

                # Check if we should run additional pass for unmapped organizations
                my $config_data = $self->get_config_data();
                my $use_default_for_unmapped = $config_data->{use_default_for_unmapped} || 'No';
                
                if ($use_default_for_unmapped eq 'Yes') {
                    # Run an additional pass WITHOUT organization filtering
                    # to catch users from unmapped schools (they'll use default branchcode)
                    log_message('Yes', "Running additional sync pass for unmapped organizations (default branchcode)");
                    
                    my $filter_params_all = {
                        'relationship.startDate.onOrBefore' => $today,
                        'relationship.endDate.onOrAfter'    => $today,
                        'relationship.entity.type'          => 'enrolment'
                    };
                    
                    eval {
                        my $result = $self->fetchDataFromAPI($data_endpoint, $filter_params_all, 'DEFAULT_BRANCH');
                        if (defined $result && $result == 0) {
                            log_message('Yes', "Additional sync pass completed");
                        }
                    };
                    
                    if ($@) {
                        if ($@ =~ /EndLastPageFromAPI/) {
                            log_message('Yes', "Additional sync pass completed (reached last page)");
                        } elsif ($@ !~ /ErrorVerifyCategorycodeBranchcode/) {
                            log_message('Yes', "Error in additional sync pass: $@");
                        }
                    }
                } else {
                    log_message('Yes', "Skipping additional sync pass (use_default_for_unmapped is disabled)");
                }

                # Mark users that were not in API but have been processed before
                $self->mark_missing_users_from_api($dbh);

                print "EndLastPageFromAPI\n";
                die "EndLastPageFromAPI";
            }
        } else {
            log_message('Yes', "No organizations left to process today");
            return 0;
        }
    } else {
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
            'relationship.endDate.onOrAfter'    => $today,
            'relationship.entity.type'          => 'enrolment'
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
                # Mark users that were not in API but have been processed before
                $self->mark_missing_users_from_api($dbh);
                print "EndLastPageFromAPI\n";
                log_message('Yes', "Processing completed without organisation filtering");
                die "EndLastPageFromAPI";
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

sub get_organisation_id {
    my ($self, $org_code, $config_data) = @_;
    
    my $ua = LWP::UserAgent->new;
    my $token = $self->get_api_token($config_data, $ua);
    return unless $token;
    
    my $ist_url    = $config_data->{ist_api_url} || '';
    my $customerId = $config_data->{ist_customer_id} || '';
    my $org_url    = "$ist_url/ss12000v2-api/source/$customerId/v2.0/organisations?organisationCode=$org_code";
    
    log_message("Yes", "get_organisation_id URL: $org_url");
    log_message("Yes", "get_organisation_id token present: " . (length($token) > 0 ? 'yes' : 'no'));

    my $request = HTTP::Request->new(
        'GET',
        $org_url,
        [
            'Accept'        => 'application/json',
            'Authorization' => "Bearer $token"
        ]
    );
    
    my $response = $ua->request($request);
    log_message("Yes", "get_organisation_id response code: " . $response->code);
    log_message("Yes", "get_organisation_id response body: " . $response->content);
        
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

sub get_api_token {
    my ($self, $config_data, $ua) = @_;
    
    my $client_id     = $config_data->{ist_client_id} || '';
    my $client_secret = xor_encrypt($config_data->{ist_client_secret}, $skey) || '';
    my $oauth_url     = $config_data->{ist_oauth_url} || '';
    
    my $token_request = POST $oauth_url, [
        client_id     => $client_id,
        client_secret => $client_secret,
        grant_type    => 'client_credentials'
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

sub get_current_enrolment_old {
    my ($enrolments) = @_;
    return undef unless defined $enrolments && ref $enrolments eq 'ARRAY';
    
    use DateTime;
    my $dt = DateTime->now;
    my $today = $dt->ymd;

    foreach my $enrolment (@$enrolments) {
        next if defined $enrolment->{cancelled} && $enrolment->{cancelled};

        my $start = $enrolment->{startDate};
        my $end   = $enrolment->{endDate};

        if (defined $start && defined $end && $start le $today && $today le $end) {
            return $enrolment;
        }
    }
    return undef;
}

sub get_current_enrolment {
    my ($enrolments, $ignore_cancelled) = @_;
    return undef unless defined $enrolments && ref $enrolments eq 'ARRAY';

    use DateTime;
    my $dt = DateTime->now;
    my $today = $dt->ymd;

    # Filter valid enrolments and sort by startDate descending (newest first)
    my @sorted_enrolments = sort {
        $b->{startDate} cmp $a->{startDate}
    } grep {
        ($ignore_cancelled || !$_->{cancelled}) &&
        defined $_->{startDate} && defined $_->{endDate} &&
        $_->{startDate} le $today &&
        $today le $_->{endDate}
    } @$enrolments;

    if (@sorted_enrolments) {
        my $enrolment = $sorted_enrolments[0];
        log_message('Yes', "Selected enrolment start: " . $enrolment->{startDate} . ", end: " . $enrolment->{endDate} . ", enroledAtId: " . ($enrolment->{enroledAt}->{id} // 'undefined'));
        return $enrolment;
    }

    # Provide detailed reason why no valid enrolment was found
    my @cancelled = grep { $_->{cancelled} } @$enrolments;
    my @outside_dates = grep { 
        ($ignore_cancelled || !$_->{cancelled}) && 
        defined $_->{startDate} && defined $_->{endDate} &&
        !($_->{startDate} le $today && $today le $_->{endDate})
    } @$enrolments;

    if (@cancelled && !$ignore_cancelled) {
        log_message('Yes', "No valid enrolment found: " . scalar(@cancelled) . " enrolment(s) cancelled (endDate: " . ($cancelled[0]->{endDate} // 'undefined') . ")");
    } elsif (@outside_dates) {
        log_message('Yes', "No valid enrolment found: enrolment(s) outside current date range");
    } elsif (!@$enrolments) {
        log_message('Yes', "No valid enrolment found: no enrolments exist for user");
    } else {
        log_message('Yes', "No valid enrolment found for user");
    }
    return undef;
}

sub mark_missing_users_from_api {
    my ($self, $dbh) = @_;
    
    my $current_version_info = $version_info;
    
    # Get API config and token for verification
    my $config_data = $self->get_config_data();
    my $ua = LWP::UserAgent->new;
    my $access_token = $self->get_api_token($config_data, $ua);
    
    if (!$access_token) {
        log_message('Yes', "ERROR: Cannot verify missing users - failed to get API token");
        return;
    }
    
    my $ist_url = $config_data->{ist_api_url} || '';
    my $customerId = $config_data->{ist_customer_id} || '';
    my $api_url_base = "$ist_url/ss12000v2-api/source/$customerId/v2.0/";
    
    log_message('Yes', "Starting mark_missing_users_from_api");
    
    # Find users previously processed by SS12000 who were NOT processed today
    # We check if opacnote doesn't contain today's date (YYYY-MM-DD format)
    my $today_date = strftime("%Y-%m-%d", localtime);
    
    my $select_potential_missing_query = qq{
        SELECT borrowernumber, cardnumber, surname, firstname, opacnote
        FROM $borrowers_table
        WHERE opacnote LIKE '%by SS12000:%'
        AND opacnote NOT LIKE '%No update: user no longer in API%'
        AND opacnote NOT LIKE '%$today_date%'
        AND cardnumber IS NOT NULL
        AND cardnumber != ''
    };
    
    eval {
        my $sth = $dbh->prepare($select_potential_missing_query);
        $sth->execute();
        
        my @confirmed_missing;
        my $checked_count = 0;
        my $skipped_count = 0;
        my $found_updated_count = 0;
        
        while (my $row = $sth->fetchrow_hashref) {
            my $cardnumber = $row->{cardnumber};
            next unless $cardnumber;
            
            # Verify with API call: GET /persons?civicNo={cardnumber}&expand=enrolments
            my $verify_url = $api_url_base . "persons?civicNo=" . $cardnumber . "&expand=enrolments";
            
            # Make API request with full response handling and retry logic
            my $ua_check = LWP::UserAgent->new(timeout => 30);
            my $http_response;
            my $max_retries = 2;
            
            for my $attempt (1 .. $max_retries) {
                my $api_request = HTTP::Request->new(GET => $verify_url);
                $api_request->header('Content-Type'  => 'application/json');
                $api_request->header('Authorization' => "Bearer $access_token");
                $http_response = $ua_check->request($api_request);
                
                # If success or 404, no retry needed
                last if $http_response->is_success || $http_response->code == 404;
                
                # For other errors, wait and retry
                if ($attempt < $max_retries) {
                    log_message('Yes', "  Retry $attempt for $cardnumber after HTTP " . $http_response->code);
                    select(undef, undef, undef, 1);  # Wait 1 second before retry
                }
            }
            $checked_count++;
            
            my $user_status = 'unknown';  # 'found', 'not_found', or 'error'
            
            my $skip_reason_from_api = '';
            
            if ($http_response->is_success) {
                # HTTP 200 - check if data is empty and analyze enrolments
                eval {
                    my $response_data = decode_json($http_response->decoded_content);
                    my $data = $response_data->{data} || [];
                    
                    if (scalar(@$data) > 0) {
                        $user_status = 'found';
                        
                        # Check enrolments to determine skip reason
                        my $person_data = $data->[0];
                        my $enrolments = $person_data->{enrolments} || [];
                        
                        if (scalar(@$enrolments) == 0) {
                            $skip_reason_from_api = 'no enrolments in API';
                        } else {
                            # Check if all enrolments are cancelled or expired
                            my $today = strftime("%Y-%m-%d", localtime);
                            my @cancelled = grep { $_->{cancelled} } @$enrolments;
                            my @expired = grep { 
                                !$_->{cancelled} && 
                                defined $_->{endDate} && 
                                $_->{endDate} lt $today 
                            } @$enrolments;
                            my @active = grep {
                                !$_->{cancelled} &&
                                (!defined $_->{endDate} || $_->{endDate} ge $today)
                            } @$enrolments;
                            
                            if (scalar(@active) == 0) {
                                if (scalar(@cancelled) > 0) {
                                    $skip_reason_from_api = 'enrolment cancelled';
                                } elsif (scalar(@expired) > 0) {
                                    $skip_reason_from_api = 'dateexpiry exceeded';
                                } else {
                                    $skip_reason_from_api = 'no valid enrolment';
                                }
                            }
                        }
                    } else {
                        $user_status = 'not_found';
                    }
                };
                if ($@) {
                    log_message('Yes', "  WARNING: Error parsing API response for $cardnumber: $@");
                    $user_status = 'error';
                }
            } elsif ($http_response->code == 404) {
                # HTTP 404 - user explicitly not found
                eval {
                    my $error_data = decode_json($http_response->decoded_content);
                    if ($error_data->{code} && $error_data->{code} eq 'not_found') {
                        $user_status = 'not_found';
                    } else {
                        $user_status = 'error';
                    }
                };
                if ($@) {
                    # 404 without JSON body - still treat as not_found
                    $user_status = 'not_found';
                }
            } else {
                # Other HTTP errors (timeout, 500, etc.) - skip this user
                my $error_body = '';
                eval { $error_body = substr($http_response->decoded_content, 0, 200); };
                log_message('Yes', "  WARNING: API error for $cardnumber (HTTP " . $http_response->code . "): $error_body - skipping");
                $user_status = 'error';
            }
            
            # Handle based on status
            if ($user_status eq 'not_found') {
                push @confirmed_missing, $row;
                log_message('Yes', "  CONFIRMED missing: cardnumber $cardnumber (not found in API)");
            } elsif ($user_status eq 'found') {
                # User exists in API - update opacnote to mark as "seen"
                my $note_suffix = $skip_reason_from_api 
                    ? "No update: $skip_reason_from_api"
                    : "verified (active enrolment, not in sync)";
                    
                my $update_found_query = qq{
                    UPDATE $borrowers_table 
                    SET opacnote = CASE
                        WHEN opacnote LIKE '%Updated by SS12000: plugin%'
                            THEN CONCAT(
                                SUBSTRING_INDEX(opacnote, 'Updated by SS12000: plugin', 1),
                                'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' ', ?
                            )
                        WHEN opacnote LIKE '%Added by SS12000: plugin%'
                            THEN CONCAT(
                                SUBSTRING_INDEX(opacnote, 'Added by SS12000: plugin', 1),
                                'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' ', ?
                            )
                        ELSE opacnote
                    END
                    WHERE borrowernumber = ?
                };
                eval {
                    $dbh->do($update_found_query, undef, 
                        $current_version_info, $note_suffix,
                        $current_version_info, $note_suffix,
                        $row->{borrowernumber}
                    );
                };
                $found_updated_count++;
                log_message('Yes', "  Found in API: cardnumber $cardnumber ($note_suffix)");
            } elsif ($user_status eq 'error') {
                $skipped_count++;
            }
            
            # Add a small delay to avoid overwhelming the API
            select(undef, undef, undef, 0.1) if $checked_count % 10 == 0;
        }
        
        log_message('Yes', "Checked $checked_count users: " . scalar(@confirmed_missing) . " missing, $found_updated_count found+updated, $skipped_count errors");
        
        if (@confirmed_missing) {
            foreach my $user (@confirmed_missing) {
                # Add to data change log
                my $insert_log_query = qq{
                    INSERT INTO $data_change_log_table (table_name, record_id, action, change_description)
                    VALUES ('borrowers', ?, 'no_longer_in_api', ?)
                };
                $dbh->do($insert_log_query, undef, 
                    $user->{borrowernumber}, 
                    "User no longer in API (verified) - cardnumber: $user->{cardnumber}"
                );
                
                # Update opacnote for this user
                my $update_query = qq{
                    UPDATE $borrowers_table 
                    SET opacnote = CASE
                        WHEN opacnote LIKE '%Updated by SS12000: plugin%'
                            THEN CONCAT(
                                SUBSTRING_INDEX(opacnote, 'Updated by SS12000: plugin', 1),
                                'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' No update: user no longer in API'
                            )
                        WHEN opacnote LIKE '%Added by SS12000: plugin%'
                            THEN CONCAT(
                                SUBSTRING_INDEX(opacnote, 'Added by SS12000: plugin', 1),
                                'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' No update: user no longer in API'
                            )
                        ELSE opacnote
                    END
                    WHERE borrowernumber = ?
                };
                $dbh->do($update_query, undef, $current_version_info, $current_version_info, $user->{borrowernumber});
            }
            
            log_message('Yes', "Marked " . scalar(@confirmed_missing) . " user(s) as no longer in API (verified via API)");
        }
    };
    if ($@) {
        log_message('Yes', "Error marking missing users: $@");
    }
}

sub fetchDataFromAPI {
    my ($self, $data_endpoint, $filter_params, $current_org_code) = @_;

    my $dbh = C4::Context->dbh;
    my $response_page_token;     

    our $added_count     = 0;
    our $updated_count   = 0;
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
    my $excluding_dutyRole_empty   = $config_data->{excluding_dutyRole_empty} || 'No';
    my $ignore_cancelled_flag      = $config_data->{ignore_cancelled_flag} || 'No';
    my $dateexpiry_fallback = $config_data->{dateexpiry_fallback} || 'keep';
    my $dateexpiry_months   = int($config_data->{dateexpiry_months} || 12);
    
    if ($debug_mode eq "Yes") { 
        log_message($debug_mode, "Starting processing for endpoint: $data_endpoint, organisation: $current_org_code");
        log_message($debug_mode, "Filter params: " . Dumper($filter_params)) if $filter_params;
    }

    my $ua = LWP::UserAgent->new;
    my $api_url      = "$ist_url/ss12000v2-api/source/$customerId/v2.0/$data_endpoint?limit=$api_limit";
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
        my $access_token  = $oauth_content->{access_token};

        my $select_tokens_query = qq{
            SELECT id, page_token_next
            FROM $logs_table
            WHERE is_processed = 1
            AND data_endpoint = ?
            AND organisation_code = ?
            AND DATE(created_at) = CURDATE()
            AND page_token_next IS NOT NULL
            AND page_token_next != 'RESET'
            ORDER BY created_at DESC
            LIMIT 1
        };
        
        my $sth_select_tokens = $dbh->prepare($select_tokens_query);
        $sth_select_tokens->execute($data_endpoint, $current_org_code);
        my ($data_id, $page_token_next) = $sth_select_tokens->fetchrow_array;

        if (defined $page_token_next) {
            $api_url = $api_url."&pageToken=$page_token_next";
        } 

        my ($response_data, $http_err_code) = getApiResponse($api_url, $access_token);

        # 410 Gone — the pageToken cursor expired on the API side.
        # This happens when a previous run completed this org but did not write
        # page_token_next = NULL (e.g. process was interrupted after the last page
        # was fetched but before the DB update). Treat as completed and move on.
        if (!defined $response_data && ($http_err_code // 0) == 410) {
            log_message('Yes', "pageToken expired (410 Gone) for org $current_org_code — marking as completed");
            if (defined $data_id) {
                $dbh->do(
                    "UPDATE $logs_table SET page_token_next = NULL, is_processed = 1 WHERE id = ?",
                    undef, $data_id
                );
            }
            die "EndLastPageFromAPI";
        }

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
                    log_message($debug_mode, "Data from API successfully inserted into $logs_table ");
                } else {
                    die "Error inserting data into $logs_table: " . $dbh->errstr;
                }
            };
            if ($@) {
                warn "Database error: $@";
            }
        }

        my $select_categories_mapping_query = qq{SELECT id, categorycode, dutyRole, not_import FROM $categories_mapping_table};
        my $select_branches_mapping_query   = qq{SELECT id, branchcode, organisationCode FROM $branches_mapping_table};
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
                $ignore_cancelled_flag,
                \@categories_mapping,
                \@branches_mapping,
                $dateexpiry_fallback,
                $dateexpiry_months,
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

# Fetch a URL with Bearer token auth.
# Returns (body, undef) on HTTP 2xx, (undef, $http_code) on any error.
# Always logs the HTTP status on failure so the caller gets a meaningful error
# instead of a cryptic "malformed JSON" from decode_json(undef).
# Callers that only need the body can still call in scalar context — they just
# get undef on error, same as before. Callers that need the status code should
# call in list context: my ($body, $err_code) = getApiResponse(...).
sub getApiResponse {
    my ($api_url, $access_token) = @_;

    my $ua = LWP::UserAgent->new;

    my $api_request = HTTP::Request->new(GET => $api_url);
    $api_request->header('Content-Type'  => 'application/json');
    $api_request->header('Authorization' => "Bearer $access_token");
    my $api_response = $ua->request($api_request);

    if ($api_response->is_success) {
        return wantarray ? ($api_response->decoded_content, undef) : $api_response->decoded_content;
    } else {
        log_message('Yes', "getApiResponse HTTP error: " . $api_response->status_line . " URL: $api_url");
        return wantarray ? (undef, $api_response->code) : undef;
    }
}


sub verify_categorycode_and_branchcode {
    my $dbh = C4::Context->dbh;

    $dbh->do("SET NAMES utf8mb4");
    $dbh->do("SET CHARACTER SET utf8mb4");

    my $select_categories_mapping_query = qq{SELECT categorycode FROM $categories_mapping_table};
    my $select_branches_mapping_query   = qq{SELECT branchcode FROM $branches_mapping_table};

    my $categories_mapping_exists = $dbh->selectall_arrayref($select_categories_mapping_query);
    my $branches_mapping_exists   = $dbh->selectall_arrayref($select_branches_mapping_query);

    if (@$categories_mapping_exists && @$branches_mapping_exists) {
        my $select_categorycode_query = qq{SELECT categorycode FROM $categories_table};
        my $select_branchcode_query   = qq{SELECT branchcode FROM $branches_table};

        my $categorycode_exists = $dbh->selectall_arrayref($select_categorycode_query);
        my $branchcode_exists   = $dbh->selectall_arrayref($select_branchcode_query);

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
            $ignore_cancelled_flag,
            $categories_mapping_ref,
            $branches_mapping_ref,
            $dateexpiry_fallback,
            $dateexpiry_months,
        ) = @_;

    my @categories_mapping = @$categories_mapping_ref;
    my @branches_mapping   = @$branches_mapping_ref;

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
                my $koha_branchcode   = $koha_default_branchcode;
                my $not_import        = 0;
                my $skip_reason       = '';

                my $response_page_data = $response_data->{data}[$i-1];
                if ($response_page_data) {

                    log_message($debug_mode, 'STARTED DEBUGGING THE CURRENT USER');
                    log_message($debug_mode, 'koha_default_categorycode: '.$koha_default_categorycode);
                    log_message($debug_mode, 'koha_default_branchcode: '.$koha_default_branchcode);

                    my $id = $response_page_data->{id};
                    log_message($debug_mode, 'api response_page_data->{id}: '.$id);

                    # Fetch group memberships to find the current "Klass" for the student
                    my $person_groupMemberships_api_url = $api_url_base."persons/".$id."?expand=groupMemberships";
                    log_message($debug_mode, 'person_groupMemberships_api_url: '.$person_groupMemberships_api_url);
                    my $response_data_groupMemberships;
                    eval {
                        my $raw_groupMemberships = getApiResponse($person_groupMemberships_api_url, $access_token);
                        if (defined $raw_groupMemberships && length($raw_groupMemberships) > 0) {
                            $response_data_groupMemberships = decode_json($raw_groupMemberships);
                            log_message($debug_mode, 'response_data_groupMemberships: '.Dumper($response_data_groupMemberships));
                        } else {
                            log_message($debug_mode, "Empty/undef response for groupMemberships, person id: $id");
                        }
                    };

                    use DateTime;
                    my $dt    = DateTime->now;
                    my $today = $dt->ymd;

                    my $klass_displayName;
                    log_message($debug_mode, '::groupMembership (trying to get Klass) BEGIN');

                    # Collect all valid klasses and pick the newest by startDate
                    my @valid_klasses;
                    if (defined $response_data_groupMemberships &&
                        defined $response_data_groupMemberships->{_embedded} &&
                        defined $response_data_groupMemberships->{_embedded}->{groupMemberships}) {
                        @valid_klasses = grep {
                            $_->{group}->{groupType} eq "Klass" &&
                            $_->{group}->{endDate} gt $today &&
                            $_->{group}->{startDate} lt $today
                        } @{$response_data_groupMemberships->{_embedded}->{groupMemberships}};

                        @valid_klasses = sort {
                            $b->{group}->{startDate} cmp $a->{group}->{startDate}
                        } @valid_klasses;
                    }

                    if (@valid_klasses) {
                        $klass_displayName = $valid_klasses[0]->{group}->{displayName};
                        log_message($debug_mode, 'Selected newest klass_displayName: ' . $klass_displayName);
                        log_message($debug_mode, 'klass startDate: ' . $valid_klasses[0]->{group}->{startDate});
                        log_message($debug_mode, 'klass endDate: ' . $valid_klasses[0]->{group}->{endDate});
                        
                        if (@valid_klasses > 1) {
                            log_message($debug_mode, 'Multiple valid klasses found (' . scalar(@valid_klasses) . '), picked the newest');
                        }
                    } else {
                        log_message($debug_mode, 'No valid Klass found');
                    }

                    log_message($debug_mode, '::groupMembership END');

                    my $person_api_url    = $api_url_base."duties?person=".$id;
                    log_message($debug_mode, 'person_api_url: '.$person_api_url);
                    my $response_data_person;
                    my $duty_role;

                    eval {
                        my $raw_duties = getApiResponse($person_api_url, $access_token);
                        if (defined $raw_duties && length($raw_duties) > 0) {
                            $response_data_person = decode_json($raw_duties);
                            log_message($debug_mode, 'response_data_person: '.Dumper($response_data_person));
                        } else {
                            log_message($debug_mode, "Empty/undef response for duties, person id: $id");
                        }
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
                                $not_import        = $category_mapping->{not_import} || 0;
                                log_message($debug_mode, 'Geted not_import flag from mysql base, category_mapping import set to: '.($not_import ? 'no' : 'yes'));
                                last; 
                            } 
                            log_message($debug_mode, 'koha_categorycode settled to: '.$koha_categorycode);
                            log_message($debug_mode, 'not_import flag settled to: '.($not_import ? 'no' : 'yes'));
                        }
                    } else {
                        if ($excluding_dutyRole_empty eq "Yes") {
                                $not_import = 1;
                                $skip_reason = 'empty duty role';
                                log_message($debug_mode, 'Duty_role is empty, not import data, excluding_dutyRole_empty in config settled to Yes');
                        }
                    }
                    log_message($debug_mode, '::duty_role END');

                    my $enrolments = $response_page_data->{enrolments}; 
                    log_message($debug_mode, 'enrolments: '.Dumper($enrolments));
                    my $enroledAtId = "";

                    log_message($debug_mode, '::organisationCode BEGIN');

                    my $current_enrolment = get_current_enrolment($enrolments, $ignore_cancelled_flag eq 'Yes');
                    my $enrolment_end_date;
                    my $enrolment_cancelled = 0;  

                    if (defined $current_enrolment) {
                        my $enroledAt = $current_enrolment->{enroledAt}; 
                        if (defined $enroledAt && ref $enroledAt eq 'HASH') {
                            $enroledAtId       = $enroledAt->{id};
                            $enrolment_end_date = $current_enrolment->{endDate};
                            $enrolment_cancelled = $current_enrolment->{cancelled} ? 1 : 0;
                            log_message($debug_mode, 'Using current enrolment - startDate: '.$current_enrolment->{startDate}.', endDate: '.$current_enrolment->{endDate}.', cancelled: '.($enrolment_cancelled ? 'yes' : 'no'));
                        }
                    } else {
                        log_message($debug_mode, 'No current valid enrolment found for student');
                    }

                    if ($enroledAtId) {
                        log_message($debug_mode, 'enroledAtId: '.$enroledAtId);
                        
                        my $person_api_url = $api_url_base."organisations/".$enroledAtId;
                        log_message($debug_mode, 'person_api_url: '.$person_api_url);
                        my $organisationCode;

                        eval {
                            my $raw_org = getApiResponse($person_api_url, $access_token);
                            if (defined $raw_org && length($raw_org) > 0) {
                                $response_data_person = decode_json($raw_org);
                                log_message($debug_mode, 'response_data_person: '.Dumper($response_data_person));
                            } else {
                                log_message($debug_mode, "Empty/undef response for organisation id: $enroledAtId");
                            }
                        };

                        if (defined $response_data_person && ref($response_data_person) eq 'HASH') {
                            if (defined $response_data_person->{organisationCode} && $response_data_person->{organisationCode} ne '') {
                                $organisationCode = $response_data_person->{organisationCode};
                            } elsif (defined $response_data_person->{parentOrganisation} &&
                                    defined $response_data_person->{parentOrganisation}->{id}) {
                                my $parent_id = $response_data_person->{parentOrganisation}->{id};
                                log_message($debug_mode, 'No organisationCode on child org, looking up parent: '.$parent_id);
                                eval {
                                    my $raw_parent = getApiResponse($api_url_base.'organisations/'.$parent_id, $access_token);
                                    if (defined $raw_parent && length($raw_parent) > 0) {
                                        my $parent_data = decode_json($raw_parent);
                                        if (defined $parent_data->{organisationCode} && $parent_data->{organisationCode} ne '') {
                                            $organisationCode = $parent_data->{organisationCode};
                                            log_message($debug_mode, 'Resolved organisationCode from parent: '.$organisationCode);
                                        }
                                    } else {
                                        log_message($debug_mode, "Empty/undef response for parent organisation id: $parent_id");
                                    }
                                };
                                if ($@) {
                                    log_message($debug_mode, 'Error looking up parent organisation: '.$@);
                                }
                            }
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
                    } else {

                        if ($excluding_enrolments_empty eq "Yes") {
                                $not_import = 1;
                                # Check why no valid enrolment was found
                                my $ignore_cancelled = ($ignore_cancelled_flag eq 'Yes');
                                my @cancelled = grep { $_->{cancelled} } @{$enrolments // []};
                                my @expired = grep { 
                                    ($ignore_cancelled || !$_->{cancelled}) && 
                                    defined $_->{endDate} && 
                                    $_->{endDate} lt $today 
                                } @{$enrolments // []};
                                
                                if (@cancelled && !$ignore_cancelled) {
                                    $skip_reason = 'enrolment cancelled';
                                } elsif (@expired) {
                                    $skip_reason = 'dateexpiry exceeded';
                                } else {
                                    $skip_reason = 'no valid enrolment';
                                }
                                log_message($debug_mode, 'Enrolments is empty, not import data, excluding_Enrolments_empty in config settled to Yes');
                        }

                    }
                    log_message($debug_mode, '::organisationCode END');

                    my $givenName  = $response_page_data->{givenName};
                    my $familyName = $response_page_data->{familyName};
                    my $birthDate  = $response_page_data->{birthDate};
                    my $sex        = $response_page_data->{sex};
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

                    my $emails  = $response_page_data->{emails};
                    my $email   = "";
                    my $B_email = "";

                    if (defined $emails && ref $emails eq 'ARRAY') {
                        my $found_private    = 0;
                        my $first_non_private;
  
                        foreach my $selectedEmail (@$emails) {
                            next unless defined $selectedEmail->{value};
                            
                            if ($selectedEmail->{type} eq "Privat") {
                                $B_email       = lc($selectedEmail->{value});
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

                    my $addresses = $response_page_data->{addresses};
                    log_message($debug_mode, 'Geted addresses from api: '.Dumper($addresses));
                    my $streetAddress          = "";
                    my $locality               = "";
                    my $postalCode             = "";
                    my $country                = "";
                    my $countyCode             = "";
                    my $municipalityCode       = "";
                    my $realEstateDesignation  = "";
                    my $type                   = "";
                    if (defined $addresses && ref $addresses eq 'ARRAY') {
                       foreach my $selectedAddresses (@$addresses) {
                            $type = $selectedAddresses->{type};
                            if (defined $type && length($type) > 1 && $type =~ /Folkbokf.?ring/) {
                                if (defined $selectedAddresses->{streetAddress}) {               
                                    $streetAddress = ucfirst(lc($selectedAddresses->{streetAddress}));
                                }
                                if (defined $selectedAddresses->{locality}) {
                                    $locality = ucfirst(lc($selectedAddresses->{locality}));
                                }
                                $postalCode            = $selectedAddresses->{postalCode};
                                $country               = $selectedAddresses->{country};
                                $countyCode            = $selectedAddresses->{countyCode};
                                $municipalityCode      = $selectedAddresses->{municipalityCode};
                                $realEstateDesignation = $selectedAddresses->{realEstateDesignation};
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
                    my $phone        = "";
                    my $mobile_phone = "";
                    if (defined $phoneNumbers && ref $phoneNumbers eq 'ARRAY') {
                        foreach my $selectedPhone (@$phoneNumbers) {
                            my $phone_value = $selectedPhone->{value};
                            my $is_mobile   = $selectedPhone->{mobile};
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
                                $enrolment_end_date,
                                $dateexpiry_fallback,
                                $dateexpiry_months,
                                $enrolment_cancelled,
                            );
                    } else {
                        # User is skipped - update opacnote with reason if user exists
                        my $current_version_info = $version_info;
                        my $skip_note_query = qq{
                            UPDATE $borrowers_table
                            SET opacnote = CASE
                                WHEN opacnote IS NULL OR opacnote = ''
                                    THEN CONCAT('Updated by SS12000: plugin ', ?, ' at ', NOW(), ' No update: ', ?)
                                WHEN opacnote LIKE '%Updated by SS12000: plugin%'
                                    THEN CONCAT(
                                        SUBSTRING_INDEX(opacnote, 'Updated by SS12000: plugin', 1),
                                        'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' No update: ', ?
                                    )
                                WHEN opacnote LIKE '%Added by SS12000: plugin%'
                                    THEN CONCAT(
                                        SUBSTRING_INDEX(opacnote, 'Added by SS12000: plugin', 1),
                                        'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' No update: ', ?
                                    )
                                ELSE CONCAT(
                                    opacnote,
                                    '\nUpdated by SS12000: plugin ', ?, ' at ', NOW(), ' No update: ', ?
                                )
                            END
                            WHERE userid = ? OR cardnumber = ?
                        };
                        eval {
                            my $skip_sth = $dbh->prepare($skip_note_query);
                            $skip_sth->execute(
                                $current_version_info, $skip_reason,
                                $current_version_info, $skip_reason,
                                $current_version_info, $skip_reason,
                                $current_version_info, $skip_reason,
                                $userid, $cardnumber
                            );
                            if ($skip_sth->rows > 0) {
                                log_message($debug_mode, "Updated opacnote for skipped user (reason: $skip_reason)");
                            }
                        };
                        if ($@) {
                            log_message($debug_mode, "Error updating opacnote for skipped user: $@");
                        }
                    }
                    $j++;
                    log_message($debug_mode, 'ENDED DEBUGGING THE CURRENT USER');
                    log_message($debug_mode, ' ');
                } 
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
        'email'       => $new_values->{email},
        'sex'         => $new_values->{sex},
        'phone'       => $new_values->{phone},
        'mobile'      => $new_values->{mobile_phone},
        'surname'     => $new_values->{surname},
        'firstname'   => $new_values->{firstname},
        'categorycode' => $new_values->{categorycode},
        'branchcode'  => $new_values->{branchcode},
        'address'     => $new_values->{streetAddress},
        'city'        => $new_values->{locality},
        'zipcode'     => $new_values->{postalCode},
        'country'     => $new_values->{country},
        'B_email'     => $new_values->{B_email},
        'userid'      => $new_values->{newUserID},
        'cardnumber'  => $new_values->{newCardnumber},
        'dateexpiry'  => $new_values->{enrolment_end_date}
    );
    
    my @changed_fields;
    
    while (my ($field, $new_value) = each %fields_to_compare) {
        my $old_value = $old_data->{$field};
        $old_value = '' if !defined $old_value;
        $new_value = '' if !defined $new_value;
        
        if ($old_value ne $new_value) {
            push @changed_fields, {
                field     => $field,
                old_value => $old_value,
                new_value => $new_value
            };
        }
    }
    
    return @changed_fields;
}

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
        $enrolment_end_date,
        $dateexpiry_fallback,
        $dateexpiry_months,
        $enrolment_cancelled,
    ) = @_;
    
    my $dbh = C4::Context->dbh;

    my $current_version_info = $version_info;

    my $newUserID;
    my $newCardnumber;

    if ($useridPlugin eq "civicNo" || $useridPlugin eq "externalIdentifier") {
        $newUserID = ($useridPlugin eq "civicNo") ? $userid : $externalIdentifier;
    }

    if ($cardnumberPlugin eq "civicNo" || $cardnumberPlugin eq "externalIdentifier") {
        $newCardnumber = ($cardnumberPlugin eq "civicNo") ? $cardnumber : $externalIdentifier;
    }

    # Hash dispatcher for dateexpiry fallback strategies.
    # Each strategy returns a hashref:
    #   fragment        => SQL fragment for the SET clause
    #   has_placeholder => 1 if fragment contains ?, 0 otherwise
    #   value           => bind value to pass (undef if no placeholder)
    my %dateexpiry_strategies = (
        'none'  => sub { {
            fragment        => 'dateexpiry = NULL',
            has_placeholder => 0,
            value           => undef,
        } },
        'keep'  => sub { {
            fragment        => 'dateexpiry = dateexpiry',
            has_placeholder => 0,
            value           => undef,
        } },
        'months' => sub {
            my @t = localtime(time);
            $t[4] += $dateexpiry_months;
            $t[5] += int($t[4] / 12);
            $t[4]  = $t[4] % 12;
            my $calculated = POSIX::strftime("%Y-%m-%d", @t);
            return {
                fragment        => 'dateexpiry = ?',
                has_placeholder => 1,
                value           => $calculated,
            };
        },
    );

    # Valid API date always wins; otherwise use the configured fallback strategy
    my $dateexpiry;
    if (defined $enrolment_end_date && $enrolment_end_date =~ /^\d{4}-\d{2}-\d{2}$/) {
        $dateexpiry = { fragment => 'dateexpiry = ?', has_placeholder => 1, value => $enrolment_end_date };
        log_message('Yes', "dateexpiry: using API value $enrolment_end_date");
    } else {
        my $strategy = $dateexpiry_strategies{$dateexpiry_fallback}
                    // $dateexpiry_strategies{'keep'};
        $dateexpiry = $strategy->();
        log_message('Yes', "dateexpiry: fallback=$dateexpiry_fallback => $dateexpiry->{fragment}");
    }

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
        my $main_record = shift @duplicates;
        
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
        
        for my $duplicate (@duplicates) {
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
            
            my @tables_to_update = (
                'issues',
                'old_issues',
                'old_reserves',
                'reserves',
                'borrower_attributes',
                'accountlines',
                'message_queue',
                'statistics',
                'borrower_files',
                'borrower_debarments',
                'borrower_modifications',
                'club_enrollments',
                'illrequests',
                'tags_all',
                'reviews'
            );
            
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
            
            eval {
                my $archive_query = qq{
                    UPDATE $borrowers_table 
                    SET
                        userid = CONCAT('ARCHIVED_', userid, '_', borrowernumber),
                        cardnumber = CONCAT('ARCHIVED_', cardnumber, '_', borrowernumber),
                        flags = -1,
                        dateexpiry = NOW(),
                        gonenoaddress = 1,
                        lost = 1,
                        debarredcomment = CONCAT('Updated by SS12000: plugin ', ?, '. Merged with borrowernumber: ', ?, ' at ', NOW()),
                        opacnote = CONCAT('Updated by SS12000: plugin ', ?, '. Merged with borrowernumber: ', ?, ' at ', NOW())
                    WHERE borrowernumber = ?
                };
                my $archive_sth = $dbh->prepare($archive_query);
                $archive_sth->execute(
                    $current_version_info,
                    $main_record->{borrowernumber},
                    $current_version_info,
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
        
        $existing_borrower = $main_record;
        
    } elsif (@duplicates == 1) {
        $existing_borrower = $duplicates[0];
    }

    if ($existing_borrower) {
        my @changes = has_changes($existing_borrower, {
            birthdate          => $birthdate,
            email              => $email,
            sex                => $sex,
            phone              => $phone,
            mobile_phone       => $mobile_phone,
            surname            => $surname,
            firstname          => $firstname,
            categorycode       => $categorycode,
            branchcode         => $branchcode,
            streetAddress      => $streetAddress,
            locality           => $locality,
            postalCode         => $postalCode,
            country            => $country,
            B_email            => $B_email,
            newUserID          => $newUserID,
            newCardnumber      => $newCardnumber,
            enrolment_end_date => $dateexpiry->{value}
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

            my $changed_fields_str = join(', ', map { $_->{field} } @changes);
            
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
                    $dateexpiry->{fragment},
                    opacnote = CASE
                        WHEN opacnote IS NULL OR opacnote = ''
                            THEN CONCAT('Updated by SS12000: plugin ', ?, ' at ', NOW(), ' Fields changed: ', ?, ?)
                        WHEN opacnote LIKE '%Updated by SS12000: plugin%'
                            THEN CONCAT(
                                SUBSTRING_INDEX(opacnote, 'Updated by SS12000: plugin', 1),
                                'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' Fields changed: ', ?, ?
                            )
                        WHEN opacnote LIKE '%Added by SS12000: plugin%'
                            THEN CONCAT(
                                REPLACE(
                                    SUBSTRING_INDEX(opacnote, 'Added by SS12000: plugin', 1),
                                    'Added by', 'Updated by'
                                ),
                                'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' Fields changed: ', ?, ?
                            )
                        ELSE CONCAT(
                            opacnote,
                            '\nUpdated by SS12000: plugin ', ?, ' at ', NOW(), ' Fields changed: ', ?, ?
                        )
                    END,
                    updated_on = NOW()
                WHERE borrowernumber = ?
            };
            
            my $update_sth = $dbh->prepare($update_query);

            my @bind_values = (
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
            );
            push @bind_values, $dateexpiry->{value} if $dateexpiry->{has_placeholder};
            my $cancelled_suffix = $enrolment_cancelled ? ' (enrolment cancelled)' : '';
            push @bind_values, (
                $current_version_info, $changed_fields_str, $cancelled_suffix,
                $current_version_info, $changed_fields_str, $cancelled_suffix,
                $current_version_info, $changed_fields_str, $cancelled_suffix,
                $current_version_info, $changed_fields_str, $cancelled_suffix,
                $existing_borrower->{'borrowernumber'}
            );

            eval { $update_sth->execute(@bind_values) };
            if ($@) {
                log_message('Yes', "Error updating user: $@");
            } else {
                $updated_count++;
                $borrowernumber = $existing_borrower->{'borrowernumber'};
                log_message('Yes', "Successfully updated borrower: " . $existing_borrower->{'borrowernumber'});
            }
        } else {
            log_message('Yes', "No changes detected for borrower: " . $existing_borrower->{'borrowernumber'});
            $borrowernumber = $existing_borrower->{'borrowernumber'};
            
            # Still update opacnote timestamp to mark user as "seen" today
            my $cancelled_suffix = $enrolment_cancelled ? ' (enrolment cancelled)' : '';
            my $touch_opacnote_query = qq{
                UPDATE $borrowers_table
                SET opacnote = CASE
                    WHEN opacnote LIKE '%Updated by SS12000: plugin%'
                        THEN CONCAT(
                            SUBSTRING_INDEX(opacnote, 'Updated by SS12000: plugin', 1),
                            'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' No changes', ?
                        )
                    WHEN opacnote LIKE '%Added by SS12000: plugin%'
                        THEN CONCAT(
                            SUBSTRING_INDEX(opacnote, 'Added by SS12000: plugin', 1),
                            'Updated by SS12000: plugin ', ?, ' at ', NOW(), ' No changes', ?
                        )
                    ELSE opacnote
                END
                WHERE borrowernumber = ?
            };
            eval {
                my $touch_sth = $dbh->prepare($touch_opacnote_query);
                $touch_sth->execute($current_version_info, $cancelled_suffix, $current_version_info, $cancelled_suffix, $borrowernumber);
            };
        }
    } else {
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
                ${\( $dateexpiry->{has_placeholder} ? '?' : $dateexpiry->{fragment} =~ s/dateexpiry = //r )},
                NOW(),
                CONCAT('Added by SS12000: plugin ', ?, ' at ', NOW(), ?)
            )
        };
        my $insert_sth = $dbh->prepare($insert_query);

        my @insert_bind = (
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
            $newUserID,
        );
        push @insert_bind, $dateexpiry->{value} if $dateexpiry->{has_placeholder};
        push @insert_bind, $current_version_info;
        push @insert_bind, ($enrolment_cancelled ? ' (enrolment cancelled)' : '');

        eval {
            $insert_sth->execute(@insert_bind);
            $borrowernumber = $dbh->last_insert_id(undef, undef, $borrowers_table, undef);
            $added_count++;
            log_message('Yes', "Successfully inserted new borrower: borrowernumber=$borrowernumber");
        };
        if ($@) {
            log_message('Yes', "Error inserting user: $@");
            return;
        }
    }

    if ($borrowernumber && $klass_displayName) {
        my $code = 'CL';
        log_message('Yes', "Processing Klass attribute for borrowernumber $borrowernumber, plugin version: $VERSION");

        my $check_types_query = qq{
            SELECT 1 FROM borrower_attribute_types WHERE code = ?
        };
        my $check_types_sth = $dbh->prepare($check_types_query);
        $check_types_sth->execute($code);
        my ($exists) = $check_types_sth->fetchrow_array();

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
                log_message('Yes', "Created borrower_attribute_types for 'CL'");
                $dbh->commit();
            };
            if ($@) {
                log_message('Yes', "Error creating borrower_attribute_types: $@");
                return;
            }
        }

        my $check_query = qq{
            SELECT attribute FROM borrower_attributes 
            WHERE borrowernumber = ? AND code = ?
        };
        my $check_sth = $dbh->prepare($check_query);
        eval {
            $check_sth->execute($borrowernumber, $code);
            log_message('Yes', "Executed SELECT for Klass attribute, borrowernumber=$borrowernumber, code=$code");
        };
        if ($@) {
            log_message('Yes', "Error executing SELECT for borrower_attributes: $@");
            return;
        }
        my $existing_attribute = $check_sth->fetchrow_array();

        log_message('Yes', "Existing Klass in DB: " . ($existing_attribute // 'None') . ", New from API: $klass_displayName");

        if (defined $existing_attribute) {
            if ($existing_attribute ne $klass_displayName) {
                my $update_query = qq{
                    UPDATE borrower_attributes
                    SET attribute = ?
                    WHERE borrowernumber = ? AND code = ?
                };
                my $update_sth = $dbh->prepare($update_query);
                eval {
                    $update_sth->execute($klass_displayName, $borrowernumber, $code);
                    log_message('Yes', "Updated Klass attribute to $klass_displayName for borrowernumber $borrowernumber");
                    $dbh->commit();
                };
                if ($@) {
                    log_message('Yes', "Error updating borrower_attributes: $@");
                }
            } else {
                log_message('Yes', "Klass attribute unchanged, no update needed");
            }
        } else {
            my $insert_query = qq{
                INSERT INTO borrower_attributes (borrowernumber, code, attribute)
                VALUES (?, ?, ?)
            };
            my $insert_sth = $dbh->prepare($insert_query);
            eval {
                $insert_sth->execute($borrowernumber, $code, $klass_displayName);
                log_message('Yes', "Inserted new Klass attribute $klass_displayName for borrowernumber $borrowernumber");
                $dbh->commit();
            };
            if ($@) {
                log_message('Yes', "Error inserting into borrower_attributes: $@");
            }
        }
    } else {
        log_message('Yes', "Skipping Klass update: borrowernumber=" . ($borrowernumber // 'undef') . ", klass_displayName=" . ($klass_displayName // 'undef'));
    }
}



sub get_log_contents {
    my ($self) = @_;
    my $log_file = get_log_file();
    my @log_lines;

    if (-f $log_file) {
        open my $fh, '<', $log_file or return [];
        my @lines = <$fh>;
        close $fh;

        @lines = @lines[-100..-1] if @lines > 100;

        foreach my $line (@lines) {
            if ($line =~ /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(.+)$/) {
                push @log_lines, {
                    timestamp => $1,
                    message   => $2
                };
            }
        }
    }

    return \@log_lines;
}

sub get_status_file {
    my $log_config_dir = C4::Context->config("logdir"); 
    return File::Spec->catfile($log_config_dir, 'imcode-export-users-status.json');
}

# Returns the path to the shared process lock file.
# Both HTTP (web UI) and cron use this same file to prevent concurrent runs.
sub get_lock_file {
    my $log_config_dir = C4::Context->config("logdir");
    return File::Spec->catfile($log_config_dir, 'imcode-export-users.lock');
}

# Attempts to acquire the process lock atomically using O_EXCL.
# $source: 'http' or 'cron' — identifies who is acquiring the lock.
# Returns undef on success (lock acquired).
# Returns a hashref with {pid, source, started_at} if lock already held by a live process.
sub acquire_lock {
    my ($self, $source) = @_;
    my $lock_file = get_lock_file();

    my $fh;
    unless (sysopen($fh, $lock_file, O_WRONLY|O_CREAT|O_EXCL, 0644)) {
        # Lock file exists — check if the owning process is still alive
        if (open my $rfh, '<', $lock_file) {
            local $/;
            my $raw  = <$rfh>;
            close $rfh;
            my $data = eval { decode_json($raw) };
            if ($data && $data->{pid} && kill(0, $data->{pid})) {
                return $data; # Live process holds the lock
            }
        }
        # Stale lock (process gone) — remove and retry once
        unlink $lock_file;
        unless (sysopen($fh, $lock_file, O_WRONLY|O_CREAT|O_EXCL, 0644)) {
            log_message("Yes", "acquire_lock: could not create lock file after removing stale lock");
            return { pid => 0, source => 'unknown', started_at => 0 };
        }
    }

    my $lock_data = {
        pid        => $$,
        source     => $source,
        started_at => time()
    };
    print $fh encode_json($lock_data);
    close $fh;
    return undef; # Lock acquired successfully
}

# Releases the process lock by removing the lock file.
sub release_lock {
    my ($self) = @_;
    my $lock_file = get_lock_file();
    unlink $lock_file if -e $lock_file;
}

# Returns the lock file contents as a hashref, or undef if no lock file exists.
sub get_lock_info {
    my ($self) = @_;
    my $lock_file = get_lock_file();
    return undef unless -f $lock_file;

    open my $fh, '<', $lock_file or return undef;
    local $/;
    my $data = eval { decode_json(<$fh>) };
    close $fh;
    return $data;
}

sub read_status {
    my ($self) = @_;
    my $file = get_status_file();
    if (-f $file) {
        open my $fh, '<', $file or return {};
        local $/;
        my $json = <$fh>;
        close $fh;
        return JSON::decode_json($json);
    }
    return {
        locked      => 0,
        pid         => undef,
        started_at  => undef,
        status      => 'idle',
        last_update => undef,
        messages    => []
    };
}

sub save_status {
    my ($self, $status) = @_;
    my $file = get_status_file();
    
    eval {
        open(my $fh, '>', $file) or die "Cannot open status file: $!";
        flock($fh, LOCK_EX) or die "Cannot lock file: $!";
        print $fh JSON::encode_json($status);
        close($fh) or die "Cannot close file: $!";
    };
    if ($@) {
        warn "Error saving status: $@";
        return 0;
    }
    return 1;
}

sub is_process_running {
    my ($self) = @_;

    # Check the shared lock file — authoritative source for running state
    my $lock_info = $self->get_lock_info();
    if ($lock_info && $lock_info->{pid}) {
        if (kill(0, $lock_info->{pid})) {
            return 1; # Process is alive
        }
        # Stale lock — clean up
        $self->release_lock();
    }
    return 0;
}

sub start_export_process {
    my ($self) = @_;
    
    if ($self->is_process_running()) {
        return { 
            status  => 'error',
            message => 'Export process is already running'
        };
    }

    # Acquire lock before forking so the child PID can be recorded atomically.
    # The lock is acquired here in the parent; the child updates it with its own PID.
    my $existing = $self->acquire_lock('http');
    if ($existing) {
        return {
            status  => 'error',
            message => 'Could not acquire process lock — another process is running'
        };
    }

    my $started_at = time();

    if (my $child_pid = fork()) {
        # Parent: record the child PID in both the lock file and status file,
        # then return immediately so the HTTP response is not blocked.
        my $lock_file = get_lock_file();
        if (open my $fh, '>', $lock_file) {
            print $fh encode_json({ pid => $child_pid, source => 'http', started_at => $started_at });
            close $fh;
        }

        my $status = {
            locked      => 1,
            pid         => $child_pid,   # child PID — not the CGI parent's PID
            started_at  => $started_at,
            status      => 'running',
            last_update => time(),
            messages    => []
        };
        $self->save_status($status);

        return {
            status     => 'started',
            pid        => $child_pid,
            started_at => $started_at
        };
    } elsif (defined $child_pid) {
        # Child: update lock file with own PID, then run export
        my $lock_file = get_lock_file();
        if (open my $fh, '>', $lock_file) {
            print $fh encode_json({ pid => $$, source => 'http', started_at => $started_at });
            close $fh;
        }
        $self->run_web_export();
        exit 0;
    } else {
        $self->release_lock();
        return {
            status  => 'error',
            message => 'Failed to fork export process'
        };
    }
}

sub run_web_export {
    my $self = shift;
    my $dbh  = C4::Context->dbh;
    
    eval {
        my $status = $self->read_status();
        $status->{messages} = [];
        $self->save_status($status);

        # cronjob() manages its own lock internally; since we already hold
        # the HTTP lock, pass is_web=1 to skip the duplicate lock attempt
        # inside cronjob and get return values instead of print output.
        my $result = $self->_cronjob_inner("persons", 1);

        $status = $self->read_status();

        if (defined $result && $result eq "EndLastPageFromAPI") {
            # All pages processed — mark complete and clean up
            $status->{status}  = 'completed';
            $status->{locked}  = 0;
            $status->{pid}     = undef;
            push @{$status->{messages}}, {
                time  => time(),
                text  => "All pages processed successfully",
                error => 0
            };
            $self->save_status($status);
            sleep 10; # Keep status file long enough for the UI to read final state
            unlink get_status_file();
        } else {
            # One page done, more pages remain — UI will show "Scan next page" button
            my ($pages_done) = $dbh->selectrow_array(qq{
                SELECT COUNT(*) FROM $logs_table 
                WHERE DATE(created_at) = CURDATE() 
                AND is_processed = 1 
                AND data_endpoint = 'persons'
            });
            $status->{status}          = 'page_completed';
            $status->{locked}          = 0;
            $status->{pid}             = undef;
            $status->{pages_completed} = $pages_done;
            push @{$status->{messages}}, {
                time  => time(),
                text  => "Page $pages_done processed. More pages available.",
                error => 0
            };
            $self->save_status($status);
        }
    };

    if ($@) {
        my $error = $@;

        if ($error =~ /EndLastPageFromAPI/) {
            # Cronjob signals completion via die — this is expected, not an error
            my $status = $self->read_status();
            $status->{status} = 'completed';
            $status->{locked} = 0;
            $status->{pid}    = undef;
            push @{$status->{messages}}, {
                time  => time(),
                text  => "All pages processed successfully",
                error => 0
            };
            $self->save_status($status);
            sleep 10;
            unlink get_status_file();
        } else {
            # Genuine error
            my $status = $self->read_status();
            $status->{status} = 'error';
            $status->{locked} = 0;
            $status->{pid}    = undef;
            push @{$status->{messages}}, {
                time  => time(),
                text  => "Error during export: $error",
                error => 1
            };
            $self->save_status($status);
        }
    }

    $self->release_lock();
}

sub force_unlock {
    my ($self) = @_;
    my $lock_info = $self->get_lock_info();
    
    if ($lock_info && $lock_info->{pid} && kill(0, $lock_info->{pid})) {
        return {
            status  => 'error',
            message => 'Process is still running'
        };
    }
    
    $self->release_lock();

    my $status = $self->read_status();
    $status->{locked} = 0;
    $status->{pid}    = undef;
    $status->{status} = 'idle';
    $self->save_status($status);
    
    return {
        status  => 'success',
        message => 'Process unlocked successfully'
    };
}


1;
