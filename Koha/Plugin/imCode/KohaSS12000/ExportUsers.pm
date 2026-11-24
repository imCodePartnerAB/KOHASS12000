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
use POSIX qw(strftime); # To format date/time
use Encode;
use URI::Escape qw(uri_escape);
use Unicode::Normalize qw(NFD);
use Koha::Plugin::imCode::KohaSS12000::ExportUsers::Rules ();
BEGIN { *SS12000Rules:: = \%Koha::Plugin::imCode::KohaSS12000::ExportUsers::Rules::; }
use Koha::Plugin::imCode::KohaSS12000::ExportUsers::Schedule ();
BEGIN { *SS12000Schedule:: = \%Koha::Plugin::imCode::KohaSS12000::ExportUsers::Schedule::; }
use Koha::Plugin::imCode::KohaSS12000::ExportUsers::Leavers ();
BEGIN { *SS12000Leavers:: = \%Koha::Plugin::imCode::KohaSS12000::ExportUsers::Leavers::; }
use Koha::Patrons;

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
our $rules_table              = 'imcode_rules';
our $rule_log_table           = 'imcode_rule_log';
our $schedule_table           = 'imcode_schedule';
our $jobs_table               = 'imcode_jobs';
our $state_table              = 'imcode_state';
our $seen_values_table        = 'imcode_seen_values';
our $leaver_mapping_table     = 'imcode_leaver_mapping';
our $leavers_table            = 'imcode_leavers';
our @rules_ddl = (
    qq{CREATE TABLE IF NOT EXISTS $rules_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        priority INT NOT NULL DEFAULT 1,
        name VARCHAR(120) NOT NULL DEFAULT '',
        enabled TINYINT(1) NOT NULL DEFAULT 1,
        scope VARCHAR(10) NOT NULL DEFAULT 'category',
        conditions_json TEXT NOT NULL,
        action_category VARCHAR(10) DEFAULT NULL,
        action_branch VARCHAR(10) DEFAULT NULL,
        not_import TINYINT(1) NOT NULL DEFAULT 0,
        KEY idx_scope_priority (scope, priority)
    );},
    qq{CREATE TABLE IF NOT EXISTS $rule_log_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        person_id VARCHAR(64) NOT NULL,
        borrowernumber INT DEFAULT NULL,
        person_kind VARCHAR(10) DEFAULT NULL,
        category_result VARCHAR(40) DEFAULT NULL,
        category_explain TEXT,
        branch_result VARCHAR(40) DEFAULT NULL,
        branch_explain TEXT,
        decision_hash VARCHAR(32) DEFAULT NULL,
        changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_person (person_id),
        KEY idx_borrowernumber (borrowernumber)
    );},
    qq{CREATE TABLE IF NOT EXISTS $schedule_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        weekday TINYINT NOT NULL,
        run_time CHAR(5) NOT NULL,
        job_type VARCHAR(10) NOT NULL DEFAULT 'import',
        UNIQUE KEY unique_entry (weekday, run_time, job_type)
    );},
    qq{CREATE TABLE IF NOT EXISTS $jobs_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        job_type VARCHAR(10) NOT NULL,
        source VARCHAR(10) NOT NULL DEFAULT 'manual',
        schedule_key VARCHAR(60) DEFAULT NULL,
        status VARCHAR(10) NOT NULL DEFAULT 'queued',
        requested_by VARCHAR(100) DEFAULT NULL,
        requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        started_at DATETIME DEFAULT NULL,
        finished_at DATETIME DEFAULT NULL,
        message VARCHAR(255) DEFAULT NULL,
        UNIQUE KEY unique_schedule_key (schedule_key),
        KEY idx_status (status)
    );},
    qq{CREATE TABLE IF NOT EXISTS $state_table (
        name VARCHAR(60) NOT NULL PRIMARY KEY,
        value VARCHAR(255) DEFAULT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );},
    qq{CREATE TABLE IF NOT EXISTS $seen_values_table (
        field_name VARCHAR(40) NOT NULL,
        value VARCHAR(190) NOT NULL,
        label VARCHAR(255) DEFAULT NULL,
        seen_count INT NOT NULL DEFAULT 0,
        last_seen DATETIME NOT NULL,
        PRIMARY KEY (field_name, value)
    );},
    qq{CREATE TABLE IF NOT EXISTS $leaver_mapping_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        from_category VARCHAR(10) NOT NULL,
        to_category VARCHAR(10) NOT NULL,
        to_noloans VARCHAR(10) DEFAULT NULL,
        UNIQUE KEY unique_from (from_category)
    );},
    qq{ALTER TABLE $leaver_mapping_table ADD COLUMN IF NOT EXISTS to_noloans VARCHAR(10) DEFAULT NULL;},
    qq{CREATE TABLE IF NOT EXISTS $leavers_table (
        borrowernumber INT NOT NULL PRIMARY KEY,
        first_seen DATETIME NOT NULL,
        reason VARCHAR(40) DEFAULT NULL,
        previous_category VARCHAR(10) DEFAULT NULL,
        anonymized_at DATETIME DEFAULT NULL
    );},
    qq{ALTER TABLE $leavers_table ADD COLUMN IF NOT EXISTS anonymized_at DATETIME DEFAULT NULL;},
);
our $added_count      = 0; # to count added
our $updated_count    = 0; # to count updated
our $processed_count  = 0; # to count processed
our $duties_forbidden = 0; # set when the duties endpoint answers 403
our %duties_by_person;     # all duties of the run grouped by person id (one read instead of one request per person)
our $duties_loaded       = 0;
our $duties_cache_failed = 0;
our $use_duties_cache    = 1;
our %org_code_cache;       # organisation id => organisationCode, valid for one run

our $VERSION = "2.0";

our $metadata = {
    name            => 'Export Users from SS12000',
    author          => 'imCode.com',
    date_authored   => '2023-08-08',
    date_updated    => '2026-11-19',
    minimum_version => '20.05',
    maximum_version => undef,
    version         => $VERSION,
    description     => 'This plugin implements export users from SS12000'
};

our $update_date = $metadata->{date_updated} =~ s/-//gr;

our $version_info = "$update_date v$VERSION";

# ---------------------------------------------------------------------------
# Translations: plain gettext .po files in ExportUsers/ (one file per language, e.g. sv.po,
# fi.po, sv-SE.po). They are read directly, so no compiled .mo files and no system locale are
# needed. To add a language, copy com.imcode.exportusers.pot to <code>.po and translate the
# msgstr lines. A text without translation is shown in English.
# ---------------------------------------------------------------------------
our %i18n_cache;    # language code => { mtime => ..., data => { msgid => msgstr } }

sub i18n_dir {
    return File::Spec->catdir(dirname(abs_path(__FILE__)), 'ExportUsers');
}

# 'sv-SE' => ('sv-SE', 'sv'); anything that is not a plain language code gives nothing
sub i18n_candidates {
    my ($lang) = @_;
    return () unless defined $lang && $lang =~ /^[A-Za-z]{2,3}(?:[-_][A-Za-z0-9]{2,8})*$/;
    (my $l = $lang) =~ s/_/-/g;
    my @codes = ($l);
    push @codes, (split /-/, $l)[0] if $l =~ /-/;
    return @codes;
}

sub i18n_unquote {
    my ($s) = @_;
    my %esc = ('"' => '"', '\\' => '\\', 'n' => "\n", 't' => "\t");
    $s =~ s/\\(["\\nt])/$esc{$1}/g;
    return $s;
}

# Read <code>.po into a hash; the file is read again when it changes on disk
sub i18n_load {
    my ($code) = @_;
    my $file = File::Spec->catfile(i18n_dir(), "$code.po");
    return unless -r $file;
    my $mtime = (stat $file)[9];
    my $cached = $i18n_cache{$code};
    return $cached->{data} if $cached && $cached->{mtime} == $mtime;

    my (%t, $id, $str, $cur);
    my $flush = sub {
        $t{$id} = $str if defined $id && $id ne '' && defined $str && $str ne '';
        undef $id; undef $str; undef $cur;
    };
    open my $fh, '<:encoding(UTF-8)', $file or return;
    while (my $line = <$fh>) {
        chomp $line;
        if    ($line =~ /^msgid\s+"(.*)"\s*$/)     { $flush->(); $id = i18n_unquote($1); $cur = 'id'; }
        elsif ($line =~ /^msgstr\s+"(.*)"\s*$/)    { $str = i18n_unquote($1); $cur = 'str'; }
        elsif ($line =~ /^"(.*)"\s*$/)             { if (($cur // '') eq 'id') { $id .= i18n_unquote($1) } elsif (($cur // '') eq 'str') { $str .= i18n_unquote($1) } }
        elsif ($line =~ /^\s*(?:#.*)?$/)           { $flush->(); }
    }
    $flush->();
    close $fh;

    $i18n_cache{$code} = { mtime => $mtime, data => \%t };
    return \%t;
}

# Language of the current request (Koha interface language, e.g. sv-SE)
sub i18n_language {
    my ($cgi) = @_;
    $cgi ||= eval { require CGI; CGI->new('') };
    my $lang = eval { C4::Languages::getlanguage($cgi) };
    return $lang || 'en';
}

# Translate $msgid; extra arguments are filled into %s placeholders
sub i18n_tr {
    my ($lang, $msgid, @args) = @_;
    my $text = $msgid;
    foreach my $code (i18n_candidates($lang)) {
        my $t = i18n_load($code) or next;
        if (defined $t->{$msgid}) { $text = $t->{$msgid}; last; }
    }
    return @args ? sprintf($text, map { $_ // '' } @args) : $text;
}

# $self->t('text %s', $value): translated for the language of this request
sub t {
    my ($self, @args) = @_;
    $self->{_i18n_lang} //= i18n_language($self->{cgi});
    return i18n_tr($self->{_i18n_lang}, @args);
}

# The imCode logo is embedded in the page (many Koha installations do not serve static plugin files)
our $logo_data_uri_cache;
sub logo_data_uri {
    return $logo_data_uri_cache if defined $logo_data_uri_cache;
    my $file = File::Spec->catfile(i18n_dir(), 'images', 'logo_imcode_footer.png');
    my $uri  = '';
    if (-r $file && open my $fh, '<:raw', $file) {
        local $/;
        require MIME::Base64;
        $uri = 'data:image/png;base64,' . MIME::Base64::encode_base64(<$fh>, '');
        close $fh;
    }
    return $logo_data_uri_cache = $uri;
}

# Function given to the templates as ss('text'); used instead of the gettext filter
sub ss_sub {
    my ($self) = @_;
    return sub { return $self->t(@_) };
}

sub new {
    my ( $class, $args ) = @_;
    $args ||= {};

    ## We need to add our metadata here so our base class can access it.
    ## Name and description are translated for the language of this request.
    my $lang = i18n_language($args->{cgi});
    $args->{'metadata'} = {
        %$metadata,
        class       => $class,
        name        => i18n_tr($lang, $metadata->{name}),
        description => i18n_tr($lang, $metadata->{description}),
    };

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
            open my $fh, '>>:encoding(UTF-8)', $my_log_file 
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
    qq{INSERT INTO imcode_config (name,value) VALUES ('use_rules','No');},
    @rules_ddl,

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

    eval {
        $dbh->do($_) or die "Failed to create rules table: " . $dbh->errstr for @rules_ddl;
        log_message("Yes", "Rules tables checked/created");
    };
    if ($@) {
        warn "Error creating rules tables: $@";
        log_message("Yes", "Error creating rules tables: $@");
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

    my @tables_to_delete = ($config_table, $logs_table, $data_change_log_table, $categories_mapping_table, $branches_mapping_table, $rules_table, $rule_log_table, $schedule_table, $jobs_table, $state_table, $seen_values_table, $leaver_mapping_table, $leavers_table); 

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

    # The change log trigger is recreated only when the stored version differs from the plugin version.
    # The table is unique on (name, value), so the old row is removed instead of updated and exactly
    # one trigger_version row is kept (older installations may have several).
    my ($trigger_current) = $dbh->selectrow_array(
        "SELECT COUNT(*) FROM $config_table WHERE name = 'trigger_version' AND value = ?", undef, $VERSION
    );
    if (!$trigger_current) {
        if (recreate_trigger($dbh)) {
            $dbh->do("DELETE FROM $config_table WHERE name = 'trigger_version'");
            $dbh->do("INSERT INTO $config_table (name, value) VALUES ('trigger_version', ?)", undef, $VERSION);
            log_message("Yes", "Trigger recreated for plugin version $VERSION");
        }
    } else {
        $dbh->do("DELETE FROM $config_table WHERE name = 'trigger_version' AND value <> ?", undef, $VERSION);
    }

    insertConfigValue($dbh, 'excluding_dutyRole_empty', 'No');
    insertConfigValue($dbh, 'excluding_enrolments_empty', 'No');
    insertConfigValue($dbh, 'ignore_cancelled_flag', 'No');
    insertConfigValue($dbh, 'use_default_for_unmapped', 'No');
    insertConfigValue($dbh, 'archived_limit', '0');
    insertConfigValue($dbh, 'dateexpiry_fallback', 'keep');
    insertConfigValue($dbh, 'dateexpiry_months', '12');
    insertConfigValue($dbh, 'match_methods', 'userid,cardnumber');
    insertConfigValue($dbh, 'civic_attribute_code', '');
    insertConfigValue($dbh, 'class_attribute_code', 'CL');
    insertConfigValue($dbh, 'email_suffix', '.NOEMAIL');
    insertConfigValue($dbh, 'email_suffix_enabled', 'No');
    insertConfigValue($dbh, 'keep_existing_category', 'No');
    insertConfigValue($dbh, 'keep_existing_contact', 'No');
    insertConfigValue($dbh, 'use_rules', 'No');
    insertConfigValue($dbh, 'leaver_mode', 'off');
    insertConfigValue($dbh, 'leaver_delete_days', '30');
    insertConfigValue($dbh, 'leaver_anonymize_days_loans', '300');
    insertConfigValue($dbh, 'leaver_max_changes', '200');
    insertConfigValue($dbh, 'leaver_expire_now', 'Yes');
    ensure_tables($dbh);

    return $self->log_page($session_data) if ($op // '') eq 'log';
    return $self->log_ajax($op)           if ($op // '') =~ /^ajax-log-/;

    # Koha requires every POST op to start with cud-; the name used by the handler is without it
    if (($op // '') =~ /^(?:cud-)?ajax-/) {
        $self->{session_data} = $session_data;
        (my $ajax_op = $op) =~ s/^cud-//;
        return $self->configure_ajax($ajax_op, $cgi);
    }

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
        $template->param(ss => $self->ss_sub);
        $template->param(
            error    => "missing_modules",
            language => C4::Languages::getlanguage($cgi) || 'en',
        );

        print $cgi->header(-type => 'text/html', -charset => 'utf-8');
        print $template->output();
        return 0;
    }

    my $template = $self->get_template({ file => 'config.tt' });
    $template->param(ss => $self->ss_sub);

    $template->param(success => $self->{'success'}) if $self->{'success'};
    $template->param(error => $self->{'error'}) if $self->{'error'};
    my $count_log_query = "SELECT COUNT(*) FROM imcode_logs WHERE DATE(created_at) = CURDATE() AND is_processed = 1";
    my ($log_count) = $dbh->selectrow_array($count_log_query);
    $template->param(log_count => $log_count);

    if ($op eq 'cud-save-config') {
        my $trim_param = sub { my $v = $_[0] // ''; $v =~ s/^\s+|\s+$//g; $v };
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

        my $branch_mappings_json   = $cgi->param('branch_mappings_json');
        my $category_mappings_json = $cgi->param('category_mappings_json');
        my $rules_json             = $cgi->param('rules_json');
        my $schedule_json          = $cgi->param('schedule_json');
        my $leaver_mapping_json    = $cgi->param('leaver_mapping_json');
        my $leaver_mode            = SS12000Leavers::normalize_mode($cgi->param('leaver_mode'));
        my $leaver_delete_days     = int($cgi->param('leaver_delete_days') // 30);
        $leaver_delete_days = 0 if $leaver_delete_days < 0; $leaver_delete_days = 3650 if $leaver_delete_days > 3650;
        my $leaver_anon_raw        = $cgi->param('leaver_anonymize_days_loans') // '';
        $leaver_anon_raw =~ s/^\s+|\s+$//g;
        my $leaver_anonymize_days_loans = $leaver_anon_raw =~ /^\d+$/ ? ($leaver_anon_raw > 3650 ? 3650 : $leaver_anon_raw + 0) : '';
        my $leaver_max_changes     = int($cgi->param('leaver_max_changes') // 200);
        $leaver_max_changes = 1 if $leaver_max_changes < 1; $leaver_max_changes = 100000 if $leaver_max_changes > 100000;
        my $leaver_expire_now      = $cgi->param('leaver_expire_now') ? 'Yes' : 'No';

        my @match_methods        = grep { /^(?:attribute|userid|cardnumber)$/ } $cgi->multi_param('match_methods');
        my $civic_attribute_code = $cgi->param('civic_attribute_code') // '';
        my $class_attribute_code = $cgi->param('class_attribute_code') || 'CL';
        my $email_suffix         = $trim_param->($cgi->param('email_suffix'));
        my $email_suffix_enabled = $cgi->param('email_suffix_enabled') ? 'Yes' : 'No';
        my $keep_existing_category = $cgi->param('keep_existing_category') ? 'Yes' : 'No';
        my $keep_existing_contact  = $cgi->param('keep_existing_contact')  ? 'Yes' : 'No';
        my $use_rules              = $cgi->param('use_rules') ? 'Yes' : 'No';
        $useridPlugin     = 'civicNo' unless defined $useridPlugin     && $useridPlugin     =~ /^(?:civicNo|externalIdentifier|email_local|email|keep)$/;
        $cardnumberPlugin = 'civicNo' unless defined $cardnumberPlugin && $cardnumberPlugin =~ /^(?:civicNo|externalIdentifier|keep)$/;

        my $excluding_dutyRole_empty = $cgi->param('excluding_dutyRole_empty');
        my $excluding_enrolments_empty = $cgi->param('excluding_enrolments_empty');
        my $ignore_cancelled_flag = $cgi->param('ignore_cancelled_flag');
        my $use_default_for_unmapped = $cgi->param('use_default_for_unmapped');
        my $dateexpiry_fallback  = $cgi->param('dateexpiry_fallback') || 'keep';
        my $dateexpiry_months    = int($cgi->param('dateexpiry_months') || 12);

        # Mappings are saved as complete lists, built as JSON by the configuration page.
        my %valid_branch   = map { $_ => 1 } @{ $dbh->selectcol_arrayref("SELECT branchcode FROM $branches_table") || [] };
        my %valid_category = map { $_ => 1 } @{ $dbh->selectcol_arrayref("SELECT categorycode FROM $categories_table") || [] };
        my %valid_attr     = map { $_ => 1 } @{ $dbh->selectcol_arrayref("SELECT code FROM borrower_attribute_types") || [] };
        my @save_errors;
        my $trim = sub { my $v = $_[0] // ''; $v =~ s/^\s+|\s+$//g; $v };

        if (defined $branch_mappings_json && $branch_mappings_json ne '') {
            my $rows = eval { decode_json_param($branch_mappings_json) };
            if (ref $rows eq 'ARRAY') {
                my (%seen, @clean);
                foreach my $r (grep { ref $_ eq 'HASH' } @$rows) {
                    my ($org, $branch) = ($trim->($r->{org}), $trim->($r->{branch}));
                    next if $org eq '' || $branch eq '' || $seen{$org}++;
                    unless ($valid_branch{$branch}) {
                        log_message("Yes", "Branch mapping skipped, unknown Koha branch '$branch' for organisation '$org'");
                        push @save_errors, $self->t("Unknown Koha branch '%s' (organisation %s) was not saved", $branch, $org);
                        next;
                    }
                    push @clean, [$org, $branch];
                }
                eval {
                    my $own_tx = $dbh->{AutoCommit} ? 1 : 0;
                    $dbh->begin_work if $own_tx;
                    $dbh->do("DELETE FROM $branches_mapping_table");
                    $dbh->do("INSERT INTO $branches_mapping_table (organisationCode, branchcode) VALUES (?, ?)", undef, $_->[0], $_->[1]) for @clean;
                    $dbh->commit if $own_tx;
                    log_message("Yes", "Saved " . scalar(@clean) . " branch mappings");
                };
                if ($@) {
                    my $err = $@;
                    eval { $dbh->rollback };
                    log_message("Yes", "Error saving branch mappings: $err");
                    push @save_errors, $self->t('Branch mappings could not be saved: %s', $err);
                }
            } else {
                push @save_errors, $self->t('Branch mappings had an invalid format and were not saved');
            }
        }

        if (defined $category_mappings_json && $category_mappings_json ne '') {
            my $rows = eval { decode_json_param($category_mappings_json) };
            if (ref $rows eq 'ARRAY') {
                my (%seen, @clean);
                foreach my $r (grep { ref $_ eq 'HASH' } @$rows) {
                    my ($role, $category) = ($trim->($r->{role}), $trim->($r->{category}));
                    # A role that is not imported never uses its category, so such a row is kept as it is
                    # even when the category is empty or no longer exists in Koha
                    my $skip_import = $r->{not_import} ? 1 : 0;
                    next if $role eq '' || ($category eq '' && !$skip_import) || $seen{"$category\t$role"}++;
                    unless ($skip_import || $valid_category{$category}) {
                        log_message("Yes", "Category mapping skipped, unknown Koha category '$category' for dutyRole '$role'");
                        push @save_errors, $self->t("Unknown Koha category '%s' (dutyRole %s) was not saved", $category, $role);
                        next;
                    }
                    push @clean, [$category, $role, ($skip_import ? 1 : undef)];
                }
                eval {
                    my $own_tx = $dbh->{AutoCommit} ? 1 : 0;
                    $dbh->begin_work if $own_tx;
                    $dbh->do("DELETE FROM $categories_mapping_table");
                    $dbh->do("INSERT INTO $categories_mapping_table (categorycode, dutyRole, not_import) VALUES (?, ?, ?)", undef, @$_) for @clean;
                    $dbh->commit if $own_tx;
                    log_message("Yes", "Saved " . scalar(@clean) . " category mappings");
                };
                if ($@) {
                    my $err = $@;
                    eval { $dbh->rollback };
                    log_message("Yes", "Error saving category mappings: $err");
                    push @save_errors, $self->t('Category mappings could not be saved: %s', $err);
                }
            } else {
                push @save_errors, $self->t('Category mappings had an invalid format and were not saved');
            }
        }

        # Rules are saved as one list; if any rule is invalid nothing is changed and every problem is reported
        if (defined $rules_json && $rules_json ne '') {
            my $raw_rules = eval { decode_json_param($rules_json) };
            if (ref $raw_rules eq 'ARRAY') {
                my ($rules, $rule_errors) = SS12000Rules::normalize_rules($raw_rules);
                if (@$rule_errors) {
                    push @save_errors, $self->t('Rules were not saved: %s', $_) for @$rule_errors;
                } else {
                    my $err = save_rules($dbh, $rules);
                    if ($err) {
                        log_message("Yes", "Error saving rules: $err");
                        push @save_errors, $self->t('Rules could not be saved: %s', $err);
                    } else {
                        log_message("Yes", "Saved " . scalar(@$rules) . " rules");
                    }
                }
            } else {
                push @save_errors, $self->t('Rules had an invalid format and were not saved');
            }
        }

        # Which category a patron who has left moves to; saved as one list, all or nothing
        if (defined $leaver_mapping_json && $leaver_mapping_json ne '') {
            my $raw_map = eval { decode_json_param($leaver_mapping_json) };
            if (ref $raw_map eq 'ARRAY') {
                my ($rows, $map_errors) = SS12000Leavers::normalize_mapping($raw_map);
                foreach my $r (@$rows) {
                    push @$map_errors, $self->t("Unknown Koha category '%s'", $_) for grep { defined && !$valid_category{$_} } ($r->{from}, $r->{to}, $r->{to_noloans});
                }
                if (@$map_errors) {
                    push @save_errors, $self->t('Patrons who have left were not saved: %s', $_) for @$map_errors;
                } else {
                    my $ok = eval {
                        my $own_tx = $dbh->{AutoCommit} ? 1 : 0;
                        $dbh->begin_work if $own_tx;
                        $dbh->do("DELETE FROM $leaver_mapping_table");
                        $dbh->do("INSERT INTO $leaver_mapping_table (from_category, to_category, to_noloans) VALUES (?, ?, ?)", undef, $_->{from}, $_->{to}, $_->{to_noloans}) for @$rows;
                        $dbh->commit if $own_tx;
                        1;
                    };
                    unless ($ok) { my $e = $@; eval { $dbh->rollback }; push @save_errors, $self->t('Patrons who have left could not be saved: %s', $e) }
                }
            } else {
                push @save_errors, $self->t('Patrons who have left had an invalid format and were not saved');
            }
        }

        # The run schedule is saved as one list; if any row is invalid nothing is changed
        if (defined $schedule_json && $schedule_json ne '') {
            my $raw_schedule = eval { decode_json_param($schedule_json) };
            if (ref $raw_schedule eq 'ARRAY') {
                my ($entries, $schedule_errors) = SS12000Schedule::normalize_entries($raw_schedule);
                if (@$schedule_errors) {
                    push @save_errors, $self->t('The schedule was not saved: %s', $_) for @$schedule_errors;
                } else {
                    my $err = save_schedule($dbh, $entries);
                    if ($err) {
                        log_message("Yes", "Error saving schedule: $err");
                        push @save_errors, $self->t('The schedule could not be saved: %s', $err);
                    } else {
                        log_message("Yes", "Saved " . scalar(@$entries) . " schedule rows");
                    }
                }
            } else {
                push @save_errors, $self->t('The schedule had an invalid format and was not saved');
            }
        }

        # Patron matching and attribute settings (validated against Koha before saving)
        my @new_key_errors;
        push @new_key_errors, $self->t("Civic number attribute '%s' does not exist (Administration > Patron attribute types)", $civic_attribute_code)
            if $civic_attribute_code ne '' && !$valid_attr{$civic_attribute_code};
        push @new_key_errors, $self->t("Class attribute '%s' does not exist (Administration > Patron attribute types)", $class_attribute_code)
            if $class_attribute_code ne 'CL' && !$valid_attr{$class_attribute_code};
        push @new_key_errors, $self->t('Matching on a patron attribute needs a civic number attribute to be selected')
            if (grep { $_ eq 'attribute' } @match_methods) && $civic_attribute_code eq '';
        push @new_key_errors, $self->t('The e-mail suffix may only contain letters, digits and . _ - + @ (max 40 characters)')
            if $email_suffix ne '' && $email_suffix !~ /^[A-Za-z0-9._\-+@]{1,40}$/;
        push @new_key_errors, $self->t('Enter the text to append to e-mail addresses, or untick the box')
            if $email_suffix_enabled eq 'Yes' && $email_suffix eq '';
        push @new_key_errors, $self->t('Select at least one way to find existing patrons')
            unless @match_methods;
        if (@new_key_errors) {
            push @save_errors, map { $self->t('%s - the patron identity settings (matching, attributes, e-mail text) were not saved', $_) } @new_key_errors;
        } else {
            my $upd = $dbh->prepare("UPDATE $config_table SET value = ? WHERE name = ?");
            $upd->execute(join(',', @match_methods), 'match_methods');
            $upd->execute($civic_attribute_code,    'civic_attribute_code');
            $upd->execute($class_attribute_code,    'class_attribute_code');
            $upd->execute($email_suffix,            'email_suffix');
            $upd->execute($email_suffix_enabled,    'email_suffix_enabled');
        }
        {
            my $upd2 = $dbh->prepare("UPDATE $config_table SET value = ? WHERE name = ?");
            $upd2->execute($keep_existing_category, 'keep_existing_category');
            $upd2->execute($keep_existing_contact,  'keep_existing_contact');
            $upd2->execute($use_rules,              'use_rules');
            $upd2->execute($leaver_mode,            'leaver_mode');
            $upd2->execute($leaver_delete_days,     'leaver_delete_days');
            $upd2->execute($leaver_anonymize_days_loans, 'leaver_anonymize_days_loans');
            $upd2->execute($leaver_max_changes,     'leaver_max_changes');
            $upd2->execute($leaver_expire_now,      'leaver_expire_now');
        }
        $template->param(error => join('; ', @save_errors)) if @save_errors;

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
    my (@branches_full, @categories_full, @attribute_types);

    eval {
        @branches_full   = map { { code => $_->[0], name => $_->[1] // '' } }
                           @{ $dbh->selectall_arrayref("SELECT branchcode, branchname FROM $branches_table ORDER BY branchname, branchcode") };
        @categories_full = map { { code => $_->[0], name => $_->[1] // '' } }
                           @{ $dbh->selectall_arrayref("SELECT categorycode, description FROM $categories_table ORDER BY categorycode") };
        @attribute_types = map { { code => $_->[0], name => $_->[1] // '' } }
                           @{ $dbh->selectall_arrayref("SELECT code, description FROM borrower_attribute_types ORDER BY code") };

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

    my %match_set = map { $_ => 1 } split /\s*,\s*/, ($config_data->{match_methods} || 'userid,cardnumber');

    $template->param(
        plugin_version      => $VERSION,
        logo_data_uri       => logo_data_uri(),
        client_secret_set   => (length($config_data->{ist_client_secret} // '') ? 1 : 0),
        branches_full       => \@branches_full,
        categories_full     => \@categories_full,
        attribute_types     => \@attribute_types,
        branches_json       => json_for_html(\@branches_full),
        categories_json     => json_for_html(\@categories_full),
        branches_mapping_json   => json_for_html([ map { { org => $_->{organisationCode}, branch => $_->{branchcode} } } @branches_mapping ]),
        categories_mapping_json => json_for_html([ map { { role => $_->{dutyRole}, category => $_->{categorycode}, not_import => ($_->{not_import} ? 1 : 0) } } @categories_mapping ]),
        match_attribute     => ($match_set{attribute}  ? 1 : 0),
        match_userid        => ($match_set{userid}     ? 1 : 0),
        match_cardnumber    => ($match_set{cardnumber} ? 1 : 0),
        civic_attribute_code => $config_data->{civic_attribute_code} // '',
        class_attribute_code => $config_data->{class_attribute_code} || 'CL',
        email_suffix         => $config_data->{email_suffix} // '.NOEMAIL',
        email_suffix_enabled => (($config_data->{email_suffix_enabled} // 'No') eq 'Yes' ? 1 : 0),
        keep_existing_category => (($config_data->{keep_existing_category} // 'No') eq 'Yes' ? 1 : 0),
        keep_existing_contact  => (($config_data->{keep_existing_contact}  // 'No') eq 'Yes' ? 1 : 0),
        use_rules              => (($config_data->{use_rules} // 'No') eq 'Yes' ? 1 : 0),
        rules_json             => json_for_html([ SS12000Rules::sort_rules(@{ load_rules($dbh) }) ]),
        rule_meta_json         => json_for_html($self->rule_meta),
        exclude_dutyrole_empty => (($config_data->{excluding_dutyRole_empty} // 'No') eq 'Yes' ? 1 : 0),
        schedule_json          => json_for_html(load_schedule($dbh)),
        leaver_mode            => SS12000Leavers::normalize_mode($config_data->{leaver_mode}),
        leaver_delete_days     => int($config_data->{leaver_delete_days} // 30),
        leaver_anonymize_days_loans => ($config_data->{leaver_anonymize_days_loans} // ''),
        leaver_max_changes     => int($config_data->{leaver_max_changes} || 200),
        leaver_expire_now      => (($config_data->{leaver_expire_now} // 'Yes') eq 'Yes' ? 1 : 0),
        leaver_mapping_json    => json_for_html(leaver_mapping_rows($dbh)),
        leaver_last            => (eval { (state_get($dbh, 'leavers_last'))[0] } // ''),
        leaver_waiting         => scalar(eval { (leaver_candidates($dbh))[0] } // 0),
        schedule_meta_json     => json_for_html($self->schedule_meta),
        jobs_json              => json_for_html(job_status($dbh)),
        client_id     => $config_data->{ist_client_id} || '',
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


# Shown under Administration > Plugins: opens the same page as configure
sub admin {
    my ($self, $args) = @_;
    return $self->configure($args);
}

# Decode JSON received in a form field. Koha normally hands over decoded characters
# (CGI -utf8), but raw UTF-8 bytes are handled too.
# Field and operator names for the rule editor (texts are translated)
sub rule_meta {
    my ($self) = @_;
    my %label = (
        school          => $self->t('School (organisation code)'),
        school_type     => $self->t('School type'),
        school_year     => $self->t('School year'),
        education_code  => $self->t('Education code (programme)'),
        class           => $self->t('Class'),
        role            => $self->t('Role (dutyRole)'),
        assignment_role => $self->t('Assignment role'),
        age             => $self->t('Age'),
        person_kind     => $self->t('Person kind (student, staff, both)'),
        school_group    => $self->t('School group (parent organisation id)'),
        duty_school     => $self->t('School of the duty (staff)'),
    );
    my %advanced = map { $_ => 1 } qw(school_group duty_school);
    my %op_label = (
        in          => $self->t('is one of'),
        starts_with => $self->t('starts with'),
        contains    => $self->t('contains'),
        empty       => $self->t('is empty'),
        not_empty   => $self->t('is not empty'),
        gte         => $self->t('is at least'),
        lte         => $self->t('is at most'),
    );
    my %operators;
    foreach my $type (qw(text number any)) {
        my @ops = SS12000Rules::operators_for($type eq 'any' ? 'custom:x' : ($type eq 'number' ? 'age' : 'class'));
        $operators{$type} = [ map { { code => $_, label => $op_label{$_} } } @ops ];
    }
    return {
        fields    => [ map { { code => $_, label => $label{$_}, type => SS12000Rules::field_type($_), advanced => ($advanced{$_} ? 1 : 0) } }
                       qw(school school_type school_year education_code class role assignment_role age person_kind school_group duty_school) ],
        operators => \%operators,
    };
}

# Replaces the saved rules with the given normalised rules. Rules keep their id when they have one,
# so rule numbers in the rule log stay valid. Returns an error text or undef.
sub save_rules {
    my ($dbh, $rules) = @_;
    my $own_tx = $dbh->{AutoCommit} ? 1 : 0;
    my $ok = eval {
        $dbh->begin_work if $own_tx;
        my $existing = $dbh->selectcol_arrayref("SELECT id FROM $rules_table") || [];
        my %have = map { $_ => 1 } @$existing;
        my %keep = map { $_->{id} => 1 } grep { defined $_->{id} && $have{ $_->{id} } } @$rules;
        $dbh->do("DELETE FROM $rules_table WHERE id = ?", undef, $_) for grep { !$keep{$_} } @$existing;
        my %used;
        foreach my $r (@$rules) {
            my @vals = ($r->{priority}, $r->{name}, $r->{enabled}, $r->{scope}, SS12000Rules::conditions_to_json($r->{conditions}),
                        $r->{action}{category}, $r->{action}{branch}, ($r->{action}{not_import} ? 1 : 0));
            if (defined $r->{id} && $have{ $r->{id} } && !$used{ $r->{id} }++) {
                $dbh->do("UPDATE $rules_table SET priority = ?, name = ?, enabled = ?, scope = ?, conditions_json = ?, action_category = ?, action_branch = ?, not_import = ? WHERE id = ?",
                         undef, @vals, $r->{id});
            } else {
                $dbh->do("INSERT INTO $rules_table (priority, name, enabled, scope, conditions_json, action_category, action_branch, not_import) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                         undef, @vals);
            }
        }
        $dbh->commit if $own_tx;
        1;
    };
    return undef if $ok;
    my $err = $@ || 'unknown error';
    eval { $dbh->rollback };
    return $err;
}

sub leaver_mapping_rows {
    my ($dbh) = @_;
    my $rows = eval { $dbh->selectall_arrayref("SELECT from_category, to_category, to_noloans FROM $leaver_mapping_table ORDER BY id") } || [];
    return [ map { +{ from => $_->[0], to => $_->[1], to_noloans => ($_->[2] // '') } } @$rows ];
}

sub state_get {
    my ($dbh, $name) = @_;
    return $dbh->selectrow_array("SELECT value, updated_at FROM $state_table WHERE name = ?", undef, $name);
}

# Patrons that the import has marked as no longer in the school register: not in IST, or without a valid
# enrolment (the notes are written by the import and by mark_missing_users_from_api)
my $LEAVER_NOTE_SQL = q{b.opacnote LIKE '%by SS12000:%' AND (
        b.opacnote LIKE '%No update: user no longer in API%'
     OR b.opacnote LIKE '%No update: enrolment cancelled%'
     OR b.opacnote LIKE '%No update: dateexpiry exceeded%'
     OR b.opacnote LIKE '%No update: no valid enrolment%'
     OR b.opacnote LIKE '%No update: no enrolments in API%')};

# Returns (count, \@borrowernumbers) of patrons waiting to be handled
sub leaver_candidates {
    my ($dbh) = @_;
    my $rows = $dbh->selectcol_arrayref("SELECT b.borrowernumber FROM $borrowers_table b WHERE $LEAVER_NOTE_SQL ORDER BY b.borrowernumber") || [];
    return (scalar(@$rows), $rows);
}

# Removes everything that identifies a patron outside the patron record itself: Koha's action log (BorrowersLog
# keeps the whole record), the plugin's change log (the trigger writes old and new values), the rule log and
# anything left in deletedborrowers.
sub leaver_purge_traces {
    my ($dbh, $bn) = @_;
    $dbh->do("DELETE FROM action_logs WHERE module = 'MEMBERS' AND object = ?", undef, $bn);
    $dbh->do("DELETE FROM $data_change_log_table WHERE table_name = 'borrowers' AND record_id = ?", undef, $bn);
    $dbh->do("DELETE FROM $rule_log_table WHERE borrowernumber = ?", undef, $bn);
    $dbh->do("DELETE FROM deletedborrowers WHERE borrowernumber = ?", undef, $bn);
    $dbh->do("DELETE FROM message_queue WHERE borrowernumber = ?", undef, $bn);
    $dbh->do("DELETE FROM borrower_modifications WHERE borrowernumber = ?", undef, $bn);
}

# Removes a patron through Koha (which refuses open loans, fines and guarantees). The record is NOT copied to
# deletedborrowers, the checkout history is anonymized and every trace is purged. Returns 1 when removed.
sub leaver_remove_patron {
    my ($dbh, $bn, $days, $category) = @_;
    my $removed = eval {
        my $patron = Koha::Patrons->find($bn);
        return 0 unless $patron && $patron->safe_to_delete;
        $patron->old_checkouts->anonymize;
        $patron->delete;
        1;
    };
    log_message('Yes', "Leavers: removing patron $bn failed: $@") if $@;
    return 0 unless $removed;
    leaver_purge_traces($dbh, $bn);
    $dbh->do("DELETE FROM $leavers_table WHERE borrowernumber = ?", undef, $bn);
    $dbh->do("INSERT INTO $data_change_log_table (table_name, record_id, action, change_description) VALUES ('borrowers', ?, 'leaver_deleted', ?)",
             undef, $bn, "removed after $days days as a leaver, category was $category");
    return 1;
}

# Wipes everything personal from a patron that cannot be removed yet (open loans), but keeps the account so the
# loans can still be followed up: name, contact details, birth date, notes, attributes (civic number, class),
# card number and the traces in the logs. The user id becomes anon<borrowernumber>.
sub leaver_anonymize_patron {
    my ($dbh, $bn, $to_category, $expire) = @_;
    my %keep = map { $_ => 1 } qw(borrowernumber categorycode branchcode surname userid cardnumber password opacnote dateenrolled dateexpiry
        date_renewed flags lang login_attempts anonymized auth_method updated_on lastseen debarred debarredcomment checkprevcheckout
        privacy privacy_guarantor_fines privacy_guarantor_checkouts);
    my $cols = $dbh->selectall_arrayref("SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'borrowers'");
    my (@set, @vals);
    foreach my $c (@$cols) {
        my ($name, $nullable, $type) = @$c;
        next if $keep{ lc $name };
        my $null = ($nullable eq 'YES');
        if    ($type =~ /char|text/)                { push @set, "`$name` = ?"; push @vals, ($null ? undef : '') }
        elsif ($type =~ /int|dec|float|double/)     { push @set, "`$name` = ?"; push @vals, ($null ? undef : 0) }
        elsif ($type =~ /date|time/ && $null)       { push @set, "`$name` = NULL" }
        elsif ($type eq 'enum' && $null)            { push @set, "`$name` = NULL" }
    }
    push @set, "surname = 'Anonymiserad'", "userid = ?", "cardnumber = NULL", "password = NULL", "anonymized = 1", "opacnote = ?";
    push @vals, "anon$bn", "Anonymized by SS12000: plugin $version_info at " . strftime('%Y-%m-%d %H:%M:%S', localtime) . " No update: user no longer in API";
    if (defined $to_category && $to_category ne '') { push @set, "categorycode = ?"; push @vals, $to_category }
    push @set, "dateexpiry = IF(dateexpiry IS NULL OR dateexpiry > CURDATE(), CURDATE(), dateexpiry)" if $expire;
    $dbh->do("UPDATE $borrowers_table SET " . join(', ', @set) . " WHERE borrowernumber = ?", undef, @vals, $bn);
    $dbh->do("DELETE FROM borrower_attributes WHERE borrowernumber = ?", undef, $bn);
    eval { my $p = Koha::Patrons->find($bn); $p->old_checkouts->anonymize if $p; 1 } or log_message('Yes', "Leavers: anonymizing the checkout history of $bn failed: $@");
    leaver_purge_traces($dbh, $bn);
    $dbh->do("UPDATE $leavers_table SET anonymized_at = NOW() WHERE borrowernumber = ?", undef, $bn);
    $dbh->do("INSERT INTO $data_change_log_table (table_name, record_id, action, change_description) VALUES ('borrowers', ?, 'leaver_anonymized', 'all personal data removed, the account is kept for the loans that are left')", undef, $bn);
}

# Patrons who have left. Per category (mapping) they move to a category while loans are left, and to another one
# (or stay) while they have no loans; the account can expire. Without loans they are removed after
# leaver_delete_days; with loans left the account is anonymized after leaver_anonymize_days_loans (a patron with
# open loans cannot be removed) and removed later when the loans are back. Everything personal is removed with
# the patron. Only categories in the mapping are touched. leaver_mode: off | report | apply.
sub process_leavers {
    my ($self, $dbh, $config, $only) = @_;    # $only: optional list of borrowernumbers (used by tests)
    my $mode = SS12000Leavers::normalize_mode($config->{leaver_mode});
    return if $mode eq 'off';

    # Patrons who are back in the register (touched by today's import without "No update") are no longer leavers
    $dbh->do(qq{DELETE FROM $leavers_table WHERE borrowernumber IN (
        SELECT borrowernumber FROM $borrowers_table WHERE opacnote LIKE CONCAT('%', CURDATE(), '%') AND opacnote NOT LIKE '%No update%')});

    my %mapping = map { $_->[0] => { loans => $_->[1], noloans => $_->[2] } }
        @{ $dbh->selectall_arrayref("SELECT from_category, to_category, to_noloans FROM $leaver_mapping_table") || [] };
    unless (%mapping) {
        log_message('Yes', 'Leavers: the mapping is empty, nothing to do');
        return;
    }
    my $alimit = $config->{leaver_anonymize_days_loans};
    my %opts   = (delete_after_days => int($config->{leaver_delete_days} // 30),
                  anonymize_after_days_loans => (defined $alimit && $alimit =~ /^\d+$/ ? $alimit + 0 : undef));
    my $max    = int($config->{leaver_max_changes} || 200);
    my $expire = (($config->{leaver_expire_now} // 'Yes') eq 'Yes');
    my (undef, $candidates) = leaver_candidates($dbh);
    if ($only) { my %want = map { $_ => 1 } @$only; $candidates = [ grep { $want{$_} } @$candidates ]; }

    my (%count, $changes, $capped);
    $changes = 0;
    foreach my $bn (@$candidates) {
        my $b = $dbh->selectrow_hashref("SELECT borrowernumber, categorycode, dateexpiry FROM $borrowers_table WHERE borrowernumber = ?", undef, $bn) or next;
        $dbh->do("INSERT IGNORE INTO $leavers_table (borrowernumber, first_seen, reason, previous_category) VALUES (?, NOW(), 'not in IST', ?)", undef, $bn, $b->{categorycode});
        my ($days, $prev, $anon) = $dbh->selectrow_array("SELECT TIMESTAMPDIFF(DAY, first_seen, NOW()), previous_category, anonymized_at IS NOT NULL FROM $leavers_table WHERE borrowernumber = ?", undef, $bn);
        my ($loans) = $dbh->selectrow_array("SELECT COUNT(*) FROM issues WHERE borrowernumber = ?", undef, $bn);
        my $d = SS12000Leavers::decide({ category => $b->{categorycode}, previous_category => $prev, has_loans => ($loans ? 1 : 0), days => $days // 0, anonymized => ($anon ? 1 : 0) }, \%mapping, \%opts);
        my $action = $d->{action};
        my $entry  = $mapping{ $prev // $b->{categorycode} } || $mapping{ $b->{categorycode} } || {};
        my $want   = $loans ? $entry->{loans} : $entry->{noloans};

        if ($mode eq 'apply' && ($action eq 'move' || $action eq 'delete' || $action eq 'anonymize')) {
            if ($changes >= $max) { $capped = 1; $count{waiting_for_next_run}++; next }
            if ($action eq 'delete') {
                if (leaver_remove_patron($dbh, $bn, $days, $b->{categorycode})) { $changes++; $count{deleted}++; next }
                $count{blocked_by_fines_or_guarantees}++;
                $action = (defined $want && $want ne $b->{categorycode}) ? 'move' : 'keep';
                $d = { action => $action, to => $want };
            }
            if ($action eq 'anonymize') {
                leaver_anonymize_patron($dbh, $bn, $want, $expire);
                $changes++; $count{anonymized}++;
                next;
            }
            if ($action eq 'move') {
                $dbh->do("UPDATE $borrowers_table SET categorycode = ?" . ($expire ? ", dateexpiry = IF(dateexpiry IS NULL OR dateexpiry > CURDATE(), CURDATE(), dateexpiry)" : '') . " WHERE borrowernumber = ?",
                         undef, $d->{to}, $bn);
                $dbh->do("INSERT INTO $data_change_log_table (table_name, record_id, action, change_description) VALUES ('borrowers', ?, 'leaver_moved', ?)",
                         undef, $bn, "left the school register: category $b->{categorycode} to $d->{to}" . ($loans ? ", $loans loan(s) left" : '') . ($expire ? ', account expires' : ''));
                $changes++; $count{moved}++;
                next;
            }
        }
        $count{ $mode eq 'report' && $action =~ /^(?:move|delete|anonymize)$/ ? "would_$action" : $action }++;
    }

    my $summary = join(', ', map { "$_ $count{$_}" } sort keys %count) || 'no patrons';
    $summary .= ' (limit per run reached)' if $capped;
    my $line = strftime('%Y-%m-%d %H:%M', localtime) . " $mode: $summary";
    state_set($dbh, 'leavers_last', substr($line, 0, 255));
    log_message('Yes', "Leavers ($mode): $summary");
}

# POST with a valid CSRF token for the current session
sub verify_csrf {
    my ($self, $cgi) = @_;
    return 0 unless ($cgi->request_method // '') eq 'POST';
    my $token = $cgi->param('csrf_token') // '';
    my $session_id = $cgi->cookie('CGISESSID') // '';
    return 0 unless length $token && length $session_id;
    return Koha::Token->new->check_csrf({ session_id => $session_id, token => $token }) ? 1 : 0;
}

sub schedule_meta {
    my ($self) = @_;
    my @names = ($self->t('Monday'), $self->t('Tuesday'), $self->t('Wednesday'), $self->t('Thursday'), $self->t('Friday'), $self->t('Saturday'), $self->t('Sunday'));
    return {
        weekdays => [ map { { code => $_ + 1, label => $names[$_] } } 0 .. 6 ],
        types    => { import => $self->t('Import of patrons'), discover => $self->t('Fetch values from IST') },
        sources  => { manual => $self->t('Started by hand'), schedule => $self->t('Schedule') },
        messages => { 'time-limit-8h' => $self->t('Stopped: the time limit of 8 hours was reached'), 'configuration-error-see-log' => $self->t('Stopped: configuration error, see the log'), 'stopped-before-the-last-page' => $self->t('Stopped before the last page, see the log') },
        storedFormat => $self->t('stored %s values, %s persons read'),
        states   => { queued => $self->t('Waiting'), running => $self->t('Running'), done => $self->t('Done'), failed => $self->t('Failed') },
    };
}

sub load_schedule {
    my ($dbh) = @_;
    my $rows = eval { $dbh->selectall_arrayref("SELECT weekday, run_time, job_type FROM $schedule_table ORDER BY weekday, run_time, job_type", { Slice => {} }) } || [];
    return [ map { { weekday => $_->{weekday} + 0, time => $_->{run_time}, job_type => $_->{job_type} } } @$rows ];
}

# Replaces the saved schedule with the given normalised entries. Returns an error text or undef.
sub save_schedule {
    my ($dbh, $entries) = @_;
    my $own_tx = $dbh->{AutoCommit} ? 1 : 0;
    my $ok = eval {
        $dbh->begin_work if $own_tx;
        $dbh->do("DELETE FROM $schedule_table");
        $dbh->do("INSERT INTO $schedule_table (weekday, run_time, job_type) VALUES (?, ?, ?)", undef, $_->{weekday}, $_->{time}, $_->{job_type}) for @$entries;
        $dbh->commit if $own_tx;
        1;
    };
    return undef if $ok;
    my $err = $@ || 'unknown error';
    eval { $dbh->rollback };
    return $err;
}

# Creates the version 2 tables when they are missing (for example when the plugin files were copied
# over a 2.0 installation, where Koha does not run upgrade again)
sub ensure_tables {
    my ($dbh) = @_;
    eval { $dbh->do($_) for @rules_ddl; 1 } or log_message('Yes', "ensure_tables failed: $@");
}

sub state_set {
    my ($dbh, $name, $value) = @_;
    $dbh->do("INSERT INTO $state_table (name, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW()", undef, $name, $value);
}

# Is an import running right now, and who started it? Looks at the shared lock file (held while one page is
# processed), the job queue, the processes of the cron wrapper and the latest activity in imcode_logs.
sub run_activity {
    my ($dbh) = @_;
    my @sources;

    my $lock = eval { Koha::Plugin::imCode::KohaSS12000::ExportUsers->new->get_lock_info };
    push @sources, 'lock:' . ($lock->{source} // '?') if ref $lock eq 'HASH' && $lock->{pid} && kill(0, $lock->{pid});

    my ($job) = eval { $dbh->selectrow_array("SELECT CONCAT(job_type, ':', source) FROM $jobs_table WHERE status = 'running' ORDER BY id LIMIT 1") };
    push @sources, "job:$job" if $job;

    push @sources, 'process' if system('pgrep -f "[r]un_ss12000\\.sh|[i]mcode_ss12000\\.pl" >/dev/null 2>&1') == 0;

    my ($last, $age) = eval { $dbh->selectrow_array("SELECT DATE_FORMAT(MAX(created_at), '%Y-%m-%d %H:%i'), TIMESTAMPDIFF(MINUTE, MAX(created_at), NOW()) FROM $logs_table WHERE data_endpoint = 'persons'") };
    push @sources, 'log' if defined $age && $age <= 10;

    return { running => (@sources ? 1 : 0), sources => \@sources, last_log => $last // '', last_log_age => defined $age ? $age + 0 : undef };
}

# Latest jobs and the cron heartbeat for the configuration page
sub job_status {
    my ($dbh) = @_;
    my $jobs = eval {
        $dbh->selectall_arrayref(
            "SELECT id, job_type, source, status, requested_by, requested_at, started_at, finished_at, message FROM $jobs_table ORDER BY id DESC LIMIT 8",
            { Slice => {} });
    } || [];
    my ($seen, $age) = eval { $dbh->selectrow_array("SELECT value, TIMESTAMPDIFF(SECOND, updated_at, NOW()) FROM $state_table WHERE name = 'tick_last_seen'") };
    my ($now) = eval { $dbh->selectrow_array("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i') ") };
    my ($values_at) = eval { $dbh->selectrow_array("SELECT DATE_FORMAT(MAX(last_seen), '%Y-%m-%d %H:%i') FROM $seen_values_table") };
    return {
        jobs        => [ map { my $j = $_; +{ map { $_ => (defined $j->{$_} ? "$j->{$_}" : '') } keys %$j } } @$jobs ],
        tick_seen   => $seen // '',
        tick_age    => defined $age ? $age + 0 : undef,
        server_time => $now // '',
        values_at   => $values_at // '',
        activity    => run_activity($dbh),
    };
}

# Queues a job unless one of the same type is already waiting or running
sub request_job {
    my ($dbh, $type, $by, $source) = @_;
    return { ok => 0 } unless defined $type && $type =~ /^(?:import|discover)$/;
    my ($open) = $dbh->selectrow_array("SELECT id FROM $jobs_table WHERE job_type = ? AND status IN ('queued', 'running') ORDER BY id LIMIT 1", undef, $type);
    return { ok => 1, id => $open, already => 1 } if $open;
    $dbh->do("INSERT INTO $jobs_table (job_type, source, status, requested_by) VALUES (?, ?, 'queued', ?)", undef, $type, ($source || 'manual'), substr($by // '', 0, 100));
    return { ok => 1, id => $dbh->last_insert_id(undef, undef, $jobs_table, undef) };
}

sub finish_job {
    my ($dbh, $id, $status, $message) = @_;
    return unless defined $id && $id =~ /^\d+$/;
    $status = 'failed' unless defined $status && $status =~ /^(?:done|failed)$/;
    $dbh->do("UPDATE $jobs_table SET status = ?, finished_at = NOW(), message = ? WHERE id = ? AND status = 'running'", undef, $status, substr($message // '', 0, 255), $id);
}

sub release_job {
    my ($dbh, $id) = @_;
    return unless defined $id && $id =~ /^\d+$/;
    $dbh->do("UPDATE $jobs_table SET status = 'queued', started_at = NULL WHERE id = ? AND status = 'running'", undef, $id);
}

# Called by cron every minute (cron/imcode_ss12000_tick.pl). Writes the heartbeat, queues the schedule rows
# that are due and starts at most one waiting job. Prints nothing itself; returns one of
#   IDLE | JOB:<id>:import (the shell script runs the import) | DONE:<id>:<status> (a job that ran inside this call)
sub tick {
    my ($self) = @_;
    my $dbh = C4::Context->dbh;

    eval { $dbh->selectrow_array("SELECT 1 FROM $jobs_table LIMIT 1"); 1 } or ensure_tables($dbh);
    state_set($dbh, 'tick_last_seen', strftime('%Y-%m-%d %H:%M:%S', localtime));
    $dbh->do("UPDATE $jobs_table SET status = 'failed', finished_at = NOW(), message = 'stopped without finishing (over 10 hours)' WHERE status = 'running' AND started_at < DATE_SUB(NOW(), INTERVAL 10 HOUR)");
    my ($running) = $dbh->selectrow_array("SELECT id FROM $jobs_table WHERE status = 'running' LIMIT 1");
    return 'IDLE' if $running;

    foreach my $due (SS12000Schedule::due_entries(load_schedule($dbh), time())) {
        $dbh->do("INSERT IGNORE INTO $jobs_table (job_type, source, schedule_key, status, requested_by) VALUES (?, 'schedule', ?, 'queued', 'schedule')",
                 undef, $due->{entry}{job_type}, $due->{key});
    }

    my ($id, $type) = $dbh->selectrow_array("SELECT id, job_type FROM $jobs_table WHERE status = 'queued' ORDER BY id LIMIT 1");
    return 'IDLE' unless $id;
    my $started = $dbh->do("UPDATE $jobs_table SET status = 'running', started_at = NOW() WHERE id = ? AND status = 'queued'", undef, $id);
    return 'IDLE' unless $started && $started > 0;

    if ($type eq 'import') {
        # An import started by an older cron line is still running: wait for it instead of resetting its progress
        if (system('pgrep -f "[r]un_ss12000\\.sh|[i]mcode_ss12000\\.pl" >/dev/null 2>&1') == 0) {
            release_job($dbh, $id);
            log_message('Yes', "Job $id (import) waits: another import is running");
            return 'IDLE';
        }
        $dbh->do("UPDATE $logs_table SET is_processed = 0, updated_count = 0, added_count = 0, page_token_next = 'RESET' WHERE DATE(created_at) = CURDATE()");
        log_message('Yes', "Job $id (import) started");
        return "JOB:$id:import";
    }

    if ($type eq 'discover') {
        log_message('Yes', "Job $id (discover) started");
        my ($ok, $message) = $self->discover_values($dbh);
        finish_job($dbh, $id, ($ok ? 'done' : 'failed'), $message);
        log_message('Yes', "Job $id (discover) finished: $message");
        return "DONE:$id:" . ($ok ? 'done' : 'failed');
    }

    finish_job($dbh, $id, 'failed', "unknown job type $type");
    return 'IDLE';
}

sub tick_cli {
    my ($self, @args) = @_;
    my $dbh = C4::Context->dbh;
    my $cmd = $args[0] // '';
    if ($cmd eq '--finish') {
        finish_job($dbh, $args[1], $args[2], join(' ', @args[3 .. $#args]));
        print "OK\n";
        return 0;
    }
    if ($cmd eq '--release') {
        release_job($dbh, $args[1]);
        print "OK\n";
        return 0;
    }
    my $result = eval { $self->tick };
    if ($@) {
        log_message('Yes', "tick failed: $@");
        print "ERROR\n";
        return 1;
    }
    print "$result\n";
    return 0;
}

# Reads the values IST really contains (schools, school types, years, programmes, classes, roles) and
# stores them for the rule editor. Only fields that are not personal data are stored.
sub discover_values {
    my ($self, $dbh) = @_;
    my $config = $self->get_config_data();
    unless ($config->{ist_client_id} && length($config->{ist_client_secret} // '') && $config->{ist_customer_id}) {
        return (0, 'IST connection is not configured');
    }
    my $token = $self->get_api_token($config, LWP::UserAgent->new);
    return (0, 'could not get an access token') unless $token;
    my $api_base = ($config->{ist_api_url} || '') . '/ss12000v2-api/source/' . ($config->{ist_customer_id} || '') . '/v2.0/';
    my $ignore_cancelled = (($config->{ignore_cancelled_flag} // 'No') eq 'Yes');
    my $today = strftime('%Y-%m-%d', localtime);

    my (%count, %label, %done, @notes);
    my $add = sub { my ($field, $value, $n) = @_; return unless defined $value && !ref $value && length $value; $count{$field}{$value} += ($n // 1) };

    my $each_page = sub {
        my ($endpoint, $params, $cb) = @_;
        my ($page_token, $pages) = (undef, 0);
        while (1) {
            my $url = $api_base . $endpoint . '?limit=100';
            $url .= '&' . uri_escape($_) . '=' . uri_escape($params->{$_}) for sort keys %{ $params || {} };
            $url .= '&pageToken=' . uri_escape($page_token) if $page_token;
            my ($body, $code, $err) = getApiResponse($url, $token);
            return (0, $code // 0) unless defined $body;
            my $data = eval { decode_json($body) };
            return (0, 0) unless ref $data eq 'HASH';
            $cb->($_) for @{ $data->{data} || [] };
            $page_token = $data->{pageToken};
            last if !$page_token || ++$pages >= 2000;
        }
        return (1, 200);
    };

    my %org_code;
    my (%children, %org_types);
    my ($ok, $code) = $each_page->('organisations', {}, sub {
        my ($o) = @_;
        $org_code{ $o->{id} } = $o->{organisationCode} // '';
        $add->('school', $o->{organisationCode});
        $org_types{$_}++ for grep { defined && !ref } @{ ref $o->{schoolTypes} eq 'ARRAY' ? $o->{schoolTypes} : [] };
        my $pid = ref $o->{parentOrganisation} eq 'HASH' ? $o->{parentOrganisation}{id} : undef;
        if ($pid && ($o->{organisationCode} // '') ne '') { push @{ $children{$pid} }, $o->{organisationCode}; }
    });
    return (0, "could not read organisations (HTTP $code)") unless $ok;
    $done{$_} = 1 for qw(school school_group);
    foreach my $pid (keys %children) {
        $count{school_group}{$pid} = scalar @{ $children{$pid} };
        $label{school_group}{$pid} = join(', ', sort @{ $children{$pid} });
    }

    ($ok, $code) = $each_page->('duties', {}, sub {
        my ($d) = @_;
        $add->('role', $d->{dutyRole});
        $add->('assignment_role', $d->{assignmentRole});
    });
    if ($ok) { $done{$_} = 1 for qw(role assignment_role) }
    else { push @notes, ($code == 403 ? 'duties are not available (403)' : "duties could not be read (HTTP $code)") }

    ($ok, $code) = $each_page->('groups', { groupType => 'Klass' }, sub { $add->('class', $_[0]{displayName}) });
    if ($ok) { $done{class} = 1 } else { push @notes, "classes could not be read (HTTP $code)" }

    my $persons = 0;
    ($ok, $code) = $each_page->('persons', {}, sub {
        my ($p) = @_;
        $persons++;
        foreach my $e (@{ ref $p->{enrolments} eq 'ARRAY' ? $p->{enrolments} : [] }) {
            next unless ref $e eq 'HASH' && defined $e->{startDate} && defined $e->{endDate};
            next unless $e->{startDate} le $today && $today le $e->{endDate};
            next if $e->{cancelled} && !$ignore_cancelled;
            $add->('school_type', $e->{schoolType});
            $add->('school_year', $e->{schoolYear});
            $add->('education_code', $e->{educationCode});
        }
    });
    if ($ok) {
        $done{$_} = 1 for qw(school_year education_code school_type);
    } else {
        push @notes, "persons could not be read (HTTP $code)";
        if (%org_types) {
            $count{school_type} = { %org_types };
            $done{school_type} = 1;
        }
    }

    return (0, 'nothing could be read: ' . join('; ', @notes)) unless %done;

    my $stored = 0;
    my $own_tx = $dbh->{AutoCommit} ? 1 : 0;
    my $saved = eval {
        $dbh->begin_work if $own_tx;
        foreach my $field (sort keys %done) {
            $dbh->do("DELETE FROM $seen_values_table WHERE field_name = ?", undef, $field);
            my @values = sort { $count{$field}{$b} <=> $count{$field}{$a} || $a cmp $b } keys %{ $count{$field} || {} };
            splice(@values, 1000) if @values > 1000;
            foreach my $v (@values) {
                # Values that the database collation treats as equal (case, trailing spaces) are added together
                $dbh->do("INSERT INTO $seen_values_table (field_name, value, label, seen_count, last_seen) VALUES (?, ?, ?, ?, NOW()) ON DUPLICATE KEY UPDATE seen_count = seen_count + VALUES(seen_count)",
                         undef, $field, substr($v, 0, 190), (defined $label{$field}{$v} ? substr($label{$field}{$v}, 0, 255) : undef), $count{$field}{$v});
                $stored++;
            }
        }
        $dbh->commit if $own_tx;
        1;
    };
    unless ($saved) {
        my $err = $@ || 'unknown error';
        eval { $dbh->rollback };
        return (0, "could not store the values: $err");
    }
    my $summary = "stored $stored values, $persons persons read";
    $summary .= '; ' . join('; ', @notes) if @notes;
    return (1, $summary);
}

sub decode_json_param {
    my ($text) = @_;
    my $json = JSON->new;
    return utf8::is_utf8($text) ? $json->decode($text) : $json->utf8->decode($text);
}

# ---------------------------------------------------------------------------
# Log viewer (Administration > Plugins > Export Users from SS12000 > Log)
# ---------------------------------------------------------------------------

# State of an imcode_logs row as a SQL expression
our $LOG_STATE_SQL = q{CASE
        WHEN data_endpoint = 'restart_marker' THEN 'restart'
        WHEN page_token_next IS NULL AND is_processed = 1 THEN 'done'
        WHEN page_token_next = 'RESET' THEN 'reset'
        WHEN is_processed = 1 THEN 'more pages'
        ELSE 'pending'
    END};

sub log_tables {
    return {
        runs => {
            table   => $logs_table,
            pk      => 'id',
            cols    => [ 'id', 'created_at', 'organisation_code', 'data_endpoint', 'iteration_number',
                         'added_count', 'updated_count', 'processed_count', $LOG_STATE_SQL, 'LEFT(response, 160)' ],
            search  => "CONCAT_WS(' ', id, created_at, organisation_code, data_endpoint, $LOG_STATE_SQL, LEFT(response, 500))",
            time    => 'created_at',
        },
        changes => {
            table   => $data_change_log_table,
            pk      => 'log_id',
            cols    => [ 'log_id', 'change_timestamp', 'record_id', 'action', 'change_description' ],
            search  => "CONCAT_WS(' ', log_id, record_id, action, change_description, change_timestamp)",
            time    => 'change_timestamp',
        },
    };
}

# Page with the two log tables (data is loaded by log_ajax)
sub log_page {
    my ($self, $session_data) = @_;
    my $cgi = $self->{'cgi'};
    my $dbh = C4::Context->dbh;

    my $template = $self->get_template({ file => 'log.tt' });
    $template->param(ss => $self->ss_sub);

    my $orgs    = $dbh->selectcol_arrayref("SELECT DISTINCT organisation_code FROM $logs_table WHERE organisation_code IS NOT NULL AND organisation_code != '' ORDER BY organisation_code") || [];
    my $actions = $dbh->selectcol_arrayref("SELECT DISTINCT action FROM $data_change_log_table WHERE action IS NOT NULL ORDER BY action") || [];
    my ($runs_total)    = $dbh->selectrow_array("SELECT COUNT(*) FROM $logs_table");
    my ($changes_total) = $dbh->selectrow_array("SELECT COUNT(*) FROM $data_change_log_table");
    my $config_data     = $self->get_config_data();

    $template->param(
        plugin_version => $VERSION,
        orgs           => $orgs,
        actions        => $actions,
        runs_total     => $runs_total    // 0,
        changes_total  => $changes_total // 0,
        logs_limit     => int($config_data->{logs_limit} || 3),
        language       => C4::Languages::getlanguage($cgi) || 'en',
    );

    print $cgi->header(-type => 'text/html', -charset => 'utf-8');
    print $template->output();
    return;
}

# JSON for the log tables (DataTables server-side protocol) and for a single response
sub log_ajax {
    my ($self, $op) = @_;
    my $cgi = $self->{'cgi'};

    my $result = eval {
        $op eq 'ajax-log-response' ? $self->log_response($cgi) : $self->log_table_data($cgi, $op eq 'ajax-log-changes' ? 'changes' : 'runs');
    };
    if ($@) {
        log_message("Yes", "log_ajax $op failed: $@");
        $result = { ok => JSON::false, message => $self->t('Unexpected error: %s', $@), draw => int($cgi->param('draw') // 0), data => [], recordsTotal => 0, recordsFiltered => 0, error => "$@" };
    }

    print_json($cgi, $result);
    return;
}

sub log_response {
    my ($self, $cgi) = @_;
    my $id = int($cgi->param('id') // 0);
    my $dbh = C4::Context->dbh;
    my ($created, $org, $resp) = $dbh->selectrow_array(
        "SELECT created_at, organisation_code, SUBSTRING(response, 1, 300000) FROM $logs_table WHERE id = ?", undef, $id
    );
    return { ok => JSON::false, message => $self->t('No such log row') } unless defined $created;
    return { ok => JSON::true, id => $id, created_at => "$created", organisation => $org // '', response => $resp // '' };
}

sub log_table_data {
    my ($self, $cgi, $kind) = @_;
    my $dbh  = C4::Context->dbh;
    my $spec = log_tables()->{$kind};

    my $draw   = int($cgi->param('draw') // 0);
    my $start  = int($cgi->param('start') // 0);  $start = 0 if $start < 0;
    my $length = int($cgi->param('length') // 25);
    $length = 25 if $length == 0;
    $length = 500 if $length < 0 || $length > 500;

    my $ncols = scalar @{ $spec->{cols} };
    my $ocol  = int($cgi->param('order[0][column]') // 0);
    $ocol = 0 if $ocol < 0 || $ocol >= $ncols;
    my $odir  = lc($cgi->param('order[0][dir]') // 'desc') eq 'asc' ? 'ASC' : 'DESC';
    my $order_sql = $spec->{cols}[$ocol] . " $odir, $spec->{pk} DESC";

    my (@conds, @binds);

    my $days = int($cgi->param('days') // 0);
    if ($days > 0) {
        push @conds, "$spec->{time} >= DATE_SUB(NOW(), INTERVAL ? DAY)";
        push @binds, $days;
    }

    if ($kind eq 'runs') {
        my $org = $cgi->param('org') // '';
        if ($org ne '') { push @conds, 'organisation_code = ?'; push @binds, $org; }
        my $state = $cgi->param('state') // '';
        if ($state =~ /^(?:restart|done|reset|more pages|pending)$/) {
            push @conds, "($LOG_STATE_SQL) = ?";
            push @binds, $state;
        }
    } else {
        my $action = $cgi->param('action') // '';
        if ($action ne '') { push @conds, 'action = ?'; push @binds, $action; }
    }

    my $search = $cgi->param('search[value]') // '';
    foreach my $word (grep { length } split /\s+/, $search) {
        (my $like = $word) =~ s/([\\%_])/\\$1/g;
        push @conds, "$spec->{search} LIKE ?";
        push @binds, "%$like%";
    }

    my $where = @conds ? 'WHERE ' . join(' AND ', @conds) : '';
    my ($total)    = $dbh->selectrow_array("SELECT COUNT(*) FROM $spec->{table}");
    my ($filtered) = $dbh->selectrow_array("SELECT COUNT(*) FROM $spec->{table} $where", undef, @binds);

    my $select = join(', ', @{ $spec->{cols} });
    my $rows = $dbh->selectall_arrayref(
        "SELECT $select FROM $spec->{table} $where ORDER BY $order_sql LIMIT $length OFFSET $start",
        undef, @binds
    );

    return {
        draw            => $draw,
        recordsTotal    => int($total // 0),
        recordsFiltered => int($filtered // 0),
        data            => [ map { [ map { defined $_ ? "$_" : '' } @$_ ] } @$rows ],
    };
}

# JSON that is safe to embed inside a <script> element
sub json_for_html {
    my ($data) = @_;
    my $json = JSON->new->canonical->encode($data);
    $json =~ s{</}{<\\/}g;
    $json =~ s{<!--}{<\\!--}g;
    return $json;
}

# Fetch every page of an IST endpoint. Returns (\@rows) or (undef, $http_code, $error_body).
sub ist_fetch_all {
    my ($api_base, $token, $endpoint, $params, $limit) = @_;
    my (@all, $page_token);
    my $pages = 0;
    while (1) {
        my $url = $api_base . $endpoint . '?limit=' . ($limit || 100);
        $url .= '&' . uri_escape($_) . '=' . uri_escape($params->{$_}) for sort keys %{ $params || {} };
        $url .= '&pageToken=' . uri_escape($page_token) if $page_token;
        my ($body, $code, $err) = getApiResponse($url, $token);
        return (undef, $code, $err) unless defined $body;
        my $data = eval { decode_json($body) };
        return (undef, 0, "Invalid JSON from $endpoint") unless ref $data eq 'HASH';
        push @all, @{ $data->{data} || [] };
        $page_token = $data->{pageToken};
        last if !$page_token || ++$pages >= 200;
    }
    return (\@all);
}

# Lower-case words without accents, punctuation, digits and generic school words
sub name_tokens {
    my ($s) = @_;
    $s = lc(NFD($s // ''));
    $s =~ s/\p{M}//g;
    $s =~ s/[^a-z0-9]+/ /g;
    my %stop = map { $_ => 1 } qw(skola skolan skolor gymn gymnasium gymnasiet grundskola grundskolan anpassade anpassad st);
    return grep { length($_) && $_ !~ /^\d+$/ && !$stop{$_} } split /\s+/, $s;
}

sub tokens_match {
    my ($x, $y) = @_;
    return 1 if $x eq $y;
    my ($short, $long) = length($x) <= length($y) ? ($x, $y) : ($y, $x);
    return length($short) >= 4 && index($long, $short) == 0;
}

# Score 0..1: share of the smaller word set that is found in the other one
sub name_similarity {
    my ($name_a, $name_b) = @_;
    my @a = name_tokens($name_a);
    my @b = name_tokens($name_b);
    return 0 unless @a && @b;
    my $hits = 0;
    foreach my $x (@a) {
        $hits++ if grep { tokens_match($x, $_) } @b;
    }
    my $min = @a < @b ? scalar(@a) : scalar(@b);
    return $hits / $min;
}

sub configure_ajax {
    my ($self, $op, $cgi) = @_;

    my $result = eval { $self->ajax_dispatch($op, $cgi) };
    if ($@) {
        log_message("Yes", "configure_ajax $op failed: $@");
        $result = { ok => JSON::false, message => $self->t('Unexpected error: %s', $@) };
    }

    print_json($cgi, $result);
    return;
}

# Print a JSON response. The module sets a :utf8 layer on STDOUT, so the already
# UTF-8 encoded body is printed through a raw handle to avoid double encoding.
sub print_json {
    my ($cgi, $data) = @_;
    my $body = JSON->new->utf8->canonical->encode($data);
    print $cgi->header(-type => 'application/json', -charset => 'utf-8', -cache_control => 'no-store');
    binmode(STDOUT, ':raw');
    print $body;
    binmode(STDOUT, ':utf8');
    return;
}

sub ajax_dispatch {
    my ($self, $op, $cgi) = @_;

    my $dbh    = C4::Context->dbh;

    if ($op eq 'ajax-job-status') {
        return { ok => JSON::true, %{ job_status($dbh) } };
    }
    if ($op eq 'ajax-job-request') {
        return { ok => JSON::false, message => $self->t('The request could not be verified. Reload the page and try again.') }
            unless $self->verify_csrf($cgi);
        my $type = $cgi->param('job_type') // '';
        my $r = request_job($dbh, $type, (($self->{session_data} || {})->{userid} // 'unknown'), 'manual');
        return { ok => JSON::false, message => $self->t('Unknown job type') } unless $r->{ok};
        return { ok => JSON::true, id => $r->{id}, already => ($r->{already} ? JSON::true : JSON::false), %{ job_status($dbh) } };
    }

    my $config = $self->get_config_data();

    unless ($config->{ist_client_id} && length($config->{ist_client_secret} // '') && $config->{ist_customer_id}) {
        return { ok => JSON::false, message => $self->t('Save Client ID, Client Secret and Customer ID first, then try again.') };
    }

    my $token = $self->get_api_token($config, LWP::UserAgent->new);
    unless ($token) {
        return { ok => JSON::false, message => $self->t('Could not get an access token. Check Client ID, Client Secret and OAUTH API URL.') };
    }
    my $api_base = ($config->{ist_api_url} || '') . '/ss12000v2-api/source/' . ($config->{ist_customer_id} || '') . '/v2.0/';

    if ($op eq 'ajax-test-connection') {
        my ($orgs, $code, $err) = ist_fetch_all($api_base, $token, 'organisations', {}, 100);
        return { ok => JSON::false, message => $self->t('Token received, but organisations returned HTTP %s. Check API URL and Customer ID.', $code // '?') }
            unless $orgs;
        my ($duties, $dcode) = getApiResponse($api_base . 'duties?limit=1', $token);
        my $duties_state = defined $duties ? 'available' : ((($dcode // 0) == 403) ? 'forbidden' : 'error');
        return {
            ok            => JSON::true,
            organisations => scalar(@$orgs),
            duties        => $duties_state,
            message       => $self->t('Connected to IST. %s organisations found.', scalar(@$orgs))
                           . ($duties_state eq 'forbidden' ? ' ' . $self->t("Duties (staff roles) are not included in this customer's subscription.") : ''),
        };
    }

    if ($op eq 'ajax-ist-orgs') {
        my ($orgs, $code) = ist_fetch_all($api_base, $token, 'organisations', {}, 100);
        return { ok => JSON::false, message => $self->t('Could not read organisations (HTTP %s)', $code // '?') } unless $orgs;
        my %by_id = map { $_->{id} => $_ } @$orgs;
        my @list = map {
            {
                id     => $_->{id},
                code   => $_->{organisationCode} // '',
                name   => $_->{displayName} // '',
                type   => $_->{organisationType} // '',
                parent => (($by_id{ ($_->{parentOrganisation} || {})->{id} // '' } || {})->{displayName} // ''),
            }
        } @$orgs;
        @list = sort { lc($a->{name}) cmp lc($b->{name}) } @list;
        return { ok => JSON::true, organisations => \@list };
    }

    if ($op eq 'ajax-ist-roles') {
        my ($duties, $code) = ist_fetch_all($api_base, $token, 'duties', {}, 100);
        unless ($duties) {
            return { ok => JSON::false, forbidden => (($code // 0) == 403 ? JSON::true : JSON::false),
                     message => (($code // 0) == 403
                        ? $self->t("The duties endpoint is not included in this customer's IST subscription (403). Staff roles cannot be read.")
                        : $self->t('Could not read duties (HTTP %s)', $code // '?')) };
        }
        my %count;
        $count{ $_->{dutyRole} }++ for grep { defined $_->{dutyRole} && $_->{dutyRole} ne '' } @$duties;
        my @roles = map { { role => $_, count => $count{$_} } } sort { lc($a) cmp lc($b) } keys %count;
        return { ok => JSON::true, roles => \@roles };
    }

    if ($op eq 'ajax-suggest-branches') {
        my ($orgs, $code) = ist_fetch_all($api_base, $token, 'organisations', {}, 100);
        return { ok => JSON::false, message => $self->t('Could not read organisations (HTTP %s)', $code // '?') } unless $orgs;

        my %mapped = map { $_ => 1 } @{ $dbh->selectcol_arrayref("SELECT organisationCode FROM $branches_mapping_table") || [] };
        my @branches = map { { code => $_->[0], name => $_->[1] // '' } }
                       @{ $dbh->selectall_arrayref("SELECT branchcode, branchname FROM $branches_table") };

        my @suggestions;
        foreach my $org (@$orgs) {
            my $org_code = $org->{organisationCode} // '';
            next if $org_code eq '' || $mapped{$org_code};
            my ($best, $best_score, $second) = (undef, 0, 0);
            foreach my $b (@branches) {
                my $score = name_similarity($org->{displayName}, $b->{name});
                $score = 1 if lc($org_code) eq lc($b->{code});
                if ($score > $best_score) { $second = $best_score; ($best, $best_score) = ($b, $score) }
                elsif ($score > $second) { $second = $score }
            }
            next unless $best && $best_score >= 0.8;
            push @suggestions, {
                org      => $org_code,
                org_name => $org->{displayName} // '',
                branch   => $best->{code},
                branch_name => $best->{name},
                score    => sprintf('%.2f', $best_score),
                ambiguous => ($second >= $best_score ? JSON::true : JSON::false),
            };
        }
        @suggestions = sort { $a->{org_name} cmp $b->{org_name} } @suggestions;
        return { ok => JSON::true, suggestions => \@suggestions };
    }

    return { ok => JSON::false, message => $self->t('Unknown operation %s', $op) };
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
    $template->param(ss => $self->ss_sub);
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
                        text  => $self->t('Process died unexpectedly'),
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
                message => $self->t('Permission denied')
            });
            exit;
        }
        
        my $status = $self->read_status();
        
        if ($status->{started_at} && time() - $status->{started_at} < 300) {
            print JSON::encode_json({
                status  => 'error',
                message => $self->t('Process is still young, wait at least 5 minutes before forcing unlock')
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
                    text  => $self->t('Process forcefully unlocked by user'),
                    error => 0
                }
            ]
        };
        
        $self->save_status($status);
        $self->release_lock();
        
        log_message("Yes", "Export process forcefully unlocked by user");
        
        print JSON::encode_json({
            status  => 'success',
            message => $self->t('Process unlocked successfully')
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
                    message => $self->t('Export process is already running')
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
    eval { $dbh->do("DELETE FROM $rule_log_table WHERE updated_at < DATE_SUB(CURDATE(), INTERVAL 90 DAY)") };

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
                } elsif (($self->{org_lookup_status} // '') eq 'not_found') {
                    # The organisation does not exist in IST (renamed, removed or mistyped mapping).
                    # Count it as done for today so it cannot block the final checks, and say so clearly.
                    my $msg = "Organisation code $org_code does not exist in IST - skipped for today (check the branch mapping)";
                    log_message('Yes', $msg);
                    $dbh->do(
                        "INSERT INTO $logs_table (page_token_next, response, data_endpoint, organisation_code, is_processed) VALUES (NULL, ?, ?, ?, 1)",
                        undef, $msg, $data_endpoint, $org_code
                    );
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

    # $self->{org_lookup_status}: 'found' | 'not_found' (IST answered, no such organisation) | 'error' (could not ask)
    $self->{org_lookup_status} = 'error';

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
        my $org_data = eval { decode_json($response->content) };
        if ($org_data && $org_data->{data} && @{$org_data->{data}}) {
            $self->{org_lookup_status} = 'found';
            return $org_data->{data}[0]->{id};
        }
        $self->{org_lookup_status} = 'not_found' if $org_data;
    } elsif ($response->code == 404) {
        $self->{org_lookup_status} = 'not_found';
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
    
    # The civic number is looked up in IST. It normally equals cardnumber, but when the civic number
    # is kept in a borrower attribute (civic_attribute_code) that attribute is used instead
    # (the card number can then be a library card that IST knows nothing about).
    my $civic_attr_code = $config_data->{civic_attribute_code} // '';
    my @missing_binds;
    my $select_potential_missing_query;
    if ($civic_attr_code ne '') {
        $select_potential_missing_query = qq{
            SELECT b.borrowernumber, ba.attribute AS cardnumber, b.surname, b.firstname, b.opacnote
            FROM $borrowers_table b
            JOIN borrower_attributes ba ON ba.borrowernumber = b.borrowernumber AND ba.code = ?
            WHERE b.opacnote LIKE '%by SS12000:%'
            AND b.opacnote NOT LIKE '%No update: user no longer in API%'
            AND b.opacnote NOT LIKE '%$today_date%'
            AND ba.attribute IS NOT NULL
            AND ba.attribute != ''
        };
        @missing_binds = ($civic_attr_code);
    } else {
        $select_potential_missing_query = qq{
            SELECT borrowernumber, cardnumber, surname, firstname, opacnote
            FROM $borrowers_table
            WHERE opacnote LIKE '%by SS12000:%'
            AND opacnote NOT LIKE '%No update: user no longer in API%'
            AND opacnote NOT LIKE '%$today_date%'
            AND cardnumber IS NOT NULL
            AND cardnumber != ''
        };
    }
    
    eval {
        my $sth = $dbh->prepare($select_potential_missing_query);
        $sth->execute(@missing_binds);
        
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
                    "User no longer in API (verified)"
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

    eval { $self->process_leavers($dbh, $config_data); 1 } or log_message('Yes', "Error handling patrons who have left: $@");
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
    my $opts = {
        match_methods        => $config_data->{match_methods} || 'userid,cardnumber',
        civic_attribute_code => $config_data->{civic_attribute_code} // '',
        class_attribute_code => $config_data->{class_attribute_code} || 'CL',
        email_suffix         => (($config_data->{email_suffix_enabled} // 'No') eq 'Yes' ? ($config_data->{email_suffix} // '') : ''),
        keep_existing_category => (($config_data->{keep_existing_category} // 'No') eq 'Yes' ? 1 : 0),
        keep_existing_contact  => (($config_data->{keep_existing_contact}  // 'No') eq 'Yes' ? 1 : 0),
    };
    
    if (($config_data->{use_rules} // 'No') eq 'Yes') {
        my $rules = load_rules($dbh);
        my $used  = SS12000Rules::fields_used($rules);
        $opts->{use_rules}              = 1;
        $opts->{rules}                  = $rules;
        $opts->{rules_need_duties}      = (grep { $used->{$_} } qw(role assignment_role duty_school person_kind)) ? 1 : 0;
        $opts->{rules_need_duty_school} = $used->{duty_school}  ? 1 : 0;
        $opts->{rules_need_school_group} = $used->{school_group} ? 1 : 0;
        $opts->{custom_paths}           = [ SS12000Rules::custom_paths($rules) ];
        $opts->{valid_categories}       = { map { $_ => 1 } @{ $dbh->selectcol_arrayref("SELECT categorycode FROM $categories_table") || [] } };
        $opts->{valid_branches}         = { map { $_ => 1 } @{ $dbh->selectcol_arrayref("SELECT branchcode FROM $branches_table") || [] } };
        log_message($debug_mode, 'Using ' . scalar(@$rules) . ' rule(s) for category and branch');
    }

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

        my ($response_data, $http_err_code, $http_err_body) = getApiResponse($api_url, $access_token);

        # 410 Gone — the pageToken cursor is no longer valid on the API side.
        # IST answers NEW_DATA_RESTART_FROM_FIRST_PAGE when data changed since the
        # previous page was served. The organisation is NOT finished: restart it from
        # the first page (already processed users are simply updated again).
        # A cap per organisation and day prevents endless restart loops.
        if (!defined $response_data && ($http_err_code // 0) == 410) {
            my $max_restarts = 10;
            my $restart_code = ($http_err_body // '') =~ /NEW_DATA_RESTART_FROM_FIRST_PAGE/
                ? 'NEW_DATA_RESTART_FROM_FIRST_PAGE' : 'unknown';
            log_message('Yes', "pageToken rejected by API (410 Gone, code: $restart_code) for org $current_org_code, response: " . ($http_err_body // ''));

            my ($restarts) = $dbh->selectrow_array(
                "SELECT COUNT(*) FROM $logs_table WHERE DATE(created_at) = CURDATE() AND organisation_code = ? AND data_endpoint = 'restart_marker'",
                undef, $current_org_code
            );

            if (($restarts // 0) >= $max_restarts) {
                my $msg = "Gave up on organisation $current_org_code after $max_restarts restarts from first page (API keeps returning 410). Will be retried on the next day";
                log_message('Yes', $msg);
                $dbh->do(
                    "INSERT INTO $logs_table (page_token_next, response, data_endpoint, organisation_code, is_processed) VALUES (NULL, ?, ?, ?, 1)",
                    undef, $msg, $data_endpoint, $current_org_code
                );
                die "EndLastPageFromAPI";
            }

            $dbh->do(
                "INSERT INTO $logs_table (page_token_next, response, data_endpoint, organisation_code, is_processed) VALUES (NULL, ?, 'restart_marker', ?, 1)",
                undef, "Restart " . (($restarts // 0) + 1) . " of $max_restarts from first page: $restart_code", $current_org_code
            );
            $dbh->do(
                "UPDATE $logs_table SET page_token_next = 'RESET'
                 WHERE data_endpoint = ? AND organisation_code = ? AND DATE(created_at) = CURDATE()
                 AND page_token_next IS NOT NULL AND page_token_next != 'RESET'",
                undef, $data_endpoint, $current_org_code
            );

            log_message('Yes', "Restarting organisation $current_org_code from first page (restart " . (($restarts // 0) + 1) . " of $max_restarts)");
            $api_url =~ s/&pageToken=[^&]*//;
            ($response_data, $http_err_code, $http_err_body) = getApiResponse($api_url, $access_token);
            if (!defined $response_data) {
                die "Restart from first page failed for org $current_org_code (HTTP " . ($http_err_code // '?') . "): " . ($http_err_body // '');
            }
            undef $data_id;
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
                $opts,
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

# Fetch a URL with Bearer token auth.
# Returns (body, undef) on HTTP 2xx, (undef, $http_code) on any error.
# Always logs the HTTP status on failure so the caller gets a meaningful error
# instead of a cryptic "malformed JSON" from decode_json(undef).
# Callers that only need the body can still call in scalar context — they just
# get undef on error, same as before. Callers that need the status code should
# call in list context: my ($body, $err_code, $err_body) = getApiResponse(...).
# Reads every duty once and groups them by person; used when the rules need roles. Falls back to one request
# per person when the read fails, and notes the 403 of customers that have not ordered duties.
sub load_all_duties_once {
    my ($api_url_base, $token) = @_;
    my ($rows, $code) = ist_fetch_all($api_url_base, $token, 'duties', {}, 100);
    unless ($rows) {
        if (($code // 0) == 403) {
            $duties_forbidden = 1;
            log_message('Yes', "Duties endpoint is not available for this customer (403 Forbidden): dutyRole will not be used");
        } else {
            $duties_cache_failed = 1;
            log_message('Yes', "Could not read all duties (HTTP " . ($code // '?') . "), using one request per person");
        }
        return 0;
    }
    %duties_by_person = ();
    foreach my $d (@$rows) {
        my $pid = ref $d->{person} eq 'HASH' ? $d->{person}{id} : undef;
        push @{ $duties_by_person{$pid} }, $d if defined $pid;
    }
    $duties_loaded = 1;
    log_message('Yes', 'Duties read once for this run: ' . scalar(@$rows) . ' duties for ' . scalar(keys %duties_by_person) . ' persons');
    return 1;
}

sub getApiResponse {
    my ($api_url, $access_token) = @_;

    my $ua = LWP::UserAgent->new;

    my $api_request = HTTP::Request->new(GET => $api_url);
    $api_request->header('Content-Type'  => 'application/json');
    $api_request->header('Authorization' => "Bearer $access_token");
    my $api_response = $ua->request($api_request);

    if ($api_response->is_success) {
        return wantarray ? ($api_response->decoded_content, undef, undef) : $api_response->decoded_content;
    } else {
        my $err_body = eval { substr($api_response->decoded_content // '', 0, 500) } // '';
        $err_body =~ s/\s+/ /g;
        log_message('Yes', "getApiResponse HTTP error: " . $api_response->status_line . " URL: $api_url Response: $err_body");
        return wantarray ? (undef, $api_response->code, $err_body) : undef;
    }
}


sub verify_categorycode_and_branchcode {
    my $dbh = C4::Context->dbh;

    $dbh->do("SET NAMES utf8mb4");
    $dbh->do("SET CHARACTER SET utf8mb4");

    # Roles flagged "do not import" never use their category, so only the other rows count here
    my $select_categories_mapping_query = qq{SELECT categorycode FROM $categories_mapping_table WHERE not_import IS NULL OR not_import = 0};
    my $select_branches_mapping_query   = qq{SELECT branchcode FROM $branches_mapping_table};

    my $categories_mapping_exists = $dbh->selectall_arrayref($select_categories_mapping_query);
    my $branches_mapping_exists   = $dbh->selectall_arrayref($select_branches_mapping_query);

    # The branch mapping is required. The category (role) mapping is optional: customers without
    # staff roles use the default category for everyone.
    if (@$branches_mapping_exists) {
        my $select_categorycode_query = qq{SELECT categorycode FROM $categories_table};
        my $select_branchcode_query   = qq{SELECT branchcode FROM $branches_table};

        my $categorycode_exists = $dbh->selectall_arrayref($select_categorycode_query);
        my $branchcode_exists   = $dbh->selectall_arrayref($select_branchcode_query);

        my $categorycode_in_categories_table_query = qq{
            SELECT categorycode FROM $categories_table
            WHERE categorycode COLLATE utf8mb4_swedish_ci IN (SELECT categorycode FROM $categories_mapping_table WHERE not_import IS NULL OR not_import = 0)
        };

        my $categorycode_in_categories_table = $dbh->selectall_arrayref($categorycode_in_categories_table_query);

        my $branchcode_in_branches_table_query = qq{
            SELECT branchcode FROM $branches_table
            WHERE branchcode COLLATE utf8mb4_swedish_ci IN (SELECT branchcode FROM $branches_mapping_table)
        };

        my $branchcode_in_branches_table = $dbh->selectall_arrayref($branchcode_in_branches_table_query);

        my $category_ok;
        if (@$categories_mapping_exists) {
            $category_ok = scalar @$categorycode_in_categories_table;
        } else {
            my ($default_cat) = $dbh->selectrow_array("SELECT value FROM $config_table WHERE name = 'koha_default_categorycode'");
            $category_ok = (defined $default_cat && $default_cat ne ''
                && $dbh->selectrow_array("SELECT 1 FROM $categories_table WHERE categorycode = ?", undef, $default_cat)) ? 1 : 0;
        }

        if (@$categorycode_exists && @$branchcode_exists && $category_ok && @$branchcode_in_branches_table) {
            return "Ok";
        } else {
            return "No";
        }
    } else {
        return "No";
    }
}

our %org_info_cache;    # organisation id => { code, parent_id }, valid for one run
our %rule_warned;       # rule warnings already written to the log in this run

# Organisation code and parent id for an organisation id (one API call per organisation and run)
sub org_info {
    my ($org_id, $access_token, $api_url_base) = @_;
    return {} unless defined $org_id && $org_id ne '';
    return $org_info_cache{$org_id} if exists $org_info_cache{$org_id};
    my $info = {};
    eval {
        my $raw = getApiResponse($api_url_base . 'organisations/' . $org_id, $access_token);
        if (defined $raw && length($raw) > 0) {
            my $org = decode_json($raw);
            if (ref $org eq 'HASH') {
                $info = { code => $org->{organisationCode}, parent_id => (ref $org->{parentOrganisation} eq 'HASH' ? $org->{parentOrganisation}{id} : undef) };
                $org_info_cache{$org_id} = $info;
            }
        }
    };
    return $info;
}

# Rules from the database as normalised rule hashes (invalid rows are skipped and logged)
sub load_rules {
    my ($dbh) = @_;
    my $rows = eval {
        $dbh->selectall_arrayref(
            "SELECT id, priority, name, enabled, scope, conditions_json, action_category, action_branch, not_import FROM $rules_table",
            { Slice => {} }
        );
    } || [];
    my @rules;
    foreach my $row (@$rows) {
        my $action = $row->{scope} eq 'branch' ? { branch => $row->{action_branch} }
                   : $row->{not_import}        ? { not_import => 1 }
                   :                             { category => $row->{action_category} };
        my ($rule, $err) = SS12000Rules::normalize_rule({
            id => $row->{id}, priority => $row->{priority}, name => $row->{name}, enabled => $row->{enabled},
            scope => $row->{scope}, conditions => SS12000Rules::conditions_from_json($row->{conditions_json}), action => $action,
        });
        if ($err) {
            log_message('Yes', "Rule #$row->{id} skipped: $err");
            next;
        }
        push @rules, $rule;
    }
    return \@rules;
}

# One row per person with the latest rule decision; changed_at only moves when the decision changes
sub save_rule_log {
    my ($dbh, $person_id, %r) = @_;
    my $hash = Digest::MD5::md5_hex(Encode::encode('UTF-8', join("\x1f", map { $_ // '' } @r{qw(person_kind category_result category_explain branch_result branch_explain)})));
    eval {
        $dbh->do(qq{
            INSERT INTO $rule_log_table (person_id, person_kind, category_result, category_explain, branch_result, branch_explain, decision_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                changed_at = IF(decision_hash <=> VALUES(decision_hash), changed_at, NOW()),
                person_kind = VALUES(person_kind),
                category_result = VALUES(category_result),
                category_explain = VALUES(category_explain),
                branch_result = VALUES(branch_result),
                branch_explain = VALUES(branch_explain),
                decision_hash = VALUES(decision_hash),
                updated_at = NOW()
        }, undef, $person_id, @r{qw(person_kind category_result category_explain branch_result branch_explain)}, $hash);
    };
    log_message('Yes', "Error saving rule log for person $person_id: $@") if $@;
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
            $opts,
        ) = @_;

    $opts ||= {};
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

                    # A class counts only if BOTH the group and the student's own membership
                    # are valid today (a group can run all school year while the student has
                    # been moved to another class). Open-ended dates are treated as valid.
                    # Order is deterministic: newest membership start, then newest group start, then name.
                    my @klass_candidates;
                    if (defined $response_data_groupMemberships &&
                        defined $response_data_groupMemberships->{_embedded} &&
                        defined $response_data_groupMemberships->{_embedded}->{groupMemberships}) {
                        foreach my $gm (@{$response_data_groupMemberships->{_embedded}->{groupMemberships}}) {
                            my $g = $gm->{group} || {};
                            next unless ($g->{groupType} // '') eq 'Klass';
                            my $g_start = $g->{startDate} // '0000-00-00';
                            my $g_end   = $g->{endDate}   // '9999-12-31';
                            my $m_start = $gm->{startDate} // '0000-00-00';
                            my $m_end   = $gm->{endDate}   // '9999-12-31';
                            push @klass_candidates, {
                                name     => $g->{displayName} // '',
                                org      => ((ref $g->{organisation} eq 'HASH') ? ($g->{organisation}{id} // '') : ''),
                                g_start  => $g_start, g_end => $g_end,
                                m_start  => $m_start, m_end => $m_end,
                                in_group => ($g_start le $today && $today le $g_end) ? 1 : 0,
                                in_memb  => ($m_start le $today && $today le $m_end) ? 1 : 0,
                            };
                        }
                    }

                    my @valid_klasses = sort {
                        $b->{m_start} cmp $a->{m_start} ||
                        $b->{g_start} cmp $a->{g_start} ||
                        $a->{name}    cmp $b->{name}
                    } grep { $_->{in_group} && $_->{in_memb} } @klass_candidates;

                    foreach my $c (@klass_candidates) {
                        log_message($debug_mode, sprintf(
                            'Klass candidate "%s": group %s..%s (%s), membership %s..%s (%s)',
                            $c->{name}, $c->{g_start}, $c->{g_end}, ($c->{in_group} ? 'valid' : 'not valid'),
                            $c->{m_start}, $c->{m_end}, ($c->{in_memb} ? 'valid' : 'not valid')
                        ));
                    }

                    if (@valid_klasses) {
                        $klass_displayName = $valid_klasses[0]->{name};
                        log_message($debug_mode, 'Selected klass_displayName: ' . $klass_displayName);
                        if (@valid_klasses > 1) {
                            log_message($debug_mode, 'Multiple valid klasses found (' . scalar(@valid_klasses) . '), picked by newest membership start, then group start, then name');
                        }
                    } else {
                        log_message($debug_mode, 'No valid Klass found');
                    }

                    log_message($debug_mode, '::groupMembership END');

                    my $person_api_url    = $api_url_base."duties?person=".$id;
                    log_message($debug_mode, 'person_api_url: '.$person_api_url);
                    my $response_data_person;
                    my $duty_role;

                    # Some customers have not ordered duties (HTTP 403). Ask once, then skip
                    # for the rest of this run instead of one failing call per user.
                    if ($duties_forbidden) {
                        log_message($debug_mode, "Skipping duties lookup (API returned 403 earlier in this run)");
                    } elsif ($opts->{use_rules} && !$opts->{rules_need_duties}) {
                        log_message($debug_mode, "Skipping duties lookup (no rule uses roles)");
                    } else {
                        # With rules the duties of everybody are read once, otherwise there is one request per person
                        if ($opts->{use_rules} && $use_duties_cache && !$duties_loaded && !$duties_cache_failed) {
                            load_all_duties_once($api_url_base, $access_token);
                        }
                        if ($duties_forbidden) {
                            log_message($debug_mode, "Skipping duties lookup (API returned 403 earlier in this run)");
                        } elsif ($duties_loaded) {
                            $response_data_person = { data => ($duties_by_person{$id} || []) };
                        } else {
                        eval {
                            my ($raw_duties, $duties_err) = getApiResponse($person_api_url, $access_token);
                            if (defined $raw_duties && length($raw_duties) > 0) {
                                $response_data_person = decode_json($raw_duties);
                                log_message($debug_mode, 'response_data_person: '.Dumper($response_data_person));
                            } elsif (($duties_err // 0) == 403) {
                                $duties_forbidden = 1;
                                log_message('Yes', "Duties endpoint is not available for this customer (403 Forbidden): dutyRole will not be used");
                            } else {
                                log_message($debug_mode, "Empty/undef response for duties, person id: $id");
                            }
                        };
                        }
                    }

                    if (
                            $response_data_person && 
                            ref($response_data_person) eq 'HASH' && 
                            $response_data_person->{data} && 
                            ref($response_data_person->{data}) eq 'ARRAY' && 
                            @{$response_data_person->{data}}
                        ) {
                        $duty_role = $response_data_person->{data}[0]->{dutyRole};
                    } 


                    my $duties_list;
                    if ($response_data_person && ref($response_data_person) eq 'HASH' && ref($response_data_person->{data}) eq 'ARRAY') {
                        $duties_list = $response_data_person->{data};
                    }

                    log_message($debug_mode, '::duty_role BEGIN');
                    if ($opts->{use_rules}) {
                        log_message($debug_mode, 'Category and branch are decided by rules (role mapping and excluding_dutyRole_empty are not used)');
                    } elsif ($duty_role) {
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

                    # With several valid classes, prefer the class that belongs to the school
                    # where the student has the active enrolment.
                    if (@valid_klasses > 1 && $enroledAtId) {
                        my @preferred = prefer_klass_in_org(\@valid_klasses, $enroledAtId);
                        if (@preferred && @preferred < @valid_klasses) {
                            @valid_klasses = @preferred;
                            $klass_displayName = $valid_klasses[0]->{name};
                            log_message($debug_mode, 'Klass narrowed to the school of the active enrolment: ' . $klass_displayName);
                        }
                    }

                    my $school_code;
                    if ($enroledAtId) {
                        log_message($debug_mode, 'enroledAtId: '.$enroledAtId);
                        
                        my $person_api_url = $api_url_base."organisations/".$enroledAtId;
                        log_message($debug_mode, 'person_api_url: '.$person_api_url);
                        my $organisationCode = $org_code_cache{$enroledAtId};
                        undef $response_data_person;

                        if (defined $organisationCode) {
                            log_message($debug_mode, "organisationCode for $enroledAtId taken from cache: $organisationCode");
                        } else {
                            eval {
                                my $raw_org = getApiResponse($person_api_url, $access_token);
                                if (defined $raw_org && length($raw_org) > 0) {
                                    $response_data_person = decode_json($raw_org);
                                    log_message($debug_mode, 'response_data_person: '.Dumper($response_data_person));
                                } else {
                                    log_message($debug_mode, "Empty/undef response for organisation id: $enroledAtId");
                                }
                            };
                        }

                        if (!defined $organisationCode && defined $response_data_person && ref($response_data_person) eq 'HASH') {
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
                            $org_code_cache{$enroledAtId} = $organisationCode if $organisationCode;
                        }

                        if ($organisationCode) {
                            $school_code = $organisationCode;
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

                    my %rule_decision;
                    if ($opts->{use_rules} && !$not_import) {
                        my $duties_for_facts;
                        if (ref $duties_list eq 'ARRAY') {
                            $duties_for_facts = [ map { +{
                                dutyRole       => $_->{dutyRole},
                                assignmentRole => $_->{assignmentRole},
                                school         => ($opts->{rules_need_duty_school} ? org_info(($_->{dutyAt} || {})->{id}, $access_token, $api_url_base)->{code} : undef),
                                startDate      => $_->{startDate},
                                endDate        => $_->{endDate},
                            } } grep { ref $_ eq 'HASH' } @$duties_list ];
                        }
                        my @group_ids;
                        if ($opts->{rules_need_school_group} && $enroledAtId) {
                            my $parent_id = org_info($enroledAtId, $access_token, $api_url_base)->{parent_id};
                            push @group_ids, $parent_id if $parent_id;
                        }
                        my $civic_for_facts = ref $response_page_data->{civicNo} eq 'HASH' ? $response_page_data->{civicNo}{value} : undef;
                        my %custom = map { $_ => [ SS12000Rules::extract_values($response_page_data, $_) ] } @{ $opts->{custom_paths} || [] };

                        my $facts = SS12000Rules::build_facts(
                            today            => $today,
                            enrolment        => $current_enrolment,
                            school_codes     => [ grep { defined } $school_code ],
                            school_group_ids => \@group_ids,
                            klass            => $klass_displayName,
                            duties           => $duties_for_facts,
                            birthdate        => $response_page_data->{birthDate},
                            civic_no         => $civic_for_facts,
                            custom           => \%custom,
                        );
                        my %ev = (valid_categories => $opts->{valid_categories}, valid_branches => $opts->{valid_branches});
                        my $cat_result = SS12000Rules::evaluate($opts->{rules}, $facts, scope => 'category', %ev);
                        my $br_result  = SS12000Rules::evaluate($opts->{rules}, $facts, scope => 'branch',   %ev);

                        if ($cat_result->{winner}) {
                            if ($cat_result->{winner}{action}{not_import}) {
                                $not_import  = 1;
                                $skip_reason = 'rule #' . ($cat_result->{winner}{id} // '?');
                            } else {
                                $koha_categorycode = $cat_result->{winner}{action}{category};
                            }
                        }
                        $koha_branchcode = $br_result->{winner}{action}{branch} if $br_result->{winner};

                        foreach my $w (@{ $cat_result->{warnings} }, @{ $br_result->{warnings} }) {
                            log_message('Yes', "Rules: $w") unless $rule_warned{$w}++;
                        }
                        %rule_decision = (
                            person_kind      => $facts->{person_kind},
                            category_result  => ($not_import ? 'do not import' : $koha_categorycode),
                            category_explain => ($cat_result->{winner} ? SS12000Rules::explain($cat_result) : "no rule matched, default category " . ($koha_categorycode // '')),
                            branch_result    => $koha_branchcode,
                            branch_explain   => ($br_result->{winner} ? SS12000Rules::explain($br_result) : 'no rule matched, branch from organisation mapping or default'),
                        );
                        log_message($debug_mode, 'Rules, category: ' . $rule_decision{category_explain});
                        log_message($debug_mode, 'Rules, branch: ' . $rule_decision{branch_explain});
                        save_rule_log($dbh, $id, %rule_decision);
                    }

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

                    # Not every IST subscription includes birthDate. A Swedish civic number
                    # (YYYYMMDDNNNN, day +60 for coordination numbers) contains the date of birth.
                    if ((!defined $birthDate || $birthDate eq '') && defined $cardnumber
                        && $cardnumber =~ /^(\d{4})(\d{2})(\d{2})\d{4}$/) {
                        my ($by, $bm, $bd) = ($1, $2, $3);
                        $bd -= 60 if $bd > 60;
                        if ($bm >= 1 && $bm <= 12 && $bd >= 1 && $bd <= 31 && $by >= 1900) {
                            $birthDate = sprintf('%04d-%02d-%02d', $by, $bm, $bd);
                            log_message($debug_mode, 'birthDate derived from civic number: '.$birthDate);
                        }
                    }

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

                        $opts->{borrowernumber_out} = undef;
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
                                $opts,
                            );
                        if (%rule_decision && $opts->{borrowernumber_out}) {
                            eval { $dbh->do("UPDATE $rule_log_table SET borrowernumber = ? WHERE person_id = ?", undef, $opts->{borrowernumber_out}, $id) };
                        }
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
                        my @skip_binds = ($userid, $cardnumber);
                        if (($opts->{civic_attribute_code} // '') ne '' && defined $cardnumber && $cardnumber ne '') {
                            $skip_note_query =~ s/WHERE userid = \? OR cardnumber = \?/WHERE userid = ? OR cardnumber = ? OR borrowernumber IN (SELECT borrowernumber FROM borrower_attributes WHERE code = ? AND attribute = ?)/;
                            push @skip_binds, $opts->{civic_attribute_code}, $cardnumber;
                        }
                        eval {
                            my $skip_sth = $dbh->prepare($skip_note_query);
                            $skip_sth->execute(
                                $current_version_info, $skip_reason,
                                $current_version_info, $skip_reason,
                                $current_version_info, $skip_reason,
                                $current_version_info, $skip_reason,
                                @skip_binds
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

# After records have been moved from a duplicate patron, remove attribute rows that are identical
# (same type and value) to an older row of the same patron. Returns the number of rows removed.
sub dedupe_borrower_attributes {
    my ($dbh, $borrowernumber) = @_;
    my $removed = eval {
        $dbh->do(q{
            DELETE a FROM borrower_attributes a
            JOIN borrower_attributes b
              ON a.borrowernumber = b.borrowernumber AND a.code = b.code AND a.attribute = b.attribute AND a.id > b.id
            WHERE a.borrowernumber = ?
        }, undef, $borrowernumber);
    };
    if ($@) {
        log_message('Yes', "Error removing duplicate attributes for borrowernumber $borrowernumber: $@");
        return 0;
    }
    $removed = ($removed // 0) + 0;
    log_message('Yes', "Removed $removed identical attribute row(s) for borrowernumber $borrowernumber after merge") if $removed;
    return $removed;
}

# Record a merge on the patron that was kept, so it shows in the change log (action "merge")
sub record_merge_change {
    my ($dbh, $main, $duplicate, $removed_attrs) = @_;
    eval {
        $dbh->do(
            "INSERT INTO $data_change_log_table (table_name, record_id, action, change_description) VALUES ('borrowers', ?, 'merge', ?)",
            undef, $main,
            "merged with duplicate borrowernumber $duplicate: related records moved to this patron, "
              . ($removed_attrs // 0) . " identical attribute row(s) removed, the duplicate was archived"
        );
    };
    log_message('Yes', "Error recording merge in change log: $@") if $@;
}

# Keep only the candidate classes whose group belongs to organisation $org (order is kept)
sub prefer_klass_in_org {
    my ($klasses, $org) = @_;
    return grep { defined $_->{org} && $_->{org} ne '' && $_->{org} eq $org } @$klasses;
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
        $opts,
    ) = @_;
    
    my $dbh = C4::Context->dbh;

    my $current_version_info = $version_info;

    $opts ||= {};

    # How existing patrons are found. Default (userid,cardnumber) is the original behaviour.
    # 'attribute' matches the civic number against a borrower attribute (civic_attribute_code).
    my $civic_attr_code = $opts->{civic_attribute_code} // '';
    my $class_attr_code = $opts->{class_attribute_code} || 'CL';
    my %match_by = map { $_ => 1 } grep { /^(?:attribute|userid|cardnumber)$/ } split /\s*,\s*/, ($opts->{match_methods} || 'userid,cardnumber');
    delete $match_by{attribute} if $civic_attr_code eq '';
    %match_by = (userid => 1, cardnumber => 1) unless %match_by;

    # $userid and $cardnumber both carry the civic number when called from fetchBorrowers
    my $civic_no = $userid;
    my ($email_local) = (defined $email ? $email : '') =~ /^([^@\s]+)\@/;

    my %id_source = (
        civicNo            => $civic_no,
        externalIdentifier => $externalIdentifier,
        email_local        => $email_local,
        email              => $email,
    );

    # 'keep' (or a source that has no value for this person) leaves the value undefined here;
    # it is resolved below once we know whether the patron already exists.
    my $newUserID     = $id_source{ $useridPlugin     // 'civicNo' };
    my $newCardnumber = $id_source{ $cardnumberPlugin // 'civicNo' };
    $newUserID     = undef if defined $newUserID     && $newUserID eq '';
    $newCardnumber = undef if defined $newCardnumber && $newCardnumber eq '';

    # Optional suffix for imported e-mail addresses (for example .NOEMAIL), so no notices are sent
    # before go-live. The user id is derived from the address above, i.e. without the suffix.
    my $email_suffix = $opts->{email_suffix} // '';
    if ($email_suffix ne '') {
        my $suffix_lc = lc $email_suffix;
        foreach my $addr ($email, $B_email) {
            next unless defined $addr && $addr ne '';
            next if lc($addr) =~ /\Q$suffix_lc\E$/;
            $addr .= $email_suffix;
        }
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

    my (@match_conds, @match_binds);
    my $has_value = sub { defined $_[0] && $_[0] ne '' };
    if ($match_by{userid}) {
        foreach my $v (grep { $has_value->($_) } ($userid, $newUserID)) {
            push @match_conds, 'b.userid = ?';
            push @match_binds, $v;
        }
    }
    if ($match_by{cardnumber}) {
        foreach my $v (grep { $has_value->($_) } ($cardnumber, $newCardnumber)) {
            push @match_conds, 'b.cardnumber = ?';
            push @match_binds, $v;
        }
    }
    if ($match_by{attribute} && $has_value->($civic_no)) {
        # compare digits only, so a stored 20120811-4011 matches 201208114011
        push @match_conds, q{b.borrowernumber IN (SELECT ba2.borrowernumber FROM borrower_attributes ba2 WHERE ba2.code = ? AND REPLACE(REPLACE(TRIM(ba2.attribute), '-', ''), ' ', '') = ?)};
        push @match_binds, $civic_attr_code, $civic_no;
    }
    my $match_where = @match_conds ? join(' OR ', @match_conds) : '0';
    log_message('Yes', "Looking for existing patron using: " . join(',', sort keys %match_by));

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
        WHERE $match_where
        ORDER BY
            (issues_count + old_issues_count + reserves_count + attributes_count + accountlines_count + messages_count) DESC,
            updated_on DESC
    };
    my $borrowernumber;
    my $find_sth = $dbh->prepare($find_duplicates_query);
    $find_sth->execute(@match_binds);
    
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
            
            my $removed_attrs = dedupe_borrower_attributes($dbh, $main_record->{borrowernumber});

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
                record_merge_change($dbh, $main_record->{borrowernumber}, $duplicate->{borrowernumber}, $removed_attrs);
            };
            if ($@) {
                log_message('Yes', "Error archiving duplicate: $@");
            }
        }
        
        $existing_borrower = $main_record;
        
    } elsif (@duplicates == 1) {
        $existing_borrower = $duplicates[0];
    }

    # Resolve identifiers that have no value from the chosen source:
    # an existing patron keeps what it has; a new patron falls back to school e-mail (userid) / civic number.
    if ($existing_borrower) {
        $newUserID     = $existing_borrower->{userid}     unless defined $newUserID;
        $newCardnumber = $existing_borrower->{cardnumber} unless defined $newCardnumber;
        # Never replace a known date of birth with nothing (IST may not deliver it)
        $birthdate = $existing_borrower->{dateofbirth} unless defined $birthdate && $birthdate ne '';

        # Optional: an existing patron keeps its category (the category is then only set for new patrons)
        $categorycode = $existing_borrower->{categorycode}
            if ($opts->{keep_existing_category} // 0) && $has_value->($existing_borrower->{categorycode});

        # Optional: IST has no value for a contact field, so keep what the patron already has
        if ($opts->{keep_existing_contact}) {
            $email         = $existing_borrower->{email}   unless $has_value->($email);
            $B_email       = $existing_borrower->{B_email} unless $has_value->($B_email);
            $phone         = $existing_borrower->{phone}   unless $has_value->($phone);
            $mobile_phone  = $existing_borrower->{mobile}  unless $has_value->($mobile_phone);
            $streetAddress = $existing_borrower->{address} unless $has_value->($streetAddress);
            $locality      = $existing_borrower->{city}    unless $has_value->($locality);
            $postalCode    = $existing_borrower->{zipcode} unless $has_value->($postalCode);
            $country       = $existing_borrower->{country} unless $has_value->($country);
        }
    } else {
        $newUserID     = (defined $email_local ? $email_local : $civic_no) unless defined $newUserID;
        $newCardnumber = $civic_no unless defined $newCardnumber;

        # A new patron must not take a userid or cardnumber that already belongs to someone else
        # (Koha keeps them unique). Fall back to the civic number, which identifies the person.
        if (defined $newUserID && $has_value->($civic_no) && $newUserID ne $civic_no) {
            my ($taken) = $dbh->selectrow_array("SELECT borrowernumber FROM $borrowers_table WHERE userid = ? LIMIT 1", undef, $newUserID);
            if ($taken) {
                log_message('Yes', "userid '$newUserID' already belongs to borrowernumber $taken (not matched to this person), using the civic number as userid");
                $newUserID = $civic_no;
            }
        }
        if (defined $newCardnumber && $has_value->($civic_no) && $newCardnumber ne $civic_no) {
            my ($taken) = $dbh->selectrow_array("SELECT borrowernumber FROM $borrowers_table WHERE cardnumber = ? LIMIT 1", undef, $newCardnumber);
            if ($taken) {
                log_message('Yes', "cardnumber '$newCardnumber' already belongs to borrowernumber $taken, using the civic number as cardnumber");
                $newCardnumber = $civic_no;
            }
        }
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

    # Optionally keep the civic number in a borrower attribute (e.g. CIVICNO), used for matching
    $opts->{borrowernumber_out} = $borrowernumber;

    if ($borrowernumber && $civic_attr_code ne '' && $has_value->($civic_no)) {
        set_borrower_attribute($dbh, $borrowernumber, $civic_attr_code, $civic_no);
    }

    if ($borrowernumber && $klass_displayName) {
        my $code = $class_attr_code;
        log_message('Yes', "Processing Klass attribute ($code) for borrowernumber $borrowernumber, plugin version: $VERSION");

        my $check_types_query = qq{
            SELECT 1 FROM borrower_attribute_types WHERE code = ?
        };
        my $check_types_sth = $dbh->prepare($check_types_query);
        $check_types_sth->execute($code);
        my ($exists) = $check_types_sth->fetchrow_array();

        if (!$exists && $code ne 'CL') {
            log_message('Yes', "Class attribute type '$code' does not exist in Koha (Administration > Patron attribute types) - class not saved");
            return;
        }

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
                $dbh->commit() unless $dbh->{AutoCommit};
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
                    $dbh->commit() unless $dbh->{AutoCommit};
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
                $dbh->commit() unless $dbh->{AutoCommit};
            };
            if ($@) {
                log_message('Yes', "Error inserting into borrower_attributes: $@");
            }
        }
    } else {
        log_message('Yes', "Skipping Klass update: borrowernumber=" . ($borrowernumber // 'undef') . ", klass_displayName=" . ($klass_displayName // 'undef'));
    }
}

# Insert or update a single-value borrower attribute. The attribute type must already exist.
sub set_borrower_attribute {
    my ($dbh, $borrowernumber, $code, $value) = @_;

    my ($type_exists) = $dbh->selectrow_array("SELECT 1 FROM borrower_attribute_types WHERE code = ?", undef, $code);
    unless ($type_exists) {
        log_message('Yes', "Patron attribute type '$code' does not exist in Koha - attribute not saved");
        return 0;
    }

    my ($existing) = $dbh->selectrow_array(
        "SELECT attribute FROM borrower_attributes WHERE borrowernumber = ? AND code = ? LIMIT 1",
        undef, $borrowernumber, $code
    );

    eval {
        if (!defined $existing) {
            $dbh->do("INSERT INTO borrower_attributes (borrowernumber, code, attribute) VALUES (?, ?, ?)", undef, $borrowernumber, $code, $value);
            log_message('Yes', "Inserted attribute $code for borrowernumber $borrowernumber");
        } elsif ($existing ne $value) {
            $dbh->do("UPDATE borrower_attributes SET attribute = ? WHERE borrowernumber = ? AND code = ?", undef, $value, $borrowernumber, $code);
            log_message('Yes', "Updated attribute $code for borrowernumber $borrowernumber");
        }
    };
    if ($@) {
        log_message('Yes', "Error saving attribute $code: $@");
        return 0;
    }
    return 1;
}



sub get_log_contents {
    my ($self) = @_;
    my $log_file = get_log_file();
    my @log_lines;

    if (-f $log_file) {
        open my $fh, '<:raw', $log_file or return [];
        my @lines = <$fh>;
        close $fh;

        @lines = @lines[-100..-1] if @lines > 100;

        foreach my $line (@lines) {
            $line = Encode::decode('UTF-8', $line, Encode::FB_DEFAULT);
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
            message => $self->t('Export process is already running')
        };
    }

    # Acquire lock before forking so the child PID can be recorded atomically.
    # The lock is acquired here in the parent; the child updates it with its own PID.
    my $existing = $self->acquire_lock('http');
    if ($existing) {
        return {
            status  => 'error',
            message => $self->t('Could not acquire process lock — another process is running')
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
            message => $self->t('Failed to fork export process')
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
                text  => $self->t('All pages processed successfully'),
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
                text  => $self->t('Page %s processed. More pages available.', $pages_done),
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
                text  => $self->t('All pages processed successfully'),
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
                text  => $self->t('Error during export: %s', $error),
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
            message => $self->t('Process is still running')
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
        message => $self->t('Process unlocked successfully')
    };
}


1;
