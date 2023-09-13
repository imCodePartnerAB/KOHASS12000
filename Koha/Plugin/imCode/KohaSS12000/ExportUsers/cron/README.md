put imcode_ss12000.pl to:
/usr/share/koha/bin/cronjobs

execution of the script:
sudo koha-foreach /usr/share/koha/bin/cronjobs/imcode_ss12000.pl

examples of use in cron:
/usr/share/koha/bin/cronjobs/crontab.example

#
# PERL5LIB=/usr/share/koha/lib
# KOHA_CONF=/etc/koha/koha-conf.xml

# Some additional variables to save you typing
# KOHA_CRON_PATH = /usr/share/koha/bin/cronjobs
#
# MULTIPLE KOHA SUPPORT:
# You can still run jobs for this user's additional koha installs,
# by manipulating those variables in the command.
#
# For example, on the same codebase:
# */10 * * * *    __KOHA_USER__  KOHA_CONF=/etc/koha/koha-conf.xml /usr/share/koha/bin/migration_tools/rebuild_zebra.pl -b -a -z >/dev/null
#
# For example, on a separate codebase:
# */10 * * * *    __KOHA_USER__  KOHA_CONF=/etc/koha/koha-conf.xml PERL5LIB=/home/koha/kohaclone /home/koha/kohaclone/misc/migration_tools/rebuild_zebra.pl -b -a -z >/dev/null
#
