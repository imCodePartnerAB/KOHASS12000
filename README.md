# KOHASS12000

![imCode](Doc/logo_imcode.png)

This is a plugin for [Koha](https://github.com/Koha-Community/Koha) by [imCode](https://imcode.com)

It exports user data from the API in SS12000 format to your Koha database

Plugin and cron jobs for importing SS12000 v 1.1


Oct 10 2023:

[1.1 version of koha-plugin-export-users_ss12000](https://github.com/imCodePartnerAB/KOHASS12000/releases/tag/v10.10.2023)



# KohaSS12000 › Configuration
![Configuration](Doc/KohaSS12000Configuration.png)

![Configuration](Doc/KohaSS12000Configuration_sv.png)


# cron jobs for importing SS12000

# Put script ExportUsers/cron/imcode_ss12000.pl to:
/usr/share/koha/bin/cronjobs

# Perform one cycle of passing through the data in the API:
sudo koha-foreach /usr/share/koha/bin/cronjobs/imcode_ss12000.pl

# Go through all the pages in the API
/var/lib/koha/defaultlibraryname/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run_ss12000.sh

# Examples of use in cron:
40 */12 * * * root /bin/timeout 8h /var/lib/koha/defaultlibraryname/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run_ss12000.sh >> /var/lib/koha/defaultlibraryname/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run_ss12000.log

# Additional, MULTIPLE KOHA SUPPORT:
PERL5LIB=/usr/share/koha/lib

KOHA_CONF=/etc/koha/koha-conf.xml

Some additional variables to save you typing
KOHA_CRON_PATH = /usr/share/koha/bin/cronjobs

You can still run jobs for this user's additional koha installs, by manipulating those variables in the command.

For example, on the same codebase:
*/10 * * * *    __KOHA_USER__  KOHA_CONF=/etc/koha/koha-conf.xml /usr/share/koha/bin/migration_tools/rebuild_zebra.pl -b -a -z >/dev/null

For example, on a separate codebase:
*/10 * * * *    __KOHA_USER__  KOHA_CONF=/etc/koha/koha-conf.xml PERL5LIB=/home/koha/kohaclone /home/koha/kohaclone/misc/migration_tools/rebuild_zebra.pl -b -a -z >/dev/null
