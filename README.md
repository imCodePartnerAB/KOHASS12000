# KOHASS12000

![imCode](Doc/logo_imcode.png)

This is a plugin for [Koha](https://github.com/Koha-Community/Koha) by [imCode](https://imcode.com)

It exports user data from the API in SS12000 format to your Koha database

Plugin and cron jobs for importing SS12000 v 1.1


Nov 17 2023:

[1.2 version of koha-plugin-export-users_ss12000](https://github.com/imCodePartnerAB/KOHASS12000/releases/tag/v17.11.2023)



# KohaSS12000 › Installation and CRON

1. Run this command on a server running Koha:
```
sudo service memcached restart ; sudo service koha-common restart
```
2. Put script [Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/imcode_ss12000.pl](Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/imcode_ss12000.pl) to 
```
/usr/share/koha/bin/cronjobs
```
![imcode_ss12000.pl](Doc/KohaSS12000Install_1.png)

3. Go through all the pages in the API
/var/lib/koha/defaultlibraryname/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run_ss12000.sh

Examples of use in cron:
```
4 */12 * * * root /bin/timeout 8h /var/lib/koha/defaultlibraryname/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run_ss12000.sh >> /var/lib/koha/defaultlibraryname/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run_ss12000.log
```
![imcode_ss12000.pl](Doc/KohaSS12000Install_2.png)

**run_ss12000.sh** the script has protection against re-running. It also has a runtime limit, which is 8 hours by default.

4. After you've added a script call to cron, you need to restart cron on the server running Koha:
```
sudo /etc/init.d/cron restart
```




# KohaSS12000 › Configuration
![Configuration](Doc/KohaSS12000Configuration.png)

![Configuration](Doc/KohaSS12000Configuration_sv.png)

