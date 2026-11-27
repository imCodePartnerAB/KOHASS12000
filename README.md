# KOHASS12000

![imCode](Doc/logo_imcode.png)

A plugin for [Koha](https://github.com/Koha-Community/Koha) by [imCode](https://imcode.com) that reads students (and staff, if the school register delivers them) from a school register through the SS12000 standard (IST) and creates and updates patrons in Koha.

Current version: **2.0**. See [CHANGELOG.md](CHANGELOG.md).

## What it does

- Imports students with a valid enrolment: name, civic number, date of birth, contact details, library, class and expiry date.
- Recognises existing patrons (user id, card number and/or a patron attribute) so there are no duplicates.
- Decides the patron category with rules (school, school year, programme, class, role, age and more).
- Maps schools to library branches. Imports staff from their duties, if ordered.
- Moves, removes or anonymises patrons who have left the register.
- Runs on a schedule or on demand. Full log of every run and every change in Koha.

## Documentation

| Document | For | Language |
|---|---|---|
| [Feature list](Doc/Funktionslista.md) ([English](Doc/Feature_list_en.md)) | Customers and sales: what is included and what is not | Swedish, English |
| [Manual](Doc/Manual_sv.md) | Library administrators: configuration page by page | Swedish |
| [Installation](Doc/Installation_sv.md) | System administrators: requirements, install, upgrade, cron, troubleshooting, data protection | Swedish |
| [SS12000 fields used](Doc/SS12000_Fields_Specification.md) | Developers | English |
| [Possible additions](TODO.md) | Developers | English |

## Install

1. Download the `.kpz` file of the latest [release](https://github.com/imCodePartnerAB/KOHASS12000/releases), or build it (below).
2. In Koha: Administration > Plugins > Upload plugin. Plugins must be enabled (`enable_plugins` is 1, `plugins_restricted` is 0 in `koha-conf.xml`).
3. The Perl module `URI::Encode` must be installed (`liburi-encode-perl` on Debian and Ubuntu).
4. Restart Plack for the instance.
5. Put one line in cron that runs every minute, to use the schedule and *Run import now*:
   ```
   * * * * * root /var/lib/koha/<instance>/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/tick_ss12000.sh >> /var/log/koha/<instance>/tick_ss12000.log 2>&1
   ```

Details, upgrade and troubleshooting are in [Doc/Installation_sv.md](Doc/Installation_sv.md).

The package contains two plugins: the main plugin (*Export Users from SS12000*) and a small companion plugin (*Imported SS12000 users (log)*) that adds the log page under Administration > Plugins.

## Build

```
./create.sh
```

This zips the `Koha` directory into `koha-plugin-export-users_ss12000_v<version>.kpz`. The version is read from `ExportUsers.pm`. `.kpz` files are ignored by git and attached to the GitHub release.

## Test

The rule engine, the schedule and the handling of patrons who have left are pure Perl without Koha dependencies:

```
prove t/
```

They do not test the import against IST or a database.

## Layout

| Path | Content |
|---|---|
| `Koha/Plugin/imCode/KohaSS12000/ExportUsers.pm` | The plugin |
| `Koha/Plugin/imCode/KohaSS12000/ExportUsersLog.pm` | Companion plugin with the log link |
| `.../ExportUsers/Rules.pm`, `Schedule.pm`, `Leavers.pm` | Rule engine, schedule and leaver decisions |
| `.../ExportUsers/config.tt`, `log.tt`, `tool.tt`, `error.tt` | Pages |
| `.../ExportUsers/sv.po`, `fi.po`, `com.imcode.exportusers.pot` | Translations (plain gettext files, read directly) |
| `.../ExportUsers/cron/` | `tick_ss12000.sh` (every minute), `run_ss12000.sh` (the import), the Perl scripts they call |
| `t/` | Unit tests |
| `Doc/` | Documentation |
| `Doc/images/`, `Doc/tools/` | Screenshots and the scripts that recreate them |

## Translations

All texts are in `.po` files in `Koha/Plugin/imCode/KohaSS12000/ExportUsers/`. To add a language, copy `com.imcode.exportusers.pot` to `<code>.po`, translate every `msgstr` and keep the `%s` placeholders. The language follows the Koha interface language. A text without a translation is shown in English. Swedish is complete. English is the language of the source texts. Finnish is partly translated (a minority of the texts); the rest is shown in English.

## Screenshots

The screenshots in `Doc/images/` are made from example data only. `Doc/tools/make-screenshots.sh` recreates them on a demo instance (see the comments in the script). Nothing in the repository refers to a real customer, school or person.

## License

See [LICENSE](LICENSE).

[imCode](https://imcode.com), info@imcode.com
