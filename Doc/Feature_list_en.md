# Feature list: SS12000 plugin for Koha

**Product:** SS12000 plugin for Koha (shown in Koha as "Export Users from SS12000")
**Version:** 2.0
**Date:** 2026-11-27

**Everything marked Yes is included. Anything that is not on this list is further development and is quoted separately.**

The plugin reads students (and staff, if ordered) from the school register through IST/SS12000 and creates and updates patrons in Koha. Swedish version (the reference for IDs): [Funktionslista.md](Funktionslista.md). Setup is described in the manual (Swedish): [Manual_sv.md](Manual_sv.md).

The column *Included* has three values: **Yes**, **Partly** (the limitation is written in the row) and **No** (see "Not included"). Texts in quotation marks are headings and buttons as they appear in Koha in English.

## Connection

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-01 | Connection to IST | Reads students (and staff) from IST. The keys are entered on the page. The secret is never shown again after saving. | Needs customer id, client id and client secret from IST. | Yes |
| F-02 | Test the connection, load schools and roles | Buttons that show whether the connection works, how many schools IST has and whether staff data (duties) is included. They also fill the drop-down lists for schools and roles. | Uses the saved settings. Roles need duties in the IST order. | Yes |
| F-03 | Fetch values from IST | Reads schools, school types, school years, education codes, classes and roles and stores them. No personal data is stored. | The values are not yet offered as choices in the rule editor. | Partly |

## Students

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-04 | Import students | Creates new patrons and updates existing ones for students with a valid enrolment today. One page at a time (default 30 persons). | Needs the school mapping and a default category. | Yes |
| F-05 | Data transferred | Name, civic number, date of birth, sex, e-mail, phone, mobile, address, library, class and expiry date. If IST has no date of birth it is calculated from the civic number. | Address: only the registered address. Sex: Man and Kvinna become M and F. The first e-mail address that is not private becomes the main address, private e-mail the alternate address. | Yes |
| F-06 | Class | The class is stored as a patron attribute and follows the student between classes. | Class groups only. The attribute type CL is created automatically, other types must already exist. | Yes |
| F-07 | Expiry date | Set to the end date of the enrolment. If there is none you choose: keep, leave empty or X months ahead. | With several valid enrolments the one that started last is used. | Yes |
| F-08 | Leave out persons without enrolment | Option: persons without a valid enrolment are not imported. | Off by default. | Yes |
| F-09 | Cancelled enrolments | Option: ignore the "cancelled" flag from IST and only look at start and end dates. | Without the option cancelled enrolments are not valid. | Yes |
| F-10 | Note on the patron | Every patron touched by the import gets a note (OPAC note) with version, time and what changed. | The Koha OPAC shows the OPAC note to the patron. The note names fields, not values. | Yes |

## Categories

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-11 | Default category | Everyone who does not get another category gets the default category. | Must be a category that exists in Koha. | Yes |
| F-12 | Category per role (older mode) | Connects a role in IST to a patron category. A role can be marked "Do not import". | Needs duties in IST. Only the first duty of a person is used. The option "Exclude empty users, dutyRole" leaves out persons without a role. The role mapping and that option are not used when "Use rules" is ticked. | Yes |
| F-13 | Category rules | Rules with conditions and an action. Conditions: school, school type, school year, education code, class, role, assignment role, age, person kind, school group, school of the duty, or another field. Action: a category or "Do not import". Priority sets the order, first match wins. | Role conditions need duties in IST. The action can only be a category or "Do not import". | Yes |
| F-14 | Import the role mapping as rules | A button that converts the role mapping into rules. It does not save by itself. | | Yes |
| F-15 | Rule decision per person | The plugin stores which rule decided for each person and which other rules also matched. | Stored in the database, not shown on any page in Koha. Rows not seen for 90 days are removed. | Yes |

## Schools and libraries

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-16 | School mapping | Connects each school in IST to a library (branch) in Koha. Several schools can share a library. | A unit without its own organisation code uses the code of its parent unit. | Yes |
| F-17 | Suggested school mapping | Suggests a library from the names. You review and save. | Suggests only when the names are very similar. | Yes |
| F-18 | Default library for schools missing in the mapping | Option: students from schools without a mapping go to the default library in an extra pass. | Off by default. | Yes |
| F-19 | School missing in IST | A school in the mapping that does not exist in IST is marked as done for the day and logged. The run is not stopped. | | Yes |

## Identity and contact details

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-20 | Find existing patrons | By user id, card number and/or a civic number attribute, in any combination. If two patrons are found for one person they are merged: the older one is archived and loans, holds and fees are moved. | Default: user id and card number. | Yes |
| F-21 | Civic number in an attribute | The civic number is stored in an attribute type you choose and used for matching. Only digits are compared. | The attribute type must exist in Koha. | Yes |
| F-22 | User id and card number | Choose the source: civic number, external identifier, school e-mail (before @ or the whole address) or keep the existing value. | New patrons without a value from the chosen source get the school e-mail name, then the civic number. The plugin sets no password. | Yes |
| F-23 | Text after e-mail addresses | Option: a text (for example .NOEMAIL) is added to the end of every imported e-mail address so Koha cannot send notices before go-live. | Off by default. | Yes |
| F-24 | Keep existing category | Option: the category is only set for new patrons. | Off by default. | Yes |
| F-25 | Keep existing contact details | Option: an empty value from IST never replaces a value that already exists in Koha. | Off by default. | Yes |
| F-26 | Remove archived duplicates | Option: archived duplicates are removed after 3, 7, 14 or 30 days. | Off by default. The removal happens during an import, directly in the database without Koha's checks. | Yes |

## Staff

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-27 | Import staff | An extra pass after the schools for persons with an active duty. The category is decided by the rules (one rule per role). The library is the school of the latest active duty. | Off by default. Needs duties in IST and "Use rules" ticked. A role without a rule is not imported. | Yes |
| F-28 | Existing staff left alone | Option: staff that already exist in Koha and were never handled by the plugin are not changed. | On by default. | Yes |

## Patrons who have left

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-29 | Mark patrons who have left | After a complete run, patrons not seen today are checked against IST. Those who are missing, or have no valid enrolment, are marked in the note. | Checks one person at a time against IST. | Yes |
| F-30 | Handle patrons who have left | Mode: nothing, report only, or move and remove. Per category you choose a category while loans are left and a category while no loans are left. The account can expire today. At most 200 changes per run (adjustable). | Only patrons in categories listed in the table that the import has handled. Off by default. | Yes |
| F-31 | Remove without loans | A patron without loans is removed after a number of days (default 30). Everything that identifies the person is removed, also from Koha's logs and the list of deleted patrons. | Koha can refuse removal, for example because of fees. The patron then stays. | Yes |
| F-32 | Anonymise when loans are left | A patron with loans left is anonymised after a number of days (default 300, empty means never). Only the account and the loans are kept. The account is removed when the loans are back. | | Yes |

## Runs and schedule

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-33 | Runs in rounds | The import goes school by school, one page at a time. The day is done when all schools are done. | The system administrator must add a cron line. At most 100 rounds (one page per school and round) and 8 hours per run. | Yes |
| F-34 | Restart when IST changed data | If IST answers that data changed, the school restarts from the first page, at most 10 times per school and day. | | Yes |
| F-35 | Schedule | Days and times for import and for "Fetch values from IST now". At most 50 rows. A start can be up to an hour late but never happens twice on the same day. | Server time. Needs the cron line that runs every minute. | Yes |
| F-36 | Run now | Buttons that start an import or a fetch of values within a minute. The import starts again from the first page. | Needs the cron line that runs every minute. | Yes |
| F-37 | Restart today's run | The button "Set API scanner from the first page" makes today's pages be processed again. | Active only when pages have been processed today. | Yes |
| F-38 | Job list and run status | Shows the latest jobs, a line when a run is in progress and a status that shows whether the minute job is active. | | Yes |
| F-39 | Protection against simultaneous runs | Two imports never run at the same time, whoever started them. A new job waits until the running one is done. | | Yes |
| F-40 | Tool (older page) | Starts a run from the web and shows log, statistics and updates. Has a button to unlock a run that got stuck. | Older page, not needed in normal operation. | Yes |

## Log and traceability

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-41 | Log: import runs | One row per processed page with school, counts, state and message. Filter, search and sort. The response from IST can be opened per row. | The whole response is stored only when "Debug Mode" is on. | Yes |
| F-42 | Log: patron changes | Shows changes to patrons: which fields changed, from and to. Each patron links to the patron record. | Changes are captured by a database function (trigger) that the plugin installs. | Yes |
| F-43 | Link under Administration > Plugins | Its own row "Imported SS12000 users (log)" that opens the log page. The log page is also reached from the configuration page. | Comes from a small companion plugin in the same package. | Yes |

## Data protection

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-44 | Debug mode | Option: full responses from IST are stored in the database and very detailed logging is written to the log file on the server. | Off by default in a new installation. Contains personal data when on. The log file contains some personal data even when off (see the manual, chapter Data protection). | Partly |
| F-45 | Pruning of the import log | You choose how many days the import log is kept (1 to 120, default 3). | Applies to import runs only. Patron changes and the log file on the server are not pruned by the plugin. | Partly |

## Operation

| ID | Function | What it does | Prerequisite or limitation | Included |
|---|---|---|---|---|
| F-46 | Installation and upgrade | The plugin is uploaded in Koha. Tables and the trigger are created. Settings are kept on upgrade. | Koha 20.05 or later, plugins enabled and the Perl module URI::Encode. | Yes |
| F-47 | Uninstall | The plugin's tables and trigger are removed. | Settings, rules and logs are lost. | Yes |
| F-48 | Languages | The interface follows the Koha language. | Swedish and English are available. Finnish is partly translated, the rest is shown in English. | Partly |
| F-49 | Permission | The pages need Koha's plugin permission. | | Yes |

## Not included (can be ordered as further development)

| ID | What is missing | Comment |
|---|---|---|
| N-01 | Guardians as Koha guarantors | IST can deliver the data, the plugin does not read it. |
| N-02 | Photo on the patron | IST can deliver the data, the plugin does not read it. |
| N-03 | Teaching groups and mentor groups | Only class groups are read. |
| N-04 | A separate setting for security marking (protected identity) | Today possible with a rule on "Other field (advanced)" (needs "Use rules") if IST delivers the data. |
| N-05 | A separate setting for persons who have emigrated or died | Same as N-04. |
| N-06 | Rules that set more fields than category | For example sort fields, attributes or the expiry date. |
| N-07 | Library (branch) as a rule in the rule editor | The engine can do it, the page has no editor for it. The library comes from the school mapping. |
| N-08 | More choices for which data is used | Middle name, address types other than registered address, other e-mail types, phone types and choice of external identifier type. |
| N-09 | Password and PIN code | The plugin sets no password and no PIN code. |
| N-10 | Messaging preferences for new patrons | New patrons are created without messaging preferences. |
| N-11 | First name in the preferred name field | The field is not filled. |
| N-12 | Enrolments that start in the future | An enrolment is imported only when it has started. |
| N-13 | Preview of rules | It is not possible to see what the rules would do before they are switched on. |
| N-14 | Rule decisions on a page in Koha | See F-15. |
| N-15 | Fetched values as choices in the rule editor | See F-03. |
| N-16 | Special handling of staff among patrons who have left | The function applies to the categories you add to the table. |
| N-17 | Circulation rules, library groups and messaging for new categories | These are Koha settings the library makes itself. |
| N-18 | Source systems other than IST | The plugin talks to IST's SS12000 interface. |
| N-19 | Stronger storage of the client secret | The secret is stored obscured (XOR), not with strong encryption. |

## Error or request?

- **Error (bug):** a function marked *Yes* or *Partly* does something other than the row says, under the stated prerequisites. Fixed at no cost.
- **Request (further development):** everything else: a function that is not on the list, something the list says the plugin does *not* do, or something beyond the stated limitation. Quoted.
- **Configuration:** it can be done with a setting on the page (category, rule, schedule, mapping). How it is charged is set in the agreement with each customer.
- **Depends on IST:** data that IST does not deliver, or a missing subscription. Neither an error nor a request: the plugin cannot fetch what is not sent.
- **When reporting an error, send:** version, what you did, what you expected, what happened, the time, and a screenshot of the page *Imported SS12000 users (log)*. Never send civic numbers.
