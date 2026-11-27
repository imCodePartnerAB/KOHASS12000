# Changelog

All notable changes to the SS12000 plugin for Koha. One entry per version, newest first.
Dates are the dates of the release tags (1.77 to 1.90) or of the last commit of the release (1.91, 2.0).

## 2.0 (2026-11-27)

**Added**
- Category rules: decide the patron category with rules (conditions, action, priority; the first matching rule wins) instead of the role mapping. Conditions: school, school type, school year, education code, class, role, assignment role, age, person kind, school group, school of the duty, or any field of the person record by path. Operators: is one of, starts with, contains, is empty, is not empty, is at least, is at most. Action: a Koha category or *Do not import*. New setting *Use rules* (off by default, behaviour as in 1.91 when off) and a button that imports the role mapping as rules. The latest decision per person is stored in `imcode_rule_log`.
- Schedule and run now: import and *Fetch values from IST* on chosen weekdays and times, a *Run import now* button, a job list and a run status line. Needs one cron line that runs every minute (`cron/tick_ss12000.sh`).
- Protection against simultaneous runs: `run_ss12000.sh` takes a `flock`, the minute job waits for a running import, one job at a time, a schedule entry never runs twice on the same day. `run_ss12000.sh` accepts an optional instance name.
- Fetch values from IST: schools, school types, school years, education codes, classes and roles are read and stored for the rule editor (`imcode_seen_values`). No personal data is stored.
- Staff import: an extra pass after the schools for persons with an active duty. Category decided by rules, library from the school of the latest active duty. Off by default. Staff that already exist in Koha and were never handled by the plugin are left as they are (on by default).
- Patrons who have left: patrons missing from the register are moved to a category chosen per category (one while loans are left, one while no loans are left), removed after a number of days when they have no loans (default 30), or anonymised when loans are left (default 300 days). Modes: off, report only, move and remove. A limit on changes per run. Removal leaves no trace in Koha's action log, the plugin's change log, the rule log, the message queue or the list of deleted patrons. Off by default.
- Documentation: feature list (Swedish and English), manual and installation guide in Swedish, screenshots, and the scripts that recreate them (`Doc/tools`).

**Changed**
- Tables created on install and upgrade: `imcode_rules`, `imcode_rule_log`, `imcode_schedule`, `imcode_jobs`, `imcode_state`, `imcode_seen_values`, `imcode_leaver_mapping`, `imcode_leavers`. The configuration page and the cron job create missing tables.
- The change log no longer stores the civic number of a patron who is not in the register any more.
- The *Set API scanner from the first page* button is always shown and disabled when nothing has been processed today.
- A run is not complete while the staff pass is pending.
- Duties are read once per run when rules need roles, instead of one request per person.
- The old screenshots in `Doc/` were replaced by new ones made from example data.

**Fixed**
- The change log trigger is recreated only when the plugin version changed, and exactly one `trigger_version` row is kept (older versions could leave several, which recreated the trigger on every page view).
- Merging two patrons removes identical patron attribute rows and records the merge in the change log (action `merge`).
- The run-now button posts the class, method and operation in the request body and with an operation that starts with `cud-`, as Koha 26.05 requires.

## 1.91 (2026-11-19)

**Added**
- New configuration page with sections: connection, patron identity, import rules, branches mapping, categories mapping, advanced. Also reachable from Administration > Plugins.
- Connection test, load organisations and roles from IST, and a suggested branch mapping from names. Several organisations can share a branch.
- Log viewer (import runs and patron changes) with search, sorting, filters and the full API response per row. A small companion plugin (*Imported SS12000 users (log)*) adds its own entry under Administration > Plugins.
- Patron identity settings: find existing patrons by user id, card number and/or a patron attribute; user id from civic number, external identifier or school e-mail (part before @, or the whole address); card number from civic number, external identifier or keep the existing value; civic number attribute; class attribute.
- Optional text appended to imported e-mail addresses (for example `.NOEMAIL`), with an on/off checkbox.
- Options: keep the category of existing patrons, keep existing contact details when IST has no value.
- Translations as plain `.po` files read directly by the plugin (Swedish, partly Finnish). No compiled `.mo` files and no system locale.
- An *About* box on the configuration page.

**Fixed**
- HTTP 410 (`NEW_DATA_RESTART_FROM_FIRST_PAGE`) restarts the organisation from the first page (at most 10 times per organisation and day).
- Organisations that do not exist in IST no longer block the end of a run, and the cron wrapper no longer loops on them.
- The class is chosen from classes where both the group and the student's membership are valid today, preferring the school of the active enrolment.
- A new patron never takes a user id or card number that already belongs to someone else.
- A known date of birth is never erased; it is derived from the civic number when IST does not deliver it.
- The category (role) mapping is optional. Roles flagged *do not import* are kept when saving. A new installation creates no example role mappings.
- Civic number matching compares digits only.
- When the duties endpoint answers 403 it is asked once per run.
- The plugin log is written as UTF-8.

## 1.90 (2026-03-19)

**Added**
- Setting `use_default_for_unmapped`: an extra pass without an organisation filter puts students from schools that are not in the branch mapping on the default branch. Off by default.
- Setting `ignore_cancelled_flag`: ignore the *cancelled* flag on enrolments and rely on start and end dates only.

## 1.89 (2026-03-12)

**Fixed**
- HTTP 410 Gone for expired page tokens is handled in `fetchDataFromAPI`.

## 1.88 (2026-03-10)

**Changed**
- Every SS12000 patron gets its OPAC note updated, also through API verification and post-sync processing.

## 1.87 (2026-03-06)

**Fixed**
- Forked child process id handling, a lock file shared by cron and the web tool, the `page_completed` status, numeric comparison warnings in the templates.

**Added (1.87-1, 2026-03-06)**
- The OPAC note is updated for users that are skipped because of cancelled enrolments.

**Added (1.87-15, 2026-03-10)**
- OPAC note tracking for every state of a user: added, updated, skipped, missing in the API.

## 1.83 (2026-02-28)

**Changed**
- The `dateexpiry` field is tracked by the change log trigger. The trigger is recreated when the configuration page is opened and, from 1.84, only when the plugin version changed.

## 1.82 (2026-02-27)

**Added**
- Setting `dateexpiry_fallback`: what to set when IST gives no valid enrolment end date (none, keep, or a number of months from now). The setting is read once per batch.

## 1.81 (2026-02-26)

**Fixed**
- `dateexpiry` was not set, the OPAC note showed a stale version, and an SQL comment caused a syntax error.

## 1.79 (2026-02-23)

**Fixed**
- A `RESET` marker is used instead of `NULL` when logs are cleared, so organisations are not skipped.
- The organisation code is resolved through the parent organisation for child organisations without their own code.

## 1.78 (2026-02-05)

**Fixed**
- `NULL` in the `UPDATE` of `dateexpiry`. The trigger compares with the NULL-safe `<=>`.
- 1.78.1: more debug logging for HTTP 404 when looking up an organisation id.

## 1.77 (2026-02-05)

**Fixed**
- `enrolment_end_date` was not read from the API.

## 1.76 and earlier

See the tags `v1.3` to `v1.76` on GitHub. A Swedish change list for 1.76 to 1.90 is in `Doc/CHANGELOG-1.76-to-1.90.md`.
