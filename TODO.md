# TODO: possible additions

Ideas found when the plugin was compared with the SS12000 API (version 2.1.0) and the Skolfederation
attribute profile (version 4.2). Nothing here is decided. The order inside each group is the suggested order.

Rules for anything on this list:
- A change that alters what existing installations do gets a setting, and the default keeps the old behaviour
  (unless the old behaviour is a data protection problem, then the default is the safe one and it is written in
  the changelog).
- Customer specific values never go into the code, the tests or this file. Use neutral examples.
- New values the administrator can choose are read from the API where possible (discovered values), and every
  text is translated.

Size: S = hours, M = a day or two, L = several days or needs a decision from the customers first.

## 0. Measured on the two installations that run the plugin today (2026-11-24)

Read only, counts only. The numbers are from the last full runs (about 21 000 and 9 000 persons).

- **One installation gets `eduPersonPrincipalNames` for practically every person** (20 981 of 20 990) and a secondary external identifier for all of them. It uses the civic number as user id and card number. The other gets neither.
- **Odd expiry dates on one installation, but not from the current code:** 2 614 of 18 703 imported patrons have no expiry date and 327 have a year between 2105 and 2109. All of them were last touched by older plugin versions (1.61 to 1.76); the 14 421 patrons touched this month all have normal dates, and the expiry code is the same in 1.90 and 2.0. They are most likely no longer in the register, so the leaver function (2.0, needs the fix for organisations that are missing in the register) is what deals with them.
- **No person has `securityMarking`, `Utvandrad` or `Avliden`** in either installation today.
- **E-mail:** every person has an address of the type `Skola elev`, about a quarter also `Privat`. Today this works as intended (school address as main, private as alternate).
- **Addresses of the type `Postadress`** exist for 3 to 4 percent of the persons and are ignored.
- **Guardians** (`responsibles`) are delivered for most persons in both. Only one installation wants staff imported; the other excludes every staff role.
- **Patrons who have left:** only a handful are marked today (the plugin has run for a short time), but it will be thousands after the end of a school year.

Order for the existing installations (code only; what an administrator switches on or off is their own configuration): the federation identifier as user id and the choice of external identifier, `securityMarking` and `personStatus` as ordinary rule fields, staff import, teaching and mentor groups. The leaver function is done in 2.0 and only needs a mapping per installation.

## 1. Safety and data protection (all installations)

Whether debug logging is on, how long logs are kept and which cron lines run are the customer's configuration and are not listed here.


| Item | API field | What is missing today | Size |
|---|---|---|---|
| Persons with protected identity | `Person.securityMarking` (`Sekretessmarkering`, `Skyddad folkbokföring`) | Works today for installations that use the rules (advanced field `securityMarking`, action do not import; shown by a test). Missing: an ordinary rule field with the discovered values, and a plain setting for installations that do not use rules. | S |
| Persons who have emigrated or died | `Person.personStatus` (`Utvandrad`, `Avliden`) | Same as above for rules. A deceased person who is already a patron should be handled at once like a patron who has left (removed or anonymized). | S |

## 2. Data quality for what is already imported

| Item | API field | What is missing today | Size |
|---|---|---|---|
| User id / card number from the federation identifier | `Person.eduPersonPrincipalNames` | Not an option (Skolfederation attribute 5). Needed for logins through the federation. | S |
| Choose the external identifier | `Person.externalIdentifiers[].context`, `globallyUnique` | The last identifier in the list wins. Add a setting for the context to use. | S |
| E-mail type order | `Person.emails[].type` (`Privat`, `Skola elev`, `Skola personal`, `Arbete övrigt`) | `Privat` becomes the alternate address, the first of the others the main address. Add a setting for which type goes where. | S |
| Address type order | `Person.addresses[].type` (`Folkbokföring`, `Särskild postadress`, `Tillfällig adress`, `Postadress`) | Only `Folkbokföring` is used; the others are ignored. Add a setting for the order of preference. | S |
| Phone type | `Person.phoneNumbers[].type` (`Hem`, `Arbete`) | Only mobile or not is used. | S |
| Middle name | `Person.middleName` | Not mapped (could go to `othernames`). | S |
| Sex `Okänt` | `Person.sex` | Check what is written to Koha for the third value. | S |

## 3. More ways to decide (rules)

| Item | What is missing today | Size |
|---|---|---|
| School unit code as a rule field | `Organisation.schoolUnitCode` (the national code) cannot be used, only the organisation code. Also `organisationType` and `municipalityCode`. | S |
| The branch as a rule field (`branch`) | Rules for a whole branch need one condition per school code. | S |
| Branch rules in the editor | The engine knows rules that set the branch (scope `branch`), the page has no editor for them. | M |
| Rules that set more than category and branch | Actions that set `sort1`, `sort2`, a patron attribute (school year, education code, school unit) or the expiry date. | M |
| Expiry date rule | The expiry date is always the end date of the enrolment. Offer a fixed date per rule, or the end date plus a number of days. | M |
| Duty details as rule fields | `Duty.dutyPercent`, `hoursPerYear`, `signature`, `description` (only the roles are available now). | M |
| Programme type | `Programme.type` (`Yrkesprogram`, `Högskoleförberedande program`, `Intruduktionsprogram` ...) needs the programmes endpoint, which some customers cannot read. Until then the education code is the way to recognise a programme. | M |
| Future enrolments | An enrolment that starts next term is not imported before it starts. A setting for "from N days before". | M |

## 4. New functions (ask the customers first)

| Item | Why it needs a decision | Size |
|---|---|---|
| Staff import | Written in the plan, not built: duty filter, one run without school filter, branch for staff with several schools, people who are both students and staff. | L |
| Teaching groups and mentor groups | Only `Klass` groups are read. `Undervisning` and `Mentor` groups as a patron attribute (Skolfederation attributes 27 and 28). | L |
| Guardians | `Person.responsibles` (`Vårdnadshavare`, `Annan ansvarig`, `God man`) as Koha guarantors. Sensitive; each customer must decide. | L |
| Photo | `Person.photo` to the patron image. | M |
| Deeper organisation hierarchies | The rule engine takes a list of organisations, but only the direct organisation and its parent are resolved. | L |

## 5. Smaller things

- Show the discovered values (classes, education codes, roles) as choices in the rule editor.
- Show the rule log (`imcode_rule_log`) on a page.
- A "show what would happen" mode for the rules before they are switched on.
- Translate the remaining server messages and the log summary lines.
- Keep the old per-year categories out of the leaver mapping by letting a row match several categories.
