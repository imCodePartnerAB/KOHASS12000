# Specification of SS12000 Fields Used by Koha::Plugin::imCode::KohaSS12000::ExportUsers

Below is a comprehensive list of fields from the SS12000 standard utilized by the `Koha::Plugin::imCode::KohaSS12000::ExportUsers` plugin, sourced from the `/persons`, `/groupMemberships`, `/duties`, and `/organisations` endpoints, their mappings to the Koha `borrowers` table, and additional notes. Fields are grouped by their corresponding SS12000 datasets.

## Dataset: Persons

- **id**
  - **Purpose**: Unique identifier for the person, used for queries to `/duties` and `/groupMemberships` endpoints and logging.
  - **Example**: `"person123"`.

- **givenName**
  - **Mapping**: `firstname` in `borrowers` table.
  - **Purpose**: User's first name.
  - **Example**: `"Anna"`.

- **familyName**
  - **Mapping**: `surname` in `borrowers` table.
  - **Purpose**: User's last name.
  - **Example**: `"Andersson"`.

## Dataset: Birthdate

- **birthDate**
  - **Mapping**: `dateofbirth` in `borrowers` table.
  - **Purpose**: User's date of birth in `YYYY-MM-DD` format.
  - **Example**: `"2005-03-15"`.

## Dataset: Sex

- **sex**
  - **Mapping**: `sex` in `borrowers` table.
  - **Purpose**: User's gender, transformed from `"Man"` to `"M"` or `"Kvinna"` to `"F"`.
  - **Example**: `"M"` or `"F"`.

## Dataset: Emails

- **emails**
  - **Mapping**: `email` and `B_email` in `borrowers` table.
  - **Purpose**:
    - Email with `type="Privat"` maps to `B_email` (lowercase).
    - First non-private email (e.g., `type="Skola"`) maps to `email` (lowercase).
  - **Example**: `[{ "type": "Privat", "value": "anna@example.com" }, { "type": "Skola", "value": "anna.school@example.com" }]` → `B_email: anna@example.com`, `email: anna.school@example.com`.

## Dataset: Addresses

- **addresses**
  - **Mapping**: `address`, `city`, `zipcode`, `country` in `borrowers` table.
  - **Purpose**: Address with type matching `Folkbokföring` (or similar):
    - `streetAddress` → `address` (title case, e.g., "Storgatan 12").
    - `locality` → `city` (e.g., "Stockholm").
    - `postalCode` → `zipcode` (e.g., "12345").
    - `country` → `country` (e.g., "Sweden").
  - **Non-mapped fields**: `countyCode`, `municipalityCode`, `realEstateDesignation` (logged but not stored in `borrowers`).
  - **Example**: `{ "type": "Folkbokföring", "streetAddress": "Storgatan 12", "locality": "Stockholm", "postalCode": "12345", "country": "Sweden" }`.

## Dataset: Phone numbers

- **phoneNumbers**
  - **Mapping**: `phone` and `mobile` in `borrowers` table.
  - **Purpose**:
    - `mobile: true` → `mobile`.
    - `mobile: false` → `phone`.
  - **Example**: `[{ "value": "+46701234567", "mobile": true }, { "value": "+4681234567", "mobile": false }]` → `mobile: +46701234567`, `phone: +4681234567`.

## Dataset: CivicNo

- **civicNo**
  - **Mapping**: `cardnumber` and `userid` in `borrowers` table.
  - **Purpose**: Used as `cardnumber` and `userid` if `cardnumberPlugin` or `useridPlugin` is set to `"civicNo"`.
  - **Example**: `{ "value": "20050315-1234" }` → `cardnumber: 20050315-1234`, `userid: 20050315-1234`.

## Dataset: Identifiers

- **externalIdentifiers**
  - **Mapping**: `cardnumber` and `userid` in `borrowers` table.
  - **Purpose**: Used as `cardnumber` and `userid` if `cardnumberPlugin` or `useridPlugin` is set to `"externalIdentifier"`.
  - **Example**: `[{ "value": "EXT123456" }]` → `cardnumber: EXT123456`, `userid: EXT123456`.

## Dataset: Enrolments (part of Persons)

- **enrolments**
  - **Fields**: `enroledAt.id`, `startDate`, `endDate`, `cancelled`.
  - **Purpose**: Identifies active enrolment (where `cancelled: false` and `startDate` ≤ current date ≤ `endDate`) to fetch `organisationCode` from `/organisations/{enroledAt.id}`.
  - **Example**: `{ "enroledAt": { "id": "org123" }, "startDate": "2024-08-01", "endDate": "2025-06-30", "cancelled": false }`.

## Dataset: GroupMemberships

- **groupMemberships.group.displayName**
  - **Mapping**: `attribute` in `borrower_attributes` table (code `CL`).
  - **Purpose**: Class name for groups where `groupType="Klass"` and active within `startDate` ≤ current date ≤ `endDate`.
  - **Example**: `{ "group": { "groupType": "Klass", "displayName": "9A", "startDate": "2024-08-01", "endDate": "2025-06-30" } }` → `attribute: 9A`.

- **groupMemberships.group.groupType**
  - **Purpose**: Filters groups to only those with `groupType="Klass"`.
  - **Example**: `"Klass"`.

- **groupMemberships.group.startDate**, **groupMemberships.group.endDate**
  - **Purpose**: Ensures the group is active based on the current date.
  - **Example**: `startDate: "2024-08-01"`, `endDate: "2025-06-30"`.

## Dataset: Duties

- **dutyRole**
  - **Mapping**: `categorycode` in `borrowers` table via `imcode_categories_mapping`.
  - **Purpose**: Maps role (e.g., "Lärare") to a Koha category code (e.g., "SKOLA"). Users without a `dutyRole` may be excluded if `excluding_dutyRole_empty = Yes`.
  - **Example**: `{ "dutyRole": "Lärare" }` → `categorycode: SKOLA`.

## Dataset: Organisations

- **organisationCode**
  - **Mapping**: `branchcode` in `borrowers` table via `imcode_branches_mapping`.
  - **Purpose**: Maps organization code (e.g., "SCHOOL123") to a Koha branch code (e.g., "MAIN"). Defaults to `koha_default_branchcode` if unmapped.
  - **Example**: `{ "organisationCode": "SCHOOL123" }` → `branchcode: MAIN`.

## Mapping of SS12000 Fields to Koha `borrowers` Table

| **SS12000 Field**                              | **Koha `borrowers` Field** | **Notes**                                                                 |
|-----------------------------------------------|---------------------------|---------------------------------------------------------------------------|
| `givenName`                                   | `firstname`               | User's first name.                                                       |
| `familyName`                                  | `surname`                 | User's last name.                                                        |
| `birthDate`                                   | `dateofbirth`             | Date of birth in `YYYY-MM-DD` format.                                    |
| `sex`                                         | `sex`                     | Transformed: `"Man"` → `"M"`, `"Kvinna"` → `"F"`.                        |
| `emails[type="Privat"].value`                 | `B_email`                 | Private email, converted to lowercase.                                   |
| `emails[non-Privat].value`                    | `email`                   | First non-private email, converted to lowercase.                         |
| `addresses[type~="Folkbokföring"].streetAddress` | `address`               | Street address, title case (e.g., "Storgatan 12").                       |
| `addresses[type~="Folkbokföring"].locality`   | `city`                    | City name (e.g., "Stockholm").                                           |
| `addresses[type~="Folkbokföring"].postalCode` | `zipcode`                 | Postal code (e.g., "12345").                                             |
| `addresses[type~="Folkbokföring"].country`    | `country`                 | Country name (e.g., "Sweden").                                           |
| `phoneNumbers[mobile=true].value`             | `mobile`                  | Mobile phone number.                                                     |
| `phoneNumbers[mobile=false].value`            | `phone`                   | Landline phone number.                                                   |
| `civicNo.value`                               | `cardnumber`, `userid`    | Used if `cardnumberPlugin`/`useridPlugin` set to `"civicNo"`.             |
| `externalIdentifiers[0].value`                | `cardnumber`, `userid`    | Used if `cardnumberPlugin`/`useridPlugin` set to `"externalIdentifier"`.  |
| `dutyRole`                                    | `categorycode`            | Mapped via `imcode_categories_mapping` (e.g., "Lärare" → "SKOLA").       |
| `organisationCode` (from `/organisations`)     | `branchcode`              | Mapped via `imcode_branches_mapping` (e.g., "SCHOOL123" → "MAIN").       |
| `groupMemberships.group.displayName`          | `attribute` (`CL`)        | Stored in `borrower_attributes` as class name (e.g., "9A").               |

## Additional Notes

1. **Data Filtering**:
   - The plugin filters `/persons` data using:
     - `relationship.organisation`: Filters by organization ID.
     - `relationship.startDate.onOrBefore`: Ensures start date is on or before the current date.
     - `relationship.endDate.onOrAfter`: Ensures end date is on or after the current date.
     - `relationship.entity.type="enrolment"`: Filters for enrolment relationships.
   - If `excluding_enrolments_empty = Yes`, users without active enrolments are excluded.
   - If `excluding_dutyRole_empty = Yes`, users without a `dutyRole` are excluded.

2. **Duplicate Handling**:
   - Duplicates are checked in the `borrowers` table by `userid` and `cardnumber`. If multiple records exist, the most active record (based on loans, holds, etc.) is retained, and others are archived by prefixing `userid` and `cardnumber` with `ARCHIVED_`.

3. **Logging**:
   - All received fields are logged if `debug_mode = Yes`, including non-mapped fields like `countyCode`, `municipalityCode`, and `realEstateDesignation` from the `Addresses` dataset.

4. **Additional Attributes**:
   - The class name (`klass_displayName`) is stored in the `borrower_attributes` table with the code `CL`.

5. **Configuration**:
   - The plugin relies on `imcode_categories_mapping` and `imcode_branches_mapping` tables to map `dutyRole` to `categorycode` and `organisationCode` to `branchcode`.
   - If mappings are missing, defaults (`koha_default_categorycode`, `koha_default_branchcode`) are used.
   - Missing mappings may trigger an `ErrorVerifyCategorycodeBranchcode` error.