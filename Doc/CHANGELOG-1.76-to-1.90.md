# Ändringslogg: version 1.76 → 1.90

## För bibliotekarien

| # | Vad har förändrats? |
|---|---|
| 1 | **Slutdatum på lånekort synkroniseras nu från IST** — kortet löper ut i takt med elevens inskrivningsperiod, inte ett fast datum |
| 2 | **Inställning för vad som händer om IST saknar slutdatum** — man kan välja: behåll befintligt datum, sätt X månader framåt, eller ta bort |
| 3 | **OPAC-notering (opacnote) uppdateras mer heltäckande** — alla lånekort som berörs av synken (tillagda, uppdaterade, hoppade över) får nu en notering om vad som hände och när |
| 4 | **Borttagna inskrivningar hanteras bättre** — användare vars inskrivning är markerad som "cancelled" kan nu antingen ignoreras eller behandlas, via ny inställning |
| 5 | **Bugfix: barn-skolor utan eget organisationsnummer** matchas nu korrekt mot moderorganisationen |
| 6 | **Stabilare synk** — problem med att vissa skolor hoppades över vid omstart av synken är åtgärdade |
| 7 | **Ny valfri inställning: användare från skolor som saknas i mappningen** — de kan nu synkas med en standardfilial istället för att hoppas över (av som standard) |

---

## Teknisk ändringslogg per version

| Version | Ändring |
|---|---|
| **1.77** | Fix: `enrolment_end_date` hämtades inte från API |
| **1.78** | Fix: NULL-krasch i UPDATE för `dateexpiry`; NULL-safe `<=>` i DB-trigger istället för `!=` |
| **1.78.1** | Utökad debug-loggning för `get_organisation_id` 404-fel |
| **1.79** | Fix: RESET-markör istället för NULL vid logrensning (förhindrade att orgar hoppades över); fix för barn-orgar utan eget `organisationCode` |
| **1.82** | Ny konfigurationsparameter `dateexpiry_fallback` (none/keep/N månader); `dateexpiry` läses nu en gång per batch, inte per användare |
| **1.83** | `dateexpiry`-fältet spåras nu i `borrowers` change-log-triggern |
| **1.84–1.85** | DB-triggern återskapas automatiskt vid konfigsidan (och sedan: bara om pluginversionen ändrats) |
| **1.87** | Fix av fork child PID-hantering; delad lock-fil för cron+HTTP; `page_completed`-status; fixar i TT numerisk jämförelse |
| **1.87-1** | Opacnote-uppdatering även för användare som hoppas över pga. cancelled enrolments |
| **1.87-15** | Heltäckande opacnote-spårning för alla användartillstånd (tillagd/uppdaterad/hoppades över/saknas i API) |
| **1.88** | Alla SS12000-användare får opacnote-uppdatering via API-verifiering även i post-sync |
| **1.89** | Fix: hanterar HTTP 410 Gone för utgångna `pageToken` i `fetchDataFromAPI` |
| **1.89-5** | Ny inställning `ignore_cancelled_flag` — ignorerar cancelled-flaggan och förlitar sig enbart på start/slutdatum |
| **1.90** | Ny inställning `use_default_for_unmapped` — extra synk-pass utan organisationsfilter som lägger användare från omappade organisationer på default branchcode (avstängd som standard) |
