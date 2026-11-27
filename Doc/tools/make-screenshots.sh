#!/bin/bash
# Gör om skärmbilderna till manualen.
#
# Kräver: en demoinstans med pluginet installerat och demo_seed.sql körd, ett konto med plugin-behörighet och
# behörighet att se låntagare, Cypress (npx cypress) och ImageMagick (convert).
#
# Inget om en viss server, port eller inloggning står i repot. Allt anges i miljön när skriptet körs:
#   DEMO_URL      adressen till personalgränssnittet, till exempel https://biblioteket-intra.example.invalid
#   DEMO_USER     kontot som loggar in
#   DEMO_PASS     lösenordet (läggs i miljön, aldrig i kommandoraden eller i en fil i repot)
#   DEMO_SQL_CMD  ett kommando som läser SQL från standard in och kör det mot demoinstansens databas,
#                 till exempel "koha-mysql <instans>" när skriptet körs på servern
#   CYPRESS_DIR   katalog där Cypress är installerat (node_modules/.bin/cypress finns)
#   DEMO_BORROWER (valfritt) låntagarnumret för exempellåntagaren, annars söks hon fram
#
# Resultat: Doc/images/NN-beskrivning.png (utan metadata).

set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out="$here/../images"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
: "${DEMO_URL:?}" "${DEMO_USER:?}" "${DEMO_PASS:?}" "${DEMO_SQL_CMD:?}" "${CYPRESS_DIR:?}"
mkdir -p "$out" "$tmp/keep"

run_sql() { # $1 = sql-fil
    # shellcheck disable=SC2086
    $DEMO_SQL_CMD < "$here/$1"
}

cypress_run() { # $1 = spec, $2 = extra env (valfritt)
    (cd "$CYPRESS_DIR" && CYPRESS_KOHA_USER="$DEMO_USER" CYPRESS_KOHA_PASS="$DEMO_PASS" \
        npx cypress run --spec "$here/$1" \
        --config "baseUrl=$DEMO_URL,screenshotsFolder=$tmp/shots,video=false,supportFile=false,specPattern=$here/*.cy.js" \
        ${2:+--env "$2"})
    # varje körning skriver över förra körningens mapp, så bilderna sparas direkt
    find "$tmp/shots" -name '*.png' -exec cp {} "$tmp/keep/" \;
}

run_sql demo_cron_saknas.sql
cypress_run docs_screenshots.cy.js
cypress_run docs_screenshots_language.cy.js
cypress_run docs_screenshots_status.cy.js "SHOT_NAME=12-cron-saknas"
run_sql demo_cron_aktiv.sql
cypress_run docs_screenshots_status.cy.js "SHOT_NAME=11-cron-aktiv"
run_sql demo_running.sql
cypress_run docs_screenshots_status.cy.js "SHOT_NAME=13-schema-pagaende"
run_sql demo_running_remove.sql
run_sql demo_cron_saknas.sql

# Ta bort metadata och lägg bilderna i Doc/images
for f in "$tmp"/keep/*.png; do
    convert "$f" -strip "$out/$(basename "$f")"
done
echo "Klart: $(ls "$out"/*.png | wc -l) bilder i $out"
