# Installation: SS12000-plugin för Koha

Version 2.0. Den här texten är till för systemadministratören. Bibliotekets administratör använder [Manual_sv.md](Manual_sv.md). Vad pluginet gör står i [Funktionslista.md](Funktionslista.md).

## 1. Krav

| Krav | Kommentar |
|---|---|
| Koha 20.05 eller nyare | Pluginets `minimum_version`. Installerat och kontrollerat på Koha 26.05. |
| Plugin påslagna i Koha | I `/etc/koha/sites/<instans>/koha-conf.xml`: `<enable_plugins>1</enable_plugins>` och `<plugins_restricted>0</plugins_restricted>` om pluginet ska laddas upp via webben. Starta om Plack efter ändring. |
| Perl-modulen `URI::Encode` | Utan den visar konfigurationssidan en felruta om en saknad modul. På Debian/Ubuntu: `sudo apt-get install liburi-encode-perl`. |
| MariaDB med `information_schema` och triggers | Pluginet skapar en trigger på tabellen `borrowers`. Databasanvändaren för Koha måste ha rättighet att skapa triggers. |
| Nätverksaccess till IST | Pluginet anropar `https://api.ist.com` (API) och `https://skolid.se/connect/token` (åtkomst), eller de adresser ni anger på konfigurationssidan. |
| IST-uppgifter | Kund-id, klient-id och klienthemlighet. Uppdrag (duties) måste ingå i IST-beställningen för personal och rollvillkor. |
| För cron | `bash`, `flock`, `pgrep`, `timeout` och `sudo koha-shell` för den användare som kör cron (vanligen root). |

Tillgängliga plugin-listor och plugin-behörigheter styrs av Koha: sidorna kräver Kohas plugin-behörighet för metoden (`configure` för konfigurationssidan, `admin` för logglänken under Administration > Plugins, `tool` för verktygssidan).

## 2. Installation

Pluginet levereras som en `.kpz`-fil (`koha-plugin-export-users_ss12000_v2_0.kpz`). Den innehåller två plugin: huvudpluginet "Export av användare från SS12000" och ett litet följeplugin "Importerade SS12000-användare (logg)" som ger logglänken under Administration > Plugins.

**Via webben.**
1. Logga in i Koha som en användare som får hantera plugin. Gå till Administration > Plugins > Ladda upp plugin.
2. Välj `.kpz`-filen och ladda upp. Koha packar upp filen och kör pluginets installation.
3. Kontrollera i pluginlistan att båda plugin står med version 2.0 och är aktiverade.
4. Starta om Plack för instansen (Plack håller gammal kod i minnet): `sudo koha-plack --restart <instans>`.

**På servern (om uppladdning inte går).**
```bash
sudo koha-shell <instans> -c "cd /var/lib/koha/<instans>/plugins && unzip -o /sökväg/till/koha-plugin-export-users_ss12000_v2_0.kpz"
sudo koha-shell <instans> -c "perl /usr/share/koha/bin/devel/install_plugins.pl"
sudo koha-plack --restart <instans>
```

`install_plugins.pl` gör om alla plugin på instansen. Ett fel i ett annat plugin visas då i utskriften men påverkar inte detta plugin.

**Vad installationen gör.** Den skapar tabellerna `imcode_config`, `imcode_logs`, `imcode_data_change_log`, `imcode_categories_mapping`, `imcode_branches_mapping`, `imcode_rules`, `imcode_rule_log`, `imcode_schedule`, `imcode_jobs`, `imcode_state`, `imcode_seen_values`, `imcode_leaver_mapping` och `imcode_leavers`, lägger in standardinställningar och skapar databastriggern `log_user_changes` på `borrowers`.

**Kontrollera installationen.** Öppna Administration > Plugins > "Export av användare från SS12000" > Åtgärder > Konfigurera. Sidan ska visa version 2.0. När konfigurationssidan öppnas första gången skapas även de inställningar som tillkommit sedan tidigare versioner, och triggern skapas om om den inte har pluginets version.

## 3. Uppgradering

Ladda upp den nya `.kpz`-filen på samma sätt som vid installation. Koha kör pluginets uppgradering. Den:
- lägger till saknade kolumner i `imcode_logs`,
- skapar tabeller som saknas (till exempel de som tillkommit i 2.0: `imcode_rules`, `imcode_rule_log`, `imcode_schedule`, `imcode_jobs`, `imcode_state`, `imcode_seen_values`, `imcode_leaver_mapping`, `imcode_leavers`),
- skapar om databastriggern.

Befintliga inställningar, skolmappning och kategorimappning behålls. Nya inställningar får standardvärden som ger samma beteende som förut (till exempel "Använd regler" av och "Låntagare som slutat" av). Starta om Plack efter uppgraderingen.

**Om filerna kopierats för hand** och pluginets version inte ändrats i Koha körs inte uppgraderingen. Konfigurationssidan och cron-jobbet skapar då saknade tabeller när de körs. Kör annars `install_plugins.pl` (se ovan).

**Kontroll efter uppgradering.** I databasen ska `select value from imcode_config where name='trigger_version'` ge exakt en rad med pluginets version.

## 4. Cron

Pluginet har två sätt att köra importen. Använd det nya (minutjobbet) för schema och **Kör nu** på konfigurationssidan. Den äldre cron-raden fungerar fortfarande.

**A. Minutjobbet (rekommenderas).** En cron-rad som körs varje minut. Skriptet gör inget om ingen körning ska starta:
```
* * * * * root /var/lib/koha/<instans>/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/tick_ss12000.sh >> /var/log/koha/<instans>/tick_ss12000.log 2>&1
```
Skriptet:
- skriver en puls som konfigurationssidan visar,
- skapar jobb för schemarader som är inne (från klockslaget och upp till en timme framåt, aldrig två gånger samma dag),
- startar högst ett jobb och kör importen med `run_ss12000.sh <instans>`.

Skriptet hittar själv alla instanser som har pluginet under `/var/lib/koha`.

**B. Äldre cron-rad.** Kör `run_ss12000.sh` på valda tider. Den går igenom alla instanser med pluginet:
```
4 */12 * * * root /bin/timeout 8h /var/lib/koha/<instans>/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run_ss12000.sh
```
Skriptet hämtar en sida per anrop, upp till 100 omgångar, och stoppar efter 8 timmar. Det har ett skydd mot dubbelkörning (`flock` på `/run/lock/imcode_ss12000_run.lock`, eller `/tmp/imcode_ss12000_run.lock` om `/run/lock` inte är skrivbar) och en kontroll av att inget `imcode_ss12000.pl` redan körs.

Du kan köra B tillsammans med A. Två importer körs aldrig samtidigt: den som kommer andra avbryts (B) eller väntar (A).

**Efter ändring i cron.** Starta om cron om cron.d-filen inte läses in automatiskt: `sudo /etc/init.d/cron restart`.

**Köra för hand.** Allt som behövs för att starta en körning direkt:
```
sudo /var/lib/koha/<instans>/plugins/Koha/Plugin/imCode/KohaSS12000/ExportUsers/cron/run_ss12000.sh <instans>
```
(Instansnamnet är valfritt. Utan det körs alla instanser.) Skriptet avbryter om det hittar en annan process vars kommandorad innehåller `imcode_ss12000.pl`. Starta det därför inte från ett kommando som självt innehåller det namnet.

**Statusrutan på konfigurationssidan.** "Schemaläggningen är aktiv" visas när minutjobbet setts de senaste tre minuterna. "Schemaläggningen är inte aktiv" betyder att minutjobbet aldrig körts. Pulsen skrivs i tabellen `imcode_state` (`tick_last_seen`).

## 5. Behörigheter

- **Koha.** Pluginets sidor kräver att användaren har plugin-behörighet i Koha. Behörigheten följer metodnamnet (`configure`, `admin`, `tool`). Loggsidan ligger under `admin`/`configure`.
- **Server.** Cron kör skripten som root (eller en användare med `sudo koha-shell`). Importen körs sedan som instansens användare (`<instans>-koha`) via `koha-shell`.
- **Filer.** Pluginets loggfil `imcode-export-users.log`, låsfilen och statusfilen ligger i Kohas loggkatalog (`/var/log/koha/<instans>/`) och måste kunna skrivas av `<instans>-koha`.
- **Databas.** Koha-användaren måste kunna `CREATE TABLE`, `CREATE TRIGGER` och `DROP TRIGGER`.

## 6. Avinstallation

Avinstallation i Koha (Administration > Plugins > Ta bort) tar bort alla pluginets tabeller (de 13 ovan) och triggern `log_user_changes`. **Alla inställningar, mappningar, regler och loggar försvinner.** Låntagarna i Koha påverkas inte.

Ta bort cron-raderna för `tick_ss12000.sh` och `run_ss12000.sh` så att de inte försöker köra.

## 7. Felsökning

| Symptom | Orsak och åtgärd |
|---|---|
| Pluginet syns inte | Kontrollera `enable_plugins`, `plugins_restricted` och `pluginsdir` i `koha-conf.xml`. Starta om Plack. |
| Felruta om saknad modul | `sudo apt-get install liburi-encode-perl`, starta om Plack. |
| 500-fel eller "Can't locate object method" efter filkopiering | Plack kör gammal kod. `sudo koha-plack --restart <instans>`. Nya plugin-metoder registreras av `install_plugins.pl`. |
| Logglänken saknas under Administration > Plugins | Följepluginet är inte installerat. Ladda upp `.kpz` igen eller kör `install_plugins.pl`. |
| "Schemaläggningen är inte aktiv" | Minutjobbet saknas i cron. Se avsnitt 4. Kör `sudo <sökväg>/tick_ss12000.sh` för hand och se utskriften. |
| Importen körs inte | Kör `run_ss12000.sh` för hand och läs utskriften. "Error occurred" betyder fel i skolmappning eller standardkategori (se konfigurationssidans varning). "Script already running" betyder att en annan körning har låset. |
| Importen hoppar över skolor | Se loggsidan. Raden med "does not exist in IST" betyder fel organisationskod i skolmappningen. |
| HTTP 410 om och om igen | IST ändrar data under körningen. Pluginet startar om skolan upp till 10 gånger per dag. |
| 403 från IST på uppdrag | Uppdrag ingår inte i IST-beställningen. Det går inte att rätta i pluginet. |
| Fel vid uppdatering av låntagare efter att databasen kopierats från en annan server | Triggern `log_user_changes` kan vara skapad för en databasanvändare som inte finns på den nya servern. Öppna konfigurationssidan eller kör `install_plugins.pl` så skapas den om. |
| Loggfilen växer | Debuggläge är på. Stäng av det. Loggfilen roteras av serverns logrotate. |

**Loggfiler.** Pluginets loggfil: `/var/log/koha/<instans>/imcode-export-users.log` (UTF-8). Läs stora loggfiler med `tail` och `nice`, inte hela. Läs aldrig gamla loggar med `grep` utan att begränsa dem.

## 8. Dataskydd i drift

- **Debuggläge** ska vara av i normal drift. Med debuggläge på skrivs personuppgifter i loggfilen och hela IST-svar sparas i databasen (importloggen rensas efter inställd tid).
- **Loggfilen** kan även med debuggläge av innehålla enstaka personnummer och fältvärden (se kapitlet Dataskydd i Manual_sv.md). Begränsa åtkomst till `/var/log/koha/<instans>/`.
- **Kohas logrotate** (`/etc/logrotate.d/koha-common`) roterar `/var/log/koha/*/*.log` veckovis. Pluginets loggfil omfattas om inget annat anges.
- **Klienthemligheten** sparas förvrängd (XOR) i databasen, inte med stark kryptering. Begränsa tillgång till databasen. Nyckeln som används finns i koden.
- **Säkerhetskopiera databasen** innan funktionen för låntagare som slutat ställs på "Flytta och ta bort".
- **Dubbletter och sammanslagning** görs direkt i databasen. Ta en säkerhetskopia innan första importen mot ett befintligt låntagarregister.

## 9. Testa pluginets enhetstester

I repots rot:
```bash
prove t/
```
Kräver bara Perl (inga Koha-moduler). Testerna täcker regelmotorn, schemat och hanteringen av låntagare som slutat. De provar inte import mot IST eller databas.
