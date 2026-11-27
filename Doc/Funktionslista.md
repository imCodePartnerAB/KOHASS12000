# Funktionslista: SS12000-plugin för Koha

**Produkt:** SS12000-plugin för Koha (i Koha: "Export av användare från SS12000")
**Version:** 2.0
**Datum:** 2026-11-27

**Allt som anges som Ja ingår. Allt som inte finns på den här listan är vidareutveckling och offereras separat.**

Pluginet läser elever (och personal, om ni har beställt det) från skolans elevregister via IST/SS12000 och skapar och uppdaterar låntagare i Koha. Hur man ställer in det beskrivs i [manualen](Manual_sv.md). English version: [Feature_list_en.md](Feature_list_en.md).

Kolumnen *Ingår* har tre värden: **Ja** (funktionen finns som raden beskriver), **Delvis** (funktionen finns, men med den begränsning som står i raden) och **Nej** (finns inte, se "Ingår inte"). Texter inom citattecken är rubriker och knappar som de står på sidan i Koha.

## Anslutning

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-01 | Anslutning till IST | Hämtar elever (och personal) från IST. Nycklarna anges på sidan. Hemligheten visas aldrig igen efter att den sparats. | Kräver kund-id, klient-id och klienthemlighet från IST. | Ja |
| F-02 | Testa anslutningen, hämta skolor och roller | Knappar som visar om anslutningen fungerar, hur många skolor IST har och om personaluppgifter (uppdrag) ingår. De fyller också rullistorna för skolor och roller. | Använder de sparade inställningarna. Roller kräver att uppdrag ingår i IST-beställningen. | Ja |
| F-03 | Hämta värden från IST | Läser skolor, skoltyper, årskurser, utbildningskoder, klasser och roller och sparar dem. Inga personuppgifter sparas. | Värdena används ännu inte som val i regelredigeraren. | Delvis |

## Elever

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-04 | Importera elever | Skapar nya låntagare och uppdaterar befintliga för elever med giltig inskrivning idag. Går en sida i taget (standard 30 personer per sida). | Kräver skolmappning och en standardkategori. | Ja |
| F-05 | Uppgifter som förs över | Namn, personnummer, födelsedatum, kön, e-post, telefon, mobil, adress, bibliotek, klass och utgångsdatum. Saknas födelsedatum i IST räknas det ut ur personnumret. | Adress: bara folkbokföringsadressen. Kön: Man och Kvinna omvandlas till M och F. Skolans e-post blir huvudadress, privat e-post blir alternativ adress. | Ja |
| F-06 | Klass | Klassen sparas som låntagarattribut och följer eleven när eleven byter klass. | Bara klassgrupper. Attributtypen CL skapas automatiskt, andra typer måste redan finnas. | Ja |
| F-07 | Utgångsdatum | Sätts till inskrivningens slutdatum. Saknas slutdatum väljer ni: behåll befintligt, lämna tomt eller X månader framåt. | Har eleven flera giltiga inskrivningar används den som började senast. | Ja |
| F-08 | Uteslut personer utan inskrivning | Val: personer utan giltig inskrivning importeras inte. | Av som standard. | Ja |
| F-09 | Avbrutna inskrivningar | Val: ignorera IST:s markering "cancelled" (avbruten) och titta bara på start- och slutdatum. | Utan valet räknas avbrutna inskrivningar inte som giltiga. | Ja |
| F-10 | Anteckning på låntagaren | Varje låntagare som importen rört får en anteckning (OPAC-anteckning) med version, tid och vad som ändrats. | Kohas OPAC visar OPAC-anteckningen för låntagaren. Anteckningen nämner fältnamn, inte värden. | Ja |

## Kategorier

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-11 | Standardkategori | Alla som inte får en annan kategori får standardkategorin. | Måste vara en kategori som finns i Koha. | Ja |
| F-12 | Kategori per roll (äldre läge) | Kopplar en roll i IST till en låntagarkategori. En roll kan markeras "Importera inte". | Kräver uppdrag i IST. Bara personens första uppdrag används. Valet "Exkludera tomma användare, dutyRole" utesluter personer utan roll. Rollmappningen och det valet används inte när "Använd regler" är ikryssad. | Ja |
| F-13 | Kategoriregler | Regler med villkor och åtgärd. Villkor: skola, skoltyp, årskurs, utbildningskod, klass, roll, uppdragsroll, ålder, persontyp, skolgrupp, skola för uppdraget eller annat fält. Åtgärd: en kategori eller "Importera inte". Prioritet avgör ordningen, första träff vinner. | Rollvillkor kräver uppdrag i IST. Åtgärden kan bara vara en kategori eller "Importera inte". | Ja |
| F-14 | Importera rollmappningen som regler | Knapp som gör om rollmappningen till regler. Den sparar inte själv. | | Ja |
| F-15 | Regelbeslut per person | Pluginet sparar vilken regel som avgjorde för varje person och vilka andra regler som också träffade. | Sparas i databasen och visas inte på någon sida i Koha. Rader som inte setts på 90 dagar tas bort. | Ja |

## Skolor och bibliotek

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-16 | Skolmappning | Kopplar varje skola i IST till ett bibliotek (filial) i Koha. Flera skolor kan dela bibliotek. | En enhet utan egen organisationskod använder den överordnade enhetens kod. | Ja |
| F-17 | Förslag på skolmappning | Föreslår bibliotek utifrån namnen. Ni granskar och sparar. | Föreslår bara när namnen är mycket lika. | Ja |
| F-18 | Standardbibliotek för skolor som saknas i mappningen | Val: elever från skolor utan mappning läggs på standardbiblioteket i en extra omgång. | Av som standard. | Ja |
| F-19 | Skola som saknas i IST | En skola i mappningen som inte finns i IST markeras som klar för dagen och loggas. Körningen stoppas inte. | | Ja |

## Identitet och kontaktuppgifter

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-20 | Hitta befintliga låntagare | Via användar-ID, kortnummer och/eller personnummerattribut, i valfri kombination. Hittas två låntagare för samma person slås de ihop: den äldre arkiveras och lån, reservationer och avgifter flyttas. | Standard: användar-ID och kortnummer. | Ja |
| F-21 | Personnummer i attribut | Personnumret sparas i en attributtyp ni väljer och används för matchning. Bara siffror jämförs. | Attributtypen måste finnas i Koha. | Ja |
| F-22 | Användar-ID och kortnummer | Välj källa: personnummer, externt id, skolans e-post (före @ eller hela adressen) eller behåll befintligt värde. | Nya låntagare utan värde från vald källa får skolans e-postnamn, därefter personnumret. Pluginet sätter inget lösenord. | Ja |
| F-23 | Text efter e-postadresser | Val: en text (till exempel .NOEMAIL) läggs sist på alla importerade e-postadresser så att Koha inte skickar meddelanden före driftstart. | Av som standard. | Ja |
| F-24 | Behåll befintlig kategori | Val: kategorin sätts bara för nya låntagare. | Av som standard. | Ja |
| F-25 | Behåll befintliga kontaktuppgifter | Val: en tom uppgift från IST ersätter aldrig ett värde som redan finns i Koha. | Av som standard. | Ja |
| F-26 | Ta bort arkiverade dubbletter | Val: arkiverade dubbletter tas bort efter 3, 7, 14 eller 30 dagar. | Av som standard. Borttagningen sker i samband med en import. | Ja |

## Personal

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-27 | Importera personal | En extra omgång efter skolorna för personer med aktivt uppdrag. Kategorin avgörs av reglerna (en regel per roll). Biblioteket är skolan för det senaste aktiva uppdraget. | Av som standard. Kräver uppdrag i IST och att "Använd regler" är ikryssad. En roll utan regel importeras inte. | Ja |
| F-28 | Befintlig personal lämnas orörd | Val: personal som redan finns i Koha och som pluginet aldrig hanterat ändras inte. | På som standard. | Ja |

## Elever som slutat

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-29 | Markera elever som slutat | Efter en komplett körning kontrolleras låntagare som inte setts idag mot IST. De som saknas, eller saknar giltig inskrivning, markeras i anteckningen. | Kontrollerar en person i taget mot IST. | Ja |
| F-30 | Hantera elever som slutat | Läge: ingenting, endast rapport eller flytta och ta bort. Per kategori väljer ni kategori medan lån finns kvar och kategori medan inga lån finns. Kontot kan gå ut idag. Högst 200 ändringar per körning (kan ändras). | Bara låntagare i kategorier som står i tabellen och som importen hanterat. Av som standard. | Ja |
| F-31 | Ta bort utan lån | Låntagare utan lån tas bort efter ett antal dagar (standard 30). Allt som identifierar personen tas bort, också ur Kohas loggar och listan över borttagna låntagare. | Koha kan vägra borttagning, till exempel vid avgifter. Då ligger låntagaren kvar. | Ja |
| F-32 | Anonymisera när lån finns kvar | Låntagare med lån kvar anonymiseras efter ett antal dagar (standard 300, tomt betyder aldrig). Bara kontot och lånen behålls. Kontot tas bort när lånen är tillbaka. | | Ja |

## Körning och schema

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-33 | Körning i omgångar | Importen går skola för skola, en sida i taget. Dagen är klar när alla skolor är klara. | Kräver att systemadministratören lagt in en cron-rad. Högst 100 omgångar (en sida per skola och omgång) och 8 timmar per körning. | Ja |
| F-34 | Omstart när IST ändrat data | Om IST svarar att data ändrats startar skolan om från första sidan, högst 10 gånger per skola och dag. | | Ja |
| F-35 | Schema | Dagar och klockslag för import och för "Hämta värden från IST nu". Högst 50 rader. Starten kan bli upp till en timme sen men sker aldrig två gånger samma dag. | Servertid. Kräver cron-raden som körs varje minut. | Ja |
| F-36 | Kör nu | Knappar som startar import eller hämtning av värden inom en minut. Importen börjar om från första sidan. | Kräver cron-raden som körs varje minut. | Ja |
| F-37 | Starta om dagens körning | Knappen "Ställ in API-scanner från första sidan" gör att dagens sidor behandlas om. | Aktiv först när sidor behandlats idag. | Ja |
| F-38 | Jobblista och körningsstatus | Visar de senaste jobben, en rad om en körning pågår och en status som visar om minutjobbet är aktivt. | | Ja |
| F-39 | Skydd mot samtidiga körningar | Två importer körs aldrig samtidigt, oavsett vem som startat dem. Ett nytt jobb väntar tills det pågående är klart. | | Ja |
| F-40 | Verktyg (äldre sida) | Startar en körning från webben och visar logg, statistik och uppdateringar. Har en knapp för att låsa upp en körning som fastnat. | Äldre sida, behövs inte i vanlig drift. | Ja |

## Logg och spårbarhet

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-41 | Logg: importkörningar | En rad per behandlad sida med skola, antal, status och meddelande. Filter, sökning och sortering. Svaret från IST kan öppnas per rad. | Hela svaret sparas bara när "Debuggläge" är på. | Ja |
| F-42 | Logg: låntagarändringar | Visar ändringar i låntagare: vilka fält som ändrats, från och till. Varje låntagare länkar till sin post. | Ändringarna fångas av en databasfunktion (trigger) som pluginet lägger in. | Ja |
| F-43 | Länk under Administration > Plugins | Egen rad "Importerade SS12000-användare (logg)" som öppnar loggsidan. Loggsidan nås också från konfigurationssidan. | Kommer från ett litet följeplugin i samma paket. | Ja |

## Dataskydd

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-44 | Debuggläge | Val: hela svar från IST sparas i databasen och mycket detaljerad loggning skrivs till loggfilen på servern. | Av som standard i en ny installation. Innehåller personuppgifter när det är på. Loggfilen innehåller en del personuppgifter även när det är av (se manualen, kapitlet Dataskydd). | Delvis |
| F-45 | Gallring av importloggen | Ni väljer hur många dagar importloggen sparas (1 till 120, standard 3). | Gäller bara importkörningarna. Låntagarändringarna och loggfilen på servern gallras inte av pluginet. | Delvis |

## Drift

| ID | Funktion | Vad den gör | Förutsättning eller begränsning | Ingår |
|---|---|---|---|---|
| F-46 | Installation och uppgradering | Pluginet laddas upp i Koha. Tabeller och triggern skapas. Vid uppgradering behålls inställningarna. | Koha 20.05 eller nyare, plugin påslagna och Perl-modulen URI::Encode. | Ja |
| F-47 | Avinstallation | Pluginets tabeller och trigger tas bort. | Inställningar, regler och loggar försvinner. | Ja |
| F-48 | Språk | Gränssnittet följer Kohas språk. | Svenska och engelska finns. Finska är delvis översatt, resten visas på engelska. | Delvis |
| F-49 | Behörighet | Sidorna kräver Kohas behörighet för plugin. | | Ja |

## Ingår inte (kan beställas som vidareutveckling)

| ID | Vad som saknas | Kommentar |
|---|---|---|
| N-01 | Vårdnadshavare som Kohas garanter | IST kan leverera uppgiften, pluginet läser den inte. |
| N-02 | Foto på låntagaren | IST kan leverera uppgiften, pluginet läser den inte. |
| N-03 | Undervisningsgrupper och mentorsgrupper | Bara klassgrupper läses. |
| N-04 | Egen inställning för sekretessmarkering (skyddad identitet) | Kan i dag göras med en regel på "Annat fält (avancerat)" (kräver "Använd regler") om IST levererar uppgiften. |
| N-05 | Egen inställning för personer som är utvandrade eller avlidna | Samma som N-04. |
| N-06 | Regler som sätter fler fält än kategori | Till exempel sorteringsfält, attribut eller utgångsdatum. |
| N-07 | Bibliotek (filial) som regel i regelredigeraren | Motorn kan det, sidan har ingen redigerare för det. Biblioteket kommer från skolmappningen. |
| N-08 | Fler val för vilka uppgifter som används | Mellannamn, andra adresstyper än folkbokföring, andra e-posttyper, telefontyper och val av typ av externt id. |
| N-09 | Lösenord och PIN-kod | Pluginet sätter inget lösenord och ingen PIN-kod. |
| N-10 | Meddelandeinställningar för nya låntagare | Nya låntagare skapas utan meddelandeinställningar. |
| N-11 | Förnamn i fältet för önskat namn (preferred name) | Fältet fylls inte. |
| N-12 | Inskrivningar som börjar i framtiden | En inskrivning importeras först när den har börjat. |
| N-13 | Förhandsgranskning av regler | Det går inte att se vad reglerna skulle göra innan de slås på. |
| N-14 | Regelbesluten på en sida i Koha | Se F-15. |
| N-15 | Hämtade värden som val i regelredigeraren | Se F-03. |
| N-16 | Särskild hantering av personal bland elever som slutat | Funktionen gäller de kategorier ni lägger in i tabellen. |
| N-17 | Lånebestämmelser, biblioteksgrupper och meddelanden för nya kategorier | Det är Koha-inställningar som biblioteket gör själv. |
| N-18 | Andra källsystem än IST | Pluginet pratar med IST:s SS12000-gränssnitt. |
| N-19 | Starkare lagring av klienthemligheten | Hemligheten sparas förvrängd (XOR), inte med stark kryptering. |

## Fel eller önskemål?

- **Fel (bugg):** en funktion som står som *Ja* eller *Delvis* gör något annat än raden säger, under de förutsättningar som anges. Rättas utan kostnad.
- **Önskemål (vidareutveckling):** allt annat: en funktion som inte finns i listan, något som listan säger att pluginet *inte* gör, eller något utöver den angivna begränsningen. Offereras.
- **Konfiguration:** det går att göra med en inställning på sidan (kategori, regel, schema, mappning). Hur det debiteras avgörs i överenskommelsen med varje kund.
- **Beroende av IST:** uppgifter som IST inte levererar, eller en prenumeration som saknas. Varken fel eller önskemål: pluginet kan inte hämta det som inte skickas.
- **Vid felanmälan, skicka:** version, vad du gjorde, vad du väntade dig, vad som hände, klockslag och en skärmbild av sidan *Importerade SS12000-användare (logg)*. Skicka aldrig personnummer.
