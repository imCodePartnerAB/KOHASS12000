// Tar skärmbilderna till manualen. Körs av make-screenshots.sh.
// Alla värden på sidorna är påhittade (demo_seed.sql, demo_patron.sql).
// Miljövariabler: CYPRESS_KOHA_USER, CYPRESS_KOHA_PASS, baseUrl (anges av skriptet).
// Bilderna hamnar i screenshotsFolder (anges av skriptet) och beskärs sedan av skriptet.

// Fel i sidans egen JavaScript (till exempel i Kohas inloggningssida) ska inte stoppa bildtagningen
Cypress.on('uncaught:exception', () => false);

const CLASS = 'Koha::Plugin::imCode::KohaSS12000::ExportUsers';
const enc = encodeURIComponent;
const configure = '/cgi-bin/koha/plugins/run.pl?class=' + enc(CLASS) + '&method=configure';
const logPage = configure + '&op=log';

// Ett element beskärs till sin egen ruta
const shotEl = (name, selector) => {
    // Den fasta spara-raden ska inte hamna ovanpå bilderna
    // Den fasta spara-raden och språkraden ska inte hamna ovanpå bilderna
    cy.get('body').then(($b) => { $b.find('#changelanguage, .ss12000-actions').css({ display: 'none' }); });
    cy.get(selector).first().scrollIntoView();
    cy.get(selector).first().screenshot(name, { overwrite: true, padding: 8 });
};

describe('Skärmbilder till manualen', () => {
    beforeEach(() => {
        cy.viewport(1280, 900);
        cy.setCookie('KohaOpacLanguage', 'sv-SE');
        cy.session('demo', () => {
            cy.visit('/cgi-bin/koha/mainpage.pl');
            cy.get('#userid').type(Cypress.env('KOHA_USER'));
            cy.get('#password').type(Cypress.env('KOHA_PASS'), { log: false });
            cy.get('#submit-button').click();
            cy.get('a[href*="logout"]').should('exist');
        });
        cy.setCookie('KohaOpacLanguage', 'sv-SE');
    });

    it('01 pluginlistan', () => {
        cy.visit('/cgi-bin/koha/plugins/plugins-home.pl');
        cy.contains('SS12000').should('exist');
        // Bara raderna för detta plugin visas i bilden: sök på "SS12000"
        cy.get('input[type=search]').first().type('SS12000');
        cy.get('table tbody tr').should('have.length', 2);
        cy.get('table').first().screenshot('01-pluginlistan', { overwrite: true, padding: 8 });
    });

    it('02 till 08 och 15 till 17: konfigurationssidans sektioner', () => {
        cy.visit(configure);
        cy.get('#ss12000-form').should('exist');
        // Toppen av sidan: rubriken, genvägarna och rutan med imCode-loggan
        cy.get('body').then(($b) => { $b.find('#changelanguage, .ss12000-actions').css({ display: 'none' }); });
        cy.get('h1').first().scrollIntoView();
        cy.window().then((win) => {
            const a = win.document.querySelector('h1').getBoundingClientRect();
            const b = win.document.querySelector('#sec-about').getBoundingClientRect();
            const x = Math.max(0, Math.floor(Math.min(a.left, b.left)) - 12);
            const y = Math.max(0, Math.floor(a.top) - 12);
            const w = Math.ceil(Math.max(a.right, b.right)) - x + 12;
            const h = Math.ceil(b.bottom) - y + 12;
            cy.screenshot('02-konfigurationssidan', { overwrite: true, capture: 'viewport', clip: { x, y, width: w, height: h } });
        });
        shotEl('03-anslutning', '#sec-connection');
        shotEl('04-lantagarens-identitet', '#sec-patrons');
        shotEl('05-importregler', '#sec-rules');
        shotEl('06-skolmappning', '#sec-branches');
        shotEl('07-kategorimappning', '#sec-categories');
        shotEl('08-kategoriregler', '#sec-catrules');
        shotEl('15-personal', '#sec-staff');
        shotEl('16-lantagare-som-slutat', '#sec-leavers');
        shotEl('17-avancerat', '#sec-advanced');
    });

    it('09 en regel redigeras', () => {
        cy.visit(configure);
        cy.get('#rule-list .ss12000-rule').should('have.length.greaterThan', 1);
        cy.get('#rule-list .ss12000-rule').first().find('input[type=text]').first().focus();
        shotEl('09-redigera-regel', '#rule-list .ss12000-rule');
    });

    it('10 och 14 schema och jobblista', () => {
        cy.visit(configure);
        cy.get('#cron-status .alert').should('exist');
        shotEl('10-schema-vilande', '#sec-schedule');
        shotEl('14-jobblista', '#job-list');
    });

    it('18 och 20 loggsidan', () => {
        cy.visit(logPage);
        cy.get('#runs-table tbody tr').should('have.length.greaterThan', 0);
        shotEl('18-logg-importkorningar', '#tab-runs');
        cy.get('.ss12000-resp').first().click();
        cy.get('#resp-dialog[open]').should('exist');
        // Rutan visar "Hämtar..." tills svaret har kommit
        cy.get('#resp-body').should('not.be.empty').and('not.contain', 'Hämtar');
        cy.get('#resp-dialog').screenshot('20-logg-uppfalld', { overwrite: true, padding: 12 });
    });

    it('19 en importerad lantagare', () => {
        cy.viewport(1280, 1500);
        // Ett enda sökresultat går direkt till låntagarens sida. DEMO_BORROWER kan anges om sökningen inte räcker.
        cy.visit(Cypress.env('DEMO_BORROWER') ? '/cgi-bin/koha/members/moremember.pl?borrowernumber=' + Cypress.env('DEMO_BORROWER') : '/cgi-bin/koha/members/member.pl?searchmember=anna.exempel&searchtype=contain');
        cy.contains('Exempel').should('exist');
        cy.get('body').then(($b) => { $b.find('#changelanguage, .ss12000-actions').css({ display: 'none' }); });
        // Bara låntagarens uppgifter, utan menyn där inloggat konto visas
        cy.get('#main-container, main').first().screenshot('19-lantagare', { overwrite: true, padding: 4 });
    });
});
