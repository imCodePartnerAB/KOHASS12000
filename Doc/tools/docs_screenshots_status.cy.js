// Statusrutan under "Schema och kör nu". Körs av make-screenshots.sh efter att databasen
// satts i rätt läge (demo_cron_saknas.sql, demo_cron_aktiv.sql eller en pågående körning).
// Miljövariabel SHOT_NAME bestämmer filnamnet.

// Fel i sidans egen JavaScript (till exempel i Kohas inloggningssida) ska inte stoppa bildtagningen
Cypress.on('uncaught:exception', () => false);

const CLASS = 'Koha::Plugin::imCode::KohaSS12000::ExportUsers';
const configure = '/cgi-bin/koha/plugins/run.pl?class=' + encodeURIComponent(CLASS) + '&method=configure';

describe('Statusrutan', () => {
    it('tar en bild av schemasektionen', () => {
        cy.viewport(1280, 1100);
        cy.setCookie('KohaOpacLanguage', 'sv-SE');
        cy.session('demo-status', () => {
            cy.visit('/cgi-bin/koha/mainpage.pl');
            cy.get('#userid').type(Cypress.env('KOHA_USER'));
            cy.get('#password').type(Cypress.env('KOHA_PASS'), { log: false });
            cy.get('#submit-button').click();
            cy.get('a[href*="logout"]').should('exist');
        });
        cy.setCookie('KohaOpacLanguage', 'sv-SE');
        cy.visit(configure);
        cy.get('#cron-status .alert').should('exist');
        // Den fasta spara-raden ska inte hamna ovanpå bilden
        cy.get('body').then(($b) => { $b.find('#changelanguage, .ss12000-actions').css({ display: 'none' }); });
        cy.get('#sec-schedule').scrollIntoView();
        cy.get('#sec-schedule').screenshot(Cypress.env('SHOT_NAME'), { overwrite: true, padding: 8 });
    });
});
