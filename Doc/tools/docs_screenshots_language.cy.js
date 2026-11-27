// Samma sida på svenska och engelska (bild 21 och 22): toppen av konfigurationssidan.
// Körs av make-screenshots.sh. Miljövariabler: CYPRESS_KOHA_USER, CYPRESS_KOHA_PASS.

Cypress.on('uncaught:exception', () => false);

const CLASS = 'Koha::Plugin::imCode::KohaSS12000::ExportUsers';
const configure = '/cgi-bin/koha/plugins/run.pl?class=' + encodeURIComponent(CLASS) + '&method=configure';

const languages = [
    { code: 'sv-SE', file: '21-rubrik-svenska' },
    { code: 'en', file: '22-rubrik-engelska' },
];

describe('Rubriken på två språk', () => {
    languages.forEach(({ code, file }) => {
        it(file, () => {
            cy.viewport(1280, 900);
            cy.session('demo-lang', () => {
                cy.visit('/cgi-bin/koha/mainpage.pl');
                cy.get('#userid').type(Cypress.env('KOHA_USER'));
                cy.get('#password').type(Cypress.env('KOHA_PASS'), { log: false });
                cy.get('#submit-button').click();
                cy.get('a[href*="logout"]').should('exist');
            });
            cy.setCookie('KohaOpacLanguage', code);
            cy.visit(configure);
            cy.get('#ss12000-form').should('exist');
            cy.get('body').then(($b) => { $b.find('#changelanguage, .ss12000-actions').css({ display: 'none' }); });
            cy.get('h1').first().scrollIntoView();
            cy.window().then((win) => {
                const a = win.document.querySelector('h1').getBoundingClientRect();
                const b = win.document.querySelector('#sec-about').getBoundingClientRect();
                const x = Math.max(0, Math.floor(Math.min(a.left, b.left)) - 12);
                const y = Math.max(0, Math.floor(a.top) - 12);
                const w = Math.ceil(Math.max(a.right, b.right)) - x + 12;
                const h = Math.ceil(b.bottom) - y + 12;
                cy.screenshot(file, { overwrite: true, capture: 'viewport', clip: { x, y, width: w, height: h } });
            });
        });
    });
});
