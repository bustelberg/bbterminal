'use client';

import { useLang, type Lang } from '../../../lib/i18n';

/**
 * `/account/security` — every string on the two-factor page.
 *
 * ⚠⚠ TRANSLATED RATHER THAN LEFT ON THE UNTRANSLATED PILE, and that is a deliberate exception to
 * "new surfaces catch up later". This is the one screen where a reader is asked to do something
 * irreversible to their own access, following instructions, while holding a phone. A half-English
 * security flow is where people give up half way — and an abandoned enrolment is not a neutral
 * outcome here: it leaves an unverified factor against the cap (see `mfaFactors.unverifiedIds`).
 *
 * ⚠ ENGLISH IS THE SOURCE, Dutch is translated from it — same rule as every other copy module,
 * and `SecurityCopy` makes a forgotten Dutch string a compile error rather than a blank line.
 */
export type SecurityCopy = {
  title: string;
  intro: string;
  /** ⚠⚠ THE POLICY, AND IT MUST STAY TRUE. It said "enrolling does not lock anything down yet"
   *  while that was so; the day the gate shipped that sentence became the one false statement on
   *  a page whose entire job is to be trusted about exactly this. */
  policy: string;
  loading: string;
  noneTitle: string;
  noneBody: string;
  add: string;
  addFirst: string;
  active: string;
  addedOn: (date: string) => string;
  remove: string;
  nameLabel: string;
  namePlaceholder: string;
  scanTitle: string;
  scanBody: string;
  cannotScan: string;
  secretLabel: string;
  copySecret: string;
  copied: string;
  codeLabel: string;
  codeHint: string;
  confirm: string;
  confirming: string;
  cancel: string;
  enrolled: string;
  removeTitle: (name: string) => string;
  removeBody: string;
  removeBodyOther: string;
  removeConfirm: string;
  removing: string;
  removed: string;
  /** ⚠ No backup codes exist — this is the entire recovery story, so it is on the page. */
  recoveryTitle: string;
  recoveryBody: string;
  /** ⚠ Shown BEFORE anyone scans, when this machine's clock is out. See `clockWarning`. */
  clockWarning: (seconds: number, ahead: boolean) => string;
  /** The `/mfa` gate — a different screen, same feature, so one copy module. */
  challenge: {
    title: string;
    body: string;
    pick: string;
    verify: string;
    verifying: string;
    signOut: string;
    noFactors: string;
  };
};

const EN: SecurityCopy = {
  title: 'Two-factor sign-in',
  intro: 'Add an authenticator app so signing in needs your password and a code from your phone.',
  policy: 'An authenticator is required to use BBTerminal. Your sign-in lasts a month; after that '
    + 'you sign in again with your password and a code.',
  loading: 'Checking your authenticators…',
  noneTitle: 'No authenticator yet',
  noneBody: 'Set one up to continue — until you do, this is the only page you can open.',
  add: 'Add another authenticator',
  addFirst: 'Set up an authenticator',
  active: 'Active',
  addedOn: (date) => `Added ${date}`,
  remove: 'Remove',
  nameLabel: 'Name this authenticator',
  namePlaceholder: 'e.g. iPhone',
  scanTitle: 'Scan this with your authenticator app',
  scanBody: 'Use Google Authenticator, 1Password, Bitwarden — any app that generates 6-digit codes.',
  cannotScan: 'Cannot scan it?',
  secretLabel: 'Enter this key by hand instead',
  copySecret: 'Copy',
  copied: 'Copied',
  codeLabel: 'Then enter the 6-digit code it shows',
  codeHint: 'The code changes every 30 seconds. Enter the current one.',
  confirm: 'Confirm and turn on',
  confirming: 'Checking…',
  cancel: 'Cancel',
  enrolled: 'Authenticator added. Keep the app installed — you will need it to sign in.',
  removeTitle: (name) => `Remove ${name}?`,
  removeBody: 'Enter the current code from this authenticator to confirm it is yours.',
  removeBodyOther: 'Enter a current code from any of your authenticators to confirm.',
  removeConfirm: 'Remove authenticator',
  removing: 'Removing…',
  removed: 'Authenticator removed.',
  recoveryTitle: 'If you lose your phone',
  recoveryBody: 'There are no backup codes. Add a second authenticator on another device while you '
    + 'can — otherwise an admin has to remove the old one for you before you can sign in again.',
  clockWarning: (seconds, ahead) =>
    `This computer's clock is ${seconds} seconds ${ahead ? 'ahead of' : 'behind'} the server. `
    + 'Codes are only accepted within about 30 seconds, so two-factor will fail until you fix it '
    + '— sync this machine’s time (Windows: Settings → Time & language → "Sync now"), '
    + 'then reload. Your phone is almost certainly fine.',
  challenge: {
    title: 'Enter your code',
    body: 'Open your authenticator app and enter the 6-digit code it shows for BBTerminal.',
    pick: 'Which authenticator?',
    verify: 'Continue',
    verifying: 'Checking…',
    signOut: 'Sign out instead',
    noFactors: 'There is no authenticator on this account. Continuing without one.',
  },
};

/** ⚠ TRANSLATED FROM THE ENGLISH ABOVE, never authored here. */
const NL: SecurityCopy = {
  title: 'Tweestapsverificatie',
  intro: 'Voeg een authenticator-app toe, zodat inloggen je wachtwoord én een code van je telefoon '
    + 'vereist.',
  policy: 'Een authenticator is verplicht om BBTerminal te gebruiken. Je blijft een maand '
    + 'ingelogd; daarna log je opnieuw in met je wachtwoord én een code.',
  loading: 'Je authenticators worden opgehaald…',
  noneTitle: 'Nog geen authenticator',
  noneBody: 'Stel er een in om verder te gaan — tot die tijd is dit de enige pagina die je kunt openen.',
  add: 'Nog een authenticator toevoegen',
  addFirst: 'Authenticator instellen',
  active: 'Actief',
  addedOn: (date) => `Toegevoegd op ${date}`,
  remove: 'Verwijderen',
  nameLabel: 'Geef deze authenticator een naam',
  namePlaceholder: 'bijv. iPhone',
  scanTitle: 'Scan dit met je authenticator-app',
  scanBody: 'Gebruik Google Authenticator, 1Password, Bitwarden — elke app die 6-cijferige codes '
    + 'maakt.',
  cannotScan: 'Kun je niet scannen?',
  secretLabel: 'Voer deze sleutel dan handmatig in',
  copySecret: 'Kopiëren',
  copied: 'Gekopieerd',
  codeLabel: 'Voer daarna de 6-cijferige code in die de app toont',
  codeHint: 'De code verandert elke 30 seconden. Voer de huidige in.',
  confirm: 'Bevestigen en inschakelen',
  confirming: 'Controleren…',
  cancel: 'Annuleren',
  enrolled: 'Authenticator toegevoegd. Houd de app geïnstalleerd — je hebt hem nodig om in te loggen.',
  removeTitle: (name) => `${name} verwijderen?`,
  removeBody: 'Voer de huidige code van deze authenticator in om te bevestigen dat hij van jou is.',
  removeBodyOther: 'Voer een huidige code van een van je authenticators in om te bevestigen.',
  removeConfirm: 'Authenticator verwijderen',
  removing: 'Verwijderen…',
  removed: 'Authenticator verwijderd.',
  recoveryTitle: 'Als je je telefoon kwijtraakt',
  recoveryBody: 'Er zijn geen back-upcodes. Voeg nu een tweede authenticator op een ander apparaat '
    + 'toe — anders moet een beheerder de oude eerst verwijderen voordat je weer kunt inloggen.',
  clockWarning: (seconds, ahead) =>
    `De klok van deze computer loopt ${seconds} seconden ${ahead ? 'voor op' : 'achter op'} de `
    + 'server. Codes worden maar ongeveer 30 seconden geaccepteerd, dus tweestapsverificatie '
    + 'mislukt tot dit is opgelost — synchroniseer de tijd van deze computer (Windows: '
    + 'Instellingen → Tijd en taal → "Nu synchroniseren") en herlaad de pagina. Aan je '
    + 'telefoon ligt het vrijwel zeker niet.',
  challenge: {
    title: 'Voer je code in',
    body: 'Open je authenticator-app en voer de 6-cijferige code in die hij voor BBTerminal toont.',
    pick: 'Welke authenticator?',
    verify: 'Doorgaan',
    verifying: 'Controleren…',
    signOut: 'Toch uitloggen',
    noFactors: 'Er staat geen authenticator op dit account. Er wordt zonder verder gegaan.',
  },
};

const COPY: Record<Lang, SecurityCopy> = { en: EN, nl: NL };

export function useSecurityCopy(): SecurityCopy & { lang: Lang } {
  const [lang] = useLang();
  return { ...COPY[lang], lang };
}

/** Exported for the parity test — never read at runtime. */
export const SECURITY_COPY = COPY;
