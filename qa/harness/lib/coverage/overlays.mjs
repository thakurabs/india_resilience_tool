// Safe overlay handling for long-running vendor QA probes.
//
// The feedback survey/promo appears asynchronously and can intercept pointer
// events. This module removes only known survey-like surfaces and preserves
// action/confirmation dialogs used by real workflows.

const SURVEY_TEXT = /HELP US IMPROVE YOUR EXPERIENCE|appreciate your quick feedback|guided walkthrough|visitor guide/i;
const ACTION_DIALOG_TEXT = /confirm|are you sure|cancel|clear|portfolio|remove all|discard|unsaved|proceed|delete|save|yes,/i;

/** Install a page-side remover for visitor-guide and feedback overlays. */
export async function installCoverageOverlayDismissal(page) {
  await page.addInitScript(({ surveySource, keepSource }) => {
    const SURVEY_RE = new RegExp(surveySource, 'i');
    const KEEP_RE = new RegExp(keepSource, 'i');
    const isSurveyBackdrop = (el) => {
      const cls = String(el.className || '');
      const text = String(el.innerText || '');
      return /inset-0/.test(cls) && /bg-black\//.test(cls) && !KEEP_RE.test(text);
    };
    const kill = () => {
      document.querySelectorAll('[data-modal-root]').forEach((el) => {
        if (isSurveyBackdrop(el) || SURVEY_RE.test(el.innerText || '')) el.remove();
      });
      document.querySelectorAll('[role="dialog"]').forEach((el) => {
        if (SURVEY_RE.test(el.innerText || '')) el.remove();
      });
      [...document.querySelectorAll('button,[role="button"],[aria-label]')]
        .filter((el) => /skip|close|got it|done/i.test(`${el.getAttribute('aria-label') || ''} ${el.innerText || ''}`))
        .filter((el) => SURVEY_RE.test(el.closest('[role="dialog"],[data-modal-root],body')?.innerText || ''))
        .forEach((el) => el.click());
    };
    try { setInterval(kill, 300); } catch (e) { /* noop */ }
    const start = () => {
      kill();
      try { new MutationObserver(kill).observe(document.body, { childList: true, subtree: true }); } catch (e) { /* noop */ }
    };
    if (document.body) start(); else document.addEventListener('DOMContentLoaded', start);
  }, { surveySource: SURVEY_TEXT.source, keepSource: ACTION_DIALOG_TEXT.source });
}

/** Run overlay dismissal once on the current page. */
export async function dismissCoverageOverlays(page) {
  return page.evaluate(({ surveySource, keepSource }) => {
    const SURVEY_RE = new RegExp(surveySource, 'i');
    const KEEP_RE = new RegExp(keepSource, 'i');
    let removed = 0;
    let clicked = 0;
    const isSurveyBackdrop = (el) => {
      const cls = String(el.className || '');
      const text = String(el.innerText || '');
      return /inset-0/.test(cls) && /bg-black\//.test(cls) && !KEEP_RE.test(text);
    };
    document.querySelectorAll('[data-modal-root]').forEach((el) => {
      if (isSurveyBackdrop(el) || SURVEY_RE.test(el.innerText || '')) {
        el.remove();
        removed += 1;
      }
    });
    document.querySelectorAll('[role="dialog"]').forEach((el) => {
      if (SURVEY_RE.test(el.innerText || '')) {
        el.remove();
        removed += 1;
      }
    });
    [...document.querySelectorAll('button,[role="button"],[aria-label]')]
      .filter((el) => /skip|close|got it|done/i.test(`${el.getAttribute('aria-label') || ''} ${el.innerText || ''}`))
      .filter((el) => SURVEY_RE.test(el.closest('[role="dialog"],[data-modal-root],body')?.innerText || ''))
      .forEach((el) => {
        el.click();
        clicked += 1;
      });
    return { removed, clicked };
  }, { surveySource: SURVEY_TEXT.source, keepSource: ACTION_DIALOG_TEXT.source }).catch(() => ({ removed: 0, clicked: 0 }));
}
