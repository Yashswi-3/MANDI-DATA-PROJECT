/**
 * Agmarknet 3-Year Mandi Data Extractor — v5.2 (final)
 * ---------------------------------------------------------------
 * Pulls: Capsicum, Cucumbar(Kheera), Karbuja(Musk Melon),
 *        Green Chilli (Naga Chilli substitute),
 *        Tomato (Cherry Tomato substitute)
 * From:  APMC Azadpur (Delhi) and Jaipur (F&V) APMC (Muhana Mandi)
 * Range: last 36 months
 *
 * v5.2: Added Tomato as the Cherry Tomato substitute (no separate Cherry
 * Tomato listing exists on Agmarknet), appended after Green Chilli.
 * Zucchini remains the only crop with no viable substitute — still
 * logged once to Fetch_Gaps as genuinely untracked, zero real data.
 *
 * v5.1: Added Green Chilli as the Naga Chilli substitute, APPENDED to
 * the end of CROPS so existing sync progress for the first 3 crops is
 * unaffected — the sync will simply continue into Green Chilli once it
 * finishes Muskmelon. Confirmed live it has real data at both mandis.
 *
 * Both substitutes are REAL data with the SAME gap pattern as any other
 * crop here (arrivals aren't daily at every mandi) — that's normal,
 * visible in Fetch_Gaps, not a bug. Naga Chilli itself and Zucchini were
 * confirmed to have ZERO data anywhere in the regulated mandi system,
 * which is a different and much stronger statement than "has gaps."
 *
 * v5 CHANGES:
 * - Starting batch size lowered 20 -> 10, to trip the 429 rate limit
 *   less often in the first place (the adaptive shrink/grow logic
 *   from v4 is still the real safety net — this just starts more
 *   conservatively so it needs to kick in less).
 * - Cherry Tomato and Zucchini do NOT exist as commodities on
 *   Agmarknet at all (confirmed live — no match in any commodity
 *   group, and no close substitute was accepted). There is no
 *   commodityId to call the API with, so they cannot be fetched.
 *   Per explicit instruction, they are kept VISIBLE in the pipeline
 *   instead of silently dropped: UNAVAILABLE_CROPS below gets logged
 *   once to Fetch_Gaps at sync start as "not tracked by Agmarknet",
 *   rather than wasting ~2,190 API calls (2 crops x 1,095 days)
 *   retrying something that can never succeed.
 * - Naga Chillies (checked as "Ghost Pepper(King Chilli)", the closest
 *   Agmarknet listing) had zero data across 7 sample dates spanning
 *   2023-2026 and was dropped per instruction — not included at all.
 *
 * v4/v3 behavior (still in effect):
 * - PARALLEL batch fetching via UrlFetchApp.fetchAll().
 * - Batch size is ADAPTIVE: halves on any HTTP 429 (floor of 2), grows
 *   back after 5 clean batches in a row.
 * - LockService prevents two executions running concurrently.
 * - A failed (non-429) date is retried once, then logged to
 *   "Fetch_Gaps" and skipped.
 * - The "nationwide data per call" behavior is a real Agmarknet API
 *   constraint (no per-market filter param) — payload is small JSON,
 *   filtered in-memory immediately, not the actual bottleneck.
 *
 * SETUP
 * 1. Run debugOneDay() first (Run menu, View > Logs) — confirms the API
 *    responds and shows one real record's shape. No API key needed.
 * 2. Run startFullSync() once. Leave it — it reschedules itself via a
 *    trigger until the full 36-month range is done for all 3 crops.
 *    Do NOT click Run again while it's active.
 * 3. Check "Raw_Data" as it fills in, and "Fetch_Gaps" for dates that
 *    failed after retry, plus the one-time Cherry Tomato/Zucchini
 *    "not tracked" entries.
 * 4. Run resetSync() to wipe progress + both sheets and start over.
 */

const MONTHS_BACK = 36;
const START_BATCH_SIZE = 10;           // initial parallel requests per batch
const MIN_BATCH_SIZE = 2;              // floor when throttled
const MAX_BATCHES_PER_RUN = 15;        // per execution
const TIME_BUDGET_MS = 5 * 60 * 1000;  // stop pulling at 5 min, leave buffer
const BATCH_PACING_MS = 800;           // pause between clean batches
const SHEET_NAME = 'Raw_Data';
const GAPS_SHEET_NAME = 'Fetch_Gaps';

// Crop label -> exact commodityGroupId + commodityId Agmarknet's API expects
// (confirmed live via https://api.agmarknet.gov.in/v1/all-type-report/commodity-filter)
// IMPORTANT: only ever APPEND new crops at the END of this array, never
// insert/reorder. WORK_INDEX (saved progress) is a flat index computed as
// cropIdx = floor(i / totalDays) — reordering existing entries would
// silently shift which crop/date each saved index points to and corrupt
// in-progress data. Appending is always safe: existing progress for
// crops 0-2 stays valid, and the sync just continues into the new crop
// once it reaches the end of Muskmelon.
const CROPS = [
  { label: 'Coloured Capsicum',    groupId: 6, commodityId: 136 },
  { label: 'English Cucumber',     groupId: 6, commodityId: 131 },
  { label: 'Karbuja (Muskmelon)',  groupId: 5, commodityId: 157 },
  // Substitute for Naga Chilli: confirmed exhaustively (13 dates, all of
  // India, both the direct API and Azadpur's own site) that Naga Chilli /
  // Ghost Pepper has ZERO data anywhere in the regulated mandi system —
  // it's not fetchable at all, not a gap. Green Chilli is the closest
  // real substitute with actual data at both our mandis (confirmed live).
  { label: 'Green Chilli (Naga Chilli substitute)', groupId: 6, commodityId: 73 },
  // Substitute for Cherry Tomato: Agmarknet has no separate Cherry Tomato
  // listing (checked all groups) — plain "Tomato" is the closest real
  // substitute, with confirmed real data at both Azadpur and Jaipur.
  { label: 'Tomato (Cherry Tomato substitute)', groupId: 6, commodityId: 65 }
];

// Crops confirmed to have NO commodity listing on Agmarknet at all —
// cannot be fetched (no ID exists to call). Logged once, not retried
// per-date. See v5 note above for what was checked and how.
const UNAVAILABLE_CROPS = [
  { label: 'Zucchini', reason: 'No matching commodity on Agmarknet (checked all groups) — no close substitute exists either.' }
];

// Exact marketName strings the API returns -> our friendly mandi label
const MANDI_LABELS = {
  'APMC Azadpur': 'Azadpur Mandi (Delhi)',
  'Jaipur (F&V) APMC': 'Muhana Mandi (Jaipur)'
};
const TARGET_MARKETS = Object.keys(MANDI_LABELS);

const HEADERS = [
  'Fetch_Date', 'Arrival_Date', 'Crop_Label', 'Commodity_Raw', 'Mandi_Label',
  'State', 'Market', 'Variety', 'Grade', 'Arrivals_MT',
  'Min_Price', 'Max_Price', 'Modal_Price', 'Average_Price', 'Price_Unit'
];

/**
 * Entry point — run this once from the Apps Script editor.
 * Fixes the date range's END date at first run (in Script Properties)
 * so the total day count stays stable across a multi-hour sync,
 * instead of silently growing by one every day the sync is still
 * running.
 */
function startFullSync() {
  const props = PropertiesService.getScriptProperties();
  if (!props.getProperty('WORK_INDEX')) props.setProperty('WORK_INDEX', '0');
  if (!props.getProperty('END_DATE')) {
    props.setProperty('END_DATE', formatDate(new Date()));
  }
  ensureSheetAndHeaders();
  logUnavailableCropsOnce();
  fetchAgmarknetData();
}

/** Logs Cherry Tomato / Zucchini to Fetch_Gaps exactly once (not per
 * date) so they stay visible in the pipeline without wasting calls. */
function logUnavailableCropsOnce() {
  const props = PropertiesService.getScriptProperties();
  if (props.getProperty('UNAVAILABLE_LOGGED')) return;
  const gapsSheet = ensureGapsSheet();
  const rows = UNAVAILABLE_CROPS.map(function (c) {
    return [new Date(), 'N/A — no commodity ID exists', c.label, c.reason];
  });
  gapsSheet.getRange(gapsSheet.getLastRow() + 1, 1, rows.length, 4).setValues(rows);
  props.setProperty('UNAVAILABLE_LOGGED', 'true');
}

/**
 * Pulls one batch of (crop, date) pairs in parallel. Called by
 * startFullSync() the first time, and by a self-created trigger every
 * run after that until the full range is complete for every crop.
 */
function fetchAgmarknetData() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    Logger.log('Another execution already holds the lock — exiting to avoid duplicate work.');
    return;
  }

  try {
    const props = PropertiesService.getScriptProperties();
    const startDate = monthsAgo(MONTHS_BACK);
    const endDateStr = props.getProperty('END_DATE');
    const endDate = endDateStr ? parseIsoDate(endDateStr) : new Date();
    const totalDays = daysBetween(startDate, endDate) + 1;
    const totalWork = CROPS.length * totalDays;

    let workIndex = Number(props.getProperty('WORK_INDEX') || 0);

    if (workIndex >= totalWork) {
      Logger.log('All crops complete for the full ' + MONTHS_BACK + '-month range.');
      deleteExistingTriggers();
      return;
    }

    const sheet = ensureSheetAndHeaders();
    const gapsSheet = ensureGapsSheet();
    const startedAt = Date.now();
    let batchesThisRun = 0;
    let cleanBatchStreak = Number(props.getProperty('CLEAN_STREAK') || 0);
    let batchSize = Number(props.getProperty('BATCH_SIZE') || START_BATCH_SIZE);
    let rateLimited = false;

    while (workIndex < totalWork
           && Date.now() - startedAt < TIME_BUDGET_MS
           && batchesThisRun < MAX_BATCHES_PER_RUN) {

      const batchEnd = Math.min(workIndex + batchSize, totalWork);
      const items = [];
      for (let i = workIndex; i < batchEnd; i++) {
        const cropIdx = Math.floor(i / totalDays);
        const dayIdx = i % totalDays;
        items.push({
          crop: CROPS[cropIdx],
          dateStr: formatDate(addDays(startDate, dayIdx))
        });
      }

      const results = fetchBatch(items);

      // Retry only genuine errors (not 429s) once, together, as their own
      // parallel batch. A 429 means "you're going too fast" — retrying it
      // immediately would just get throttled again, so those are handled
      // by shrinking the batch size and backing off instead, below.
      const retryIdx = [];
      results.forEach(function (r, i) { if (r.status === 'error') retryIdx.push(i); });
      if (retryIdx.length > 0) {
        Utilities.sleep(1500);
        const retryItems = retryIdx.map(function (i) { return items[i]; });
        const retryResults = fetchBatch(retryItems);
        retryResults.forEach(function (r, j) { results[retryIdx[j]] = r; });
      }

      const anyRateLimited = results.some(function (r) { return r.status === 'rate_limited'; });

      if (anyRateLimited) {
        // Don't advance workIndex — this whole batch gets retried later
        // (re-fetching a few already-successful items again is cheap;
        // losing track of which ones succeeded is not worth the
        // complexity). Shrink batch size and back off across runs.
        rateLimited = true;
        batchSize = Math.max(MIN_BATCH_SIZE, Math.floor(batchSize / 2));
        cleanBatchStreak = 0;
        props.setProperty('BATCH_SIZE', String(batchSize));
        props.setProperty('CLEAN_STREAK', '0');
        const failStreak = Number(props.getProperty('FAIL_STREAK') || 0) + 1;
        props.setProperty('FAIL_STREAK', String(failStreak));
        const delaySec = Math.min(30 * Math.pow(2, failStreak - 1), 600);
        Logger.log('Rate limited (HTTP 429) — shrinking batch size to ' + batchSize
          + ' and waiting ' + delaySec + 's before retrying (streak ' + failStreak + ').');
        scheduleNextRun(delaySec);
        break;
      }

      props.deleteProperty('FAIL_STREAK');
      cleanBatchStreak++;
      if (cleanBatchStreak >= 5 && batchSize < START_BATCH_SIZE) {
        batchSize = Math.min(START_BATCH_SIZE, batchSize + 2);
        cleanBatchStreak = 0;
        Logger.log('5 clean batches in a row — growing batch size back to ' + batchSize + '.');
      }
      props.setProperty('BATCH_SIZE', String(batchSize));
      props.setProperty('CLEAN_STREAK', String(cleanBatchStreak));

      const rowsToWrite = [];
      const gapRows = [];
      results.forEach(function (result, i) {
        const item = items[i];
        if (result.status !== 'ok') {
          gapRows.push([new Date(), item.dateStr, item.crop.label, 'Failed after retry (' + result.status + ')']);
          return;
        }
        extractRows(result.json, item.crop, item.dateStr).forEach(function (row) {
          rowsToWrite.push(row);
        });
      });

      if (rowsToWrite.length > 0) {
        sheet.getRange(sheet.getLastRow() + 1, 1, rowsToWrite.length, HEADERS.length).setValues(rowsToWrite);
      }
      if (gapRows.length > 0) {
        gapsSheet.getRange(gapsSheet.getLastRow() + 1, 1, gapRows.length, 4).setValues(gapRows);
      }

      workIndex = batchEnd;
      props.setProperty('WORK_INDEX', String(workIndex));
      batchesThisRun++;

      if (workIndex < totalWork) {
        Utilities.sleep(BATCH_PACING_MS);
      }
    }

    if (rateLimited) {
      return; // scheduleNextRun already called with the backoff delay above
    }

    if (workIndex < totalWork) {
      scheduleNextRun(15);
      Logger.log('Paused at ' + workIndex + '/' + totalWork
        + ' (crop,date) pairs, batch size ' + batchSize + ' — next run scheduled in ~15s.');
    } else {
      Logger.log('All crops complete for the full ' + MONTHS_BACK + '-month range.');
      deleteExistingTriggers();
    }
  } finally {
    lock.releaseLock();
  }
}

/** Fires a batch of requests in parallel and returns one result object
 * per item, in the same order as the input: { status: 'ok', json } |
 * { status: 'rate_limited' } | { status: 'error' }. */
function fetchBatch(items) {
  const requests = items.map(function (item) {
    return {
      url: buildUrl(item.crop.groupId, item.crop.commodityId, item.dateStr),
      muteHttpExceptions: true,
      headers: {
        'Accept': 'application/json',
        'Referer': 'https://agmarknet.gov.in/',
        'Origin': 'https://agmarknet.gov.in'
      }
    };
  });

  let responses;
  try {
    responses = UrlFetchApp.fetchAll(requests);
  } catch (err) {
    Logger.log('Batch fetch threw entirely: ' + err);
    return items.map(function () { return { status: 'error' }; });
  }

  return responses.map(function (response, i) {
    try {
      const code = response.getResponseCode();
      if (code === 429) {
        return { status: 'rate_limited' };
      }
      if (code !== 200) {
        Logger.log('HTTP ' + code + ' for ' + items[i].crop.label + ' ' + items[i].dateStr);
        return { status: 'error' };
      }
      const json = JSON.parse(response.getContentText());
      if (!json.success) return { status: 'error' };
      return { status: 'ok', json: json };
    } catch (err) {
      Logger.log('Parse error for ' + items[i].crop.label + ' ' + items[i].dateStr + ': ' + err);
      return { status: 'error' };
    }
  });
}

function buildUrl(groupId, commodityId, dateStr) {
  return 'https://api.agmarknet.gov.in/v1/prices-and-arrivals/market-report/specific'
    + '?date=' + dateStr
    + '&commodityGroupId=' + groupId
    + '&commodityId=' + commodityId
    + '&includeExcel=false';
}

/** Pulls out just our two target markets' rows from one day's API response. */
function extractRows(apiResult, crop, dateStr) {
  const rows = [];
  const states = apiResult.states || [];
  states.forEach(function (state) {
    (state.markets || []).forEach(function (market) {
      if (TARGET_MARKETS.indexOf(market.marketName) === -1) return;
      const mandiLabel = MANDI_LABELS[market.marketName];
      (market.data || []).forEach(function (rec) {
        const minPrice = toNumber(rec.minimumPrice);
        const maxPrice = toNumber(rec.maximumPrice);
        const averagePrice = (minPrice !== '' && maxPrice !== '') ? (minPrice + maxPrice) / 2 : '';
        rows.push([
          new Date(),
          dateStr,
          crop.label,
          apiResult.commodityName || '',
          mandiLabel,
          state.stateName || '',
          market.marketName || '',
          rec.variety || '',
          rec.grade || '',
          toNumber(rec.arrivals),
          minPrice,
          maxPrice,
          toNumber(rec.modalPrice),
          averagePrice,
          rec.unitOfPrice || 'Rs./Quintal'
        ]);
      });
    });
  });
  return rows;
}

function ensureSheetAndHeaders() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) sheet = ss.insertSheet(SHEET_NAME);
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);
    sheet.setFrozenRows(1);
  }
  return sheet;
}

function ensureGapsSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(GAPS_SHEET_NAME);
  if (!sheet) sheet = ss.insertSheet(GAPS_SHEET_NAME);
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, 4).setValues([['Logged_At', 'Arrival_Date', 'Crop_Label', 'Reason']]);
    sheet.setFrozenRows(1);
  }
  return sheet;
}

function monthsAgo(months) {
  const d = new Date();
  d.setMonth(d.getMonth() - months);
  d.setHours(0, 0, 0, 0);
  return d;
}

function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

function daysBetween(a, b) {
  const msPerDay = 24 * 60 * 60 * 1000;
  const aa = new Date(a.getFullYear(), a.getMonth(), a.getDate());
  const bb = new Date(b.getFullYear(), b.getMonth(), b.getDate());
  return Math.round((bb - aa) / msPerDay);
}

/** Agmarknet's API expects/returns YYYY-MM-DD. */
function formatDate(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return y + '-' + m + '-' + d;
}

function parseIsoDate(s) {
  const parts = s.split('-');
  return new Date(Number(parts[0]), Number(parts[1]) - 1, Number(parts[2]));
}

function toNumber(value) {
  const n = parseFloat(value);
  return isNaN(n) ? '' : n;
}

/** Creates a one-off trigger to resume the sync after delaySeconds (default 45). */
function scheduleNextRun(delaySeconds) {
  deleteExistingTriggers();
  ScriptApp.newTrigger('fetchAgmarknetData')
    .timeBased()
    .after((delaySeconds || 45) * 1000)
    .create();
}

function deleteExistingTriggers() {
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === 'fetchAgmarknetData') {
      ScriptApp.deleteTrigger(t);
    }
  });
}

/** Wipes saved progress + both sheets, so the next run starts fresh. */
function resetSync() {
  PropertiesService.getScriptProperties().deleteAllProperties();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  [SHEET_NAME, GAPS_SHEET_NAME].forEach(function (name) {
    const sheet = ss.getSheetByName(name);
    if (sheet) ss.deleteSheet(sheet);
  });
  deleteExistingTriggers();
}

/**
 * Run this FIRST, once, to sanity-check the API and see one real day's
 * response shape (View > Logs to read output). No API key needed.
 */
function debugOneDay() {
  const crop = CROPS[2]; // Karbuja (Muskmelon)
  const dateStr = '2024-01-15'; // known-good date, confirmed live
  const result = fetchBatch([{ crop: crop, dateStr: dateStr }])[0];
  if (result.status !== 'ok') {
    Logger.log('Fetch did not succeed — status: ' + result.status);
    return;
  }
  Logger.log('Title: ' + result.json.title);
  Logger.log('Matching rows for our two mandis: ' + JSON.stringify(extractRows(result.json, crop, dateStr), null, 2));
}
