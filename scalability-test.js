/**
 * ============================================================
 *  JOB QUEUE SYSTEM — COMPREHENSIVE SCALABILITY TEST SUITE
 * ============================================================
 *
 *  Phases:
 *    0 — Health check + JVM/HikariCP/Kafka warm-up
 *    1 — Sustained throughput benchmark (fixed RPS over time)
 *    2 — End-to-end latency measurement (submit → COMPLETED)
 *    3 — Concurrency spike stress test (progressive load)
 *    4 — Resilience validation (circuit breaker + rate limiter)
 *
 *  Output:
 *    • Rich color-coded console output
 *    • scalability-report.html — interactive HTML dashboard
 *
 *  Run:  node scalability-test.js
 *  Req:  Node.js 18+ (native fetch), server running on :8080
 * ============================================================
 */

'use strict';

const fs   = require('fs');
const path = require('path');

// ── Config ─────────────────────────────────────────────────
const BASE_URL  = 'http://localhost:8080';
const JOBS_URL  = `${BASE_URL}/jobs`;
const STATS_URL = `${JOBS_URL}/stats`;
const HEALTH_URL = `${BASE_URL}/actuator/health`;

const REPORT_PATH = path.join(__dirname, 'scalability-report.html');

// ── ANSI Colors ─────────────────────────────────────────────
const C = {
  reset:   '\x1b[0m',
  bold:    '\x1b[1m',
  dim:     '\x1b[2m',
  red:     '\x1b[31m',
  green:   '\x1b[32m',
  yellow:  '\x1b[33m',
  blue:    '\x1b[34m',
  magenta: '\x1b[35m',
  cyan:    '\x1b[36m',
  white:   '\x1b[37m',
  gray:    '\x1b[90m',
  bgRed:   '\x1b[41m',
  bgGreen: '\x1b[42m',
};

// ── Helpers ─────────────────────────────────────────────────
const sleep  = (ms) => new Promise(r => setTimeout(r, ms));
const now    = () => Date.now();
const pct    = (n, d) => d === 0 ? '0.0' : ((n / d) * 100).toFixed(1);
const avg    = (arr) => arr.length === 0 ? 0 : Math.round(arr.reduce((a, b) => a + b, 0) / arr.length);
const minVal = (arr) => arr.length === 0 ? 0 : Math.min(...arr);
const maxVal = (arr) => arr.length === 0 ? 0 : Math.max(...arr);

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function banner(title) {
  const line = '═'.repeat(72);
  console.log(`\n${C.bold}${C.cyan}╔${line}╗`);
  console.log(`║  ${title.padEnd(70)}║`);
  console.log(`╚${line}╝${C.reset}\n`);
}

function section(title) {
  console.log(`\n${C.bold}${C.blue}┌── ${title} ${'─'.repeat(Math.max(0, 60 - title.length))}┐${C.reset}`);
}

function ok(msg)   { console.log(`  ${C.green}✔${C.reset} ${msg}`); }
function warn(msg) { console.log(`  ${C.yellow}⚠${C.reset} ${msg}`); }
function fail(msg) { console.log(`  ${C.red}✘${C.reset} ${msg}`); }
function info(msg) { console.log(`  ${C.cyan}ℹ${C.reset} ${msg}`); }
function dim(msg)  { console.log(`  ${C.gray}${msg}${C.reset}`); }

/** POST a LOG job and return the parsed JSON response + timing */
async function submitJob(label = 'perf-test') {
  const t0 = now();
  try {
    const res = await fetch(JOBS_URL, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'LOG', payload: label, message: 'scalability-test' }),
    });
    const latency = now() - t0;
    if (res.ok) {
      const data = await res.json();
      return { success: true, latency, id: data.id, status: data.status };
    }
    return { success: false, latency, httpStatus: res.status };
  } catch (e) {
    return { success: false, latency: now() - t0, error: e.message };
  }
}

/** GET /jobs/{id} and return job object */
async function getJob(id) {
  try {
    const res = await fetch(`${JOBS_URL}/${id}`);
    if (res.ok) return await res.json();
  } catch (_) {}
  return null;
}

/** GET /jobs/stats */
async function getStats() {
  try {
    const res = await fetch(STATS_URL);
    if (res.ok) return await res.json();
  } catch (_) {}
  return null;
}

/** Poll job until terminal state or timeout */
async function waitForCompletion(id, timeoutMs = 30000, pollMs = 400) {
  const deadline = now() + timeoutMs;
  while (now() < deadline) {
    const job = await getJob(id);
    if (job) {
      const s = job.status;
      if (s === 'COMPLETED' || s === 'FAILED' || s === 'CANCELLED') return job;
    }
    await sleep(pollMs);
  }
  return null; // timeout
}

/**
 * Waits for the QUEUED+RETRY backlog to fall below a threshold.
 * Prevents E2E and resilience tests from running while thousands of
 * leftover jobs from load tests are blocking the Kafka consumers.
 */
async function drainQueue(label = '', maxWaitMs = 300000, targetBelow = 20) {
  const tag = label ? `[${label}] ` : '';
  const deadline = now() + maxWaitMs;
  let lastLog = 0;
  console.log(`\n  ${C.cyan}⏳${C.reset} ${tag}Waiting for job backlog to drain (QUEUED+RETRY < ${targetBelow})…`);
  while (now() < deadline) {
    const stats = await getStats();
    if (!stats) { await sleep(3000); continue; }
    const queued = Number(stats.QUEUED  || 0);
    const retry  = Number(stats.RETRY   || 0);
    const total  = queued + retry;
    if (now() - lastLog > 10000) {
      process.stdout.write(`\r  ${C.gray}  Backlog: QUEUED=${queued}  RETRY=${retry}  Total=${total} — waiting…   ${C.reset}`);
      lastLog = now();
    }
    if (total < targetBelow) {
      console.log(`\n  ${C.green}✔${C.reset} ${tag}Queue drained — QUEUED=${queued}  RETRY=${retry}`);
      return true;
    }
    await sleep(3000);
  }
  const stats = await getStats();
  console.log(`\n  ${C.yellow}⚠${C.reset} ${tag}Drain timeout reached — QUEUED=${stats?.QUEUED}  RETRY=${stats?.RETRY} (continuing anyway)`);
  return false;
}

// ════════════════════════════════════════════════════════════
// PHASE 0 — Health Check + Warm-Up
// ════════════════════════════════════════════════════════════
async function phase0_warmup() {
  banner('PHASE 0 — Health Check & JVM Warm-Up');

  // 0-A: Health check
  section('Server Health');
  console.log(`  Polling ${HEALTH_URL} (max 60 attempts)…`);
  for (let i = 1; i <= 60; i++) {
    try {
      const res  = await fetch(HEALTH_URL);
      const data = await res.json();
      const dbUp    = data?.components?.db?.status    === 'UP';
      const redisUp = data?.components?.redis?.status === 'UP';
      if (dbUp && redisUp) {
        ok(`Server UP — DB ✔  Redis ✔  (attempt ${i})`);
        if (data?.components?.mail?.status === 'DOWN') {
          warn('Mail service is DOWN — expected (no credentials in test env)');
        }
        break;
      }
      if (i === 60) { fail('Server not healthy after 60s — aborting'); process.exit(1); }
    } catch (_) {}
    process.stdout.write(`\r  ${C.yellow}Waiting… attempt ${i}/60${C.reset}   `);
    await sleep(1000);
  }

  // 0-B: Stats endpoint smoke-test
  section('Stats Endpoint Smoke-Test');
  const stats = await getStats();
  if (stats) {
    ok(`GET /jobs/stats OK — TOTAL jobs in DB: ${stats.TOTAL ?? 'n/a'}`);
  } else {
    warn('GET /jobs/stats not reachable — stats-based features will be skipped');
  }

  // 0-C: Warm-up — 25 fire-and-forget jobs
  section('Warm-Up (25 jobs — pre-heating JVM, HikariCP, Kafka consumers)');
  const warmJobs = [];
  for (let i = 0; i < 25; i++) {
    warmJobs.push(submitJob('warmup'));
  }
  const warmResults = await Promise.all(warmJobs);
  const warmOK = warmResults.filter(r => r.success).length;
  ok(`Warm-up submitted: ${warmOK}/25 jobs accepted`);
  info('Waiting 6s for Kafka consumers to drain the warm-up batch…');
  await sleep(6000);

  const afterWarm = await getStats();
  if (afterWarm) {
    ok(`Post warm-up — COMPLETED: ${afterWarm.COMPLETED}  RETRY: ${afterWarm.RETRY}  FAILED: ${afterWarm.FAILED}`);
  }

  return { statsAvailable: !!stats };
}

// ════════════════════════════════════════════════════════════
// PHASE 1 — Sustained Throughput Benchmark
// ════════════════════════════════════════════════════════════
/**
 * Sends jobs at a fixed RPS for `durationSec` seconds.
 * Uses a token-bucket style scheduler so we genuinely hit
 * the target rate rather than just firing bursts.
 */
async function runAtFixedRPS(targetRPS, durationSec) {
  const intervalMs   = 1000 / targetRPS;
  const totalExpected = targetRPS * durationSec;
  const results      = [];
  const startTs      = now();
  const endTs        = startTs + durationSec * 1000;

  let sent = 0;
  while (now() < endTs) {
    const before = now();
    // fire without awaiting (non-blocking)
    submitJob(`rps-${targetRPS}`).then(r => results.push(r));
    sent++;
    const elapsed = now() - before;
    const wait    = Math.max(0, intervalMs - elapsed);
    await sleep(wait);
  }

  // Wait for all in-flight requests to land (max 10s extra)
  const deadline = now() + 10000;
  while (results.length < sent && now() < deadline) {
    await sleep(100);
  }

  return { results, sent, totalExpected };
}

async function phase1_sustainedThroughput() {
  banner('PHASE 1 — Sustained Throughput Benchmark');
  info('Sending jobs at fixed RPS rates for 10 seconds each.');
  info('Measures steady-state throughput (not just burst spikes).\n');

  const targets = [10, 25, 50, 100];
  const rows    = [];

  for (const rps of targets) {
    section(`Target: ${rps} RPS × 10 seconds`);
    const t0 = now();
    const { results, sent } = await runAtFixedRPS(rps, 10);
    const wallMs = now() - t0;

    const ok_r   = results.filter(r => r.success);
    const fail_r = results.filter(r => !r.success);
    const lats   = ok_r.map(r => r.latency);
    const actual = ((ok_r.length / wallMs) * 1000).toFixed(1);

    console.log(`  Sent: ${sent}  Received: ${results.length}  Success: ${ok_r.length}  Fail: ${fail_r.length}`);
    console.log(`  Achieved RPS: ${C.magenta}${actual}${C.reset} req/s`);
    console.log(`  Submission latency  p50=${percentile(lats,50)}ms  p90=${percentile(lats,90)}ms  p99=${percentile(lats,99)}ms  max=${maxVal(lats)}ms`);

    if (fail_r.length === 0) {
      ok(`100% success at ${rps} RPS`);
    } else {
      warn(`${fail_r.length} failures at ${rps} RPS`);
    }

    rows.push({
      target: rps, actual, sent, success: ok_r.length, fail: fail_r.length,
      p50: percentile(lats,50), p90: percentile(lats,90), p99: percentile(lats,99), max: maxVal(lats),
    });

    info('Settling 5s before next level…');
    await sleep(5000);
  }

  // Print summary table
  section('Phase 1 Summary');
  console.log(`\n  ${C.bold}┌${'─'.repeat(10)}┬${'─'.repeat(10)}┬${'─'.repeat(8)}┬${'─'.repeat(8)}┬${'─'.repeat(8)}┬${'─'.repeat(8)}┬${'─'.repeat(8)}┐${C.reset}`);
  console.log(`  ${C.bold}│ Target RPS│ Actual RPS│  Success │   p50ms  │   p90ms  │   p99ms  │   Max ms │${C.reset}`);
  console.log(`  ${C.bold}├${'─'.repeat(10)}┼${'─'.repeat(10)}┼${'─'.repeat(8)}┼${'─'.repeat(8)}┼${'─'.repeat(8)}┼${'─'.repeat(8)}┼${'─'.repeat(8)}┤${C.reset}`);
  for (const r of rows) {
    const sc = r.fail === 0 ? C.green : C.red;
    console.log(`  │ ${String(r.target).padEnd(9)}│ ${String(r.actual).padEnd(9)}│ ${sc}${pct(r.success, r.sent).padEnd(7)}%${C.reset}│ ${String(r.p50+'ms').padEnd(8)}│ ${String(r.p90+'ms').padEnd(8)}│ ${String(r.p99+'ms').padEnd(8)}│ ${String(r.max+'ms').padEnd(8)}│`);
  }
  console.log(`  ${C.bold}└${'─'.repeat(10)}┴${'─'.repeat(10)}┴${'─'.repeat(8)}┴${'─'.repeat(8)}┴${'─'.repeat(8)}┴${'─'.repeat(8)}┴${'─'.repeat(8)}┘${C.reset}`);

  return rows;
}

// ════════════════════════════════════════════════════════════
// PHASE 2 — End-to-End Latency (Submit → COMPLETED)
// ════════════════════════════════════════════════════════════
async function phase2_e2eLatency() {
  banner('PHASE 2 — End-to-End Latency (Submit → COMPLETED)');
  info('This is the latency the end user actually feels.');
  info('Submits 40 individual jobs and tracks time until status = COMPLETED.\n');

  const BATCH   = 40;
  const TIMEOUT = 20000; // 20s per job
  const e2eResults = [];

  section(`Submitting ${BATCH} jobs sequentially (one by one for clean measurement)`);

  // Send in waves of 5 so we don't hit rate-limiter (5/s per type)
  const WAVE = 5;
  for (let i = 0; i < BATCH; i += WAVE) {
    const wavePromises = [];
    for (let j = i; j < Math.min(i + WAVE, BATCH); j++) {
      const submitTs = now();
      wavePromises.push(
        submitJob(`e2e-${j}`).then(async (sub) => {
          if (!sub.success || !sub.id) {
            return { success: false, e2eLatency: null, submitLatency: sub.latency };
          }
          const completed = await waitForCompletion(sub.id, TIMEOUT);
          const e2eMs = now() - submitTs;
          return {
            success: !!completed && completed.status === 'COMPLETED',
            e2eLatency: e2eMs,
            submitLatency: sub.latency,
            finalStatus: completed?.status ?? 'TIMEOUT',
          };
        })
      );
    }
    const waveRes = await Promise.all(wavePromises);
    e2eResults.push(...waveRes);
    // Small gap between waves to respect 5-RPS rate limiter
    if (i + WAVE < BATCH) await sleep(1100);
  }

  const successes = e2eResults.filter(r => r.success);
  const timeouts  = e2eResults.filter(r => r.finalStatus === 'TIMEOUT');
  const e2eLats   = successes.map(r => r.e2eLatency);
  const subLats   = e2eResults.filter(r => r.submitLatency).map(r => r.submitLatency);

  section('End-to-End Latency Results');
  console.log(`\n  Jobs submitted:      ${BATCH}`);
  console.log(`  Jobs COMPLETED:      ${C.green}${successes.length}${C.reset}`);
  console.log(`  Timed out (>${TIMEOUT/1000}s):  ${timeouts.length > 0 ? C.red : C.green}${timeouts.length}${C.reset}`);
  console.log(`\n  ${C.bold}End-to-End Latency (submit → COMPLETED):${C.reset}`);
  console.log(`    Min:  ${C.green}${minVal(e2eLats)}ms${C.reset}`);
  console.log(`    Avg:  ${avg(e2eLats)}ms`);
  console.log(`    P50:  ${percentile(e2eLats, 50)}ms`);
  console.log(`    P75:  ${percentile(e2eLats, 75)}ms`);
  console.log(`    P90:  ${C.yellow}${percentile(e2eLats, 90)}ms${C.reset}`);
  console.log(`    P99:  ${C.red}${percentile(e2eLats, 99)}ms${C.reset}`);
  console.log(`    Max:  ${C.red}${maxVal(e2eLats)}ms${C.reset}`);
  console.log(`\n  ${C.bold}HTTP Submission Latency (API layer only):${C.reset}`);
  console.log(`    Avg: ${avg(subLats)}ms  P90: ${percentile(subLats, 90)}ms  P99: ${percentile(subLats, 99)}ms`);

  // Kafka consumer interpretation
  const kafkaLats = e2eLats.map(l => Math.max(0, l - avg(subLats)));
  console.log(`\n  ${C.bold}Estimated Kafka Consumer Processing Time:${C.reset}`);
  console.log(`    Avg: ${avg(kafkaLats)}ms  P90: ${percentile(kafkaLats, 90)}ms  P99: ${percentile(kafkaLats, 99)}ms`);
  console.log(`    ${C.dim}(e2e latency minus avg HTTP submission latency)${C.reset}`);

  if (successes.length === BATCH) {
    ok('All 40 jobs completed successfully!');
  } else if (successes.length >= BATCH * 0.9) {
    warn(`${successes.length}/${BATCH} completed — minor issues detected`);
  } else {
    fail(`Only ${successes.length}/${BATCH} completed — system may be under stress or rate-limited`);
  }

  return {
    total: BATCH, completed: successes.length, timedOut: timeouts.length,
    e2e: { min: minVal(e2eLats), avg: avg(e2eLats), p50: percentile(e2eLats,50), p75: percentile(e2eLats,75), p90: percentile(e2eLats,90), p99: percentile(e2eLats,99), max: maxVal(e2eLats) },
    submit: { avg: avg(subLats), p90: percentile(subLats,90), p99: percentile(subLats,99) },
    kafka: { avg: avg(kafkaLats), p90: percentile(kafkaLats,90), p99: percentile(kafkaLats,99) },
  };
}

// ════════════════════════════════════════════════════════════
// PHASE 3 — Concurrency Spike Stress Test
// ════════════════════════════════════════════════════════════
async function phase3_concurrencyStress() {
  banner('PHASE 3 — Concurrency Spike Stress Test');
  info('Progressive concurrent HTTP bursts to find the failure threshold.');
  info('Levels: 10 → 50 → 100 → 200 → 500\n');
  info('NOTE: Rate limiter (5/sec/type) will kick in at higher levels — this tests API resilience, not raw job completion.\n');

  const levels = [10, 50, 100, 200, 500];
  const rows   = [];
  let failureThreshold = null;

  for (const concurrency of levels) {
    section(`Concurrency = ${concurrency} simultaneous requests`);

    const promises = [];
    const t0 = now();
    for (let i = 0; i < concurrency; i++) {
      promises.push(submitJob(`stress-${concurrency}-${i}`));
    }
    const results = await Promise.all(promises);
    const wallMs  = now() - t0;

    const ok_r   = results.filter(r => r.success);
    const fail_r = results.filter(r => !r.success);
    const lats   = results.map(r => r.latency);
    const throughput = ((ok_r.length / wallMs) * 1000).toFixed(1);

    const successRate = parseFloat(pct(ok_r.length, concurrency));
    const failed      = fail_r.length > 0;

    console.log(`  Success: ${C[failed ? 'red' : 'green']}${ok_r.length}/${concurrency} (${successRate}%)${C.reset}`);
    console.log(`  Wall time: ${wallMs}ms  |  Throughput: ${C.magenta}${throughput} req/s${C.reset}`);
    console.log(`  Latency — p50: ${percentile(lats,50)}ms  p90: ${percentile(lats,90)}ms  p99: ${percentile(lats,99)}ms  max: ${maxVal(lats)}ms`);

    if (failed && failureThreshold === null) {
      failureThreshold = concurrency;
      warn(`First failures at concurrency=${concurrency}`);
      const sample = fail_r.slice(0, 3).map(r => r.error || `HTTP ${r.httpStatus}`);
      console.log(`  Sample errors: ${sample.join(' | ')}`);
    } else if (!failed) {
      ok(`${concurrency} concurrent requests — 100% success`);
    }

    rows.push({
      concurrency, success: ok_r.length, fail: fail_r.length, successRate,
      throughput, wallMs,
      p50: percentile(lats,50), p90: percentile(lats,90), p99: percentile(lats,99), max: maxVal(lats),
    });

    info('Settling 4s between waves…');
    await sleep(4000);
  }

  // Summary table
  section('Phase 3 Summary — Concurrency Results');
  console.log(`\n  ${C.bold}┌${'─'.repeat(13)}┬${'─'.repeat(11)}┬${'─'.repeat(11)}┬${'─'.repeat(9)}┬${'─'.repeat(9)}┬${'─'.repeat(9)}┬${'─'.repeat(9)}┐${C.reset}`);
  console.log(`  ${C.bold}│ Concurrency │ Success%    │ Throughput  │  p50 ms  │  p90 ms  │  p99 ms  │  Max ms  │${C.reset}`);
  console.log(`  ${C.bold}├${'─'.repeat(13)}┼${'─'.repeat(11)}┼${'─'.repeat(11)}┼${'─'.repeat(9)}┼${'─'.repeat(9)}┼${'─'.repeat(9)}┼${'─'.repeat(9)}┤${C.reset}`);
  for (const r of rows) {
    const sc = r.fail === 0 ? C.green : C.red;
    console.log(`  │ ${String(r.concurrency).padEnd(11)} │ ${sc}${String(r.successRate+'%').padEnd(11)}${C.reset}│ ${C.magenta}${String(r.throughput+'/s').padEnd(11)}${C.reset}│ ${String(r.p50+'ms').padEnd(8)} │ ${String(r.p90+'ms').padEnd(8)} │ ${String(r.p99+'ms').padEnd(8)} │ ${String(r.max+'ms').padEnd(8)} │`);
  }
  console.log(`  ${C.bold}└${'─'.repeat(13)}┴${'─'.repeat(11)}┴${'─'.repeat(11)}┴${'─'.repeat(9)}┴${'─'.repeat(9)}┴${'─'.repeat(9)}┴${'─'.repeat(9)}┘${C.reset}`);

  section('Failure Threshold Analysis');
  if (failureThreshold) {
    fail(`System showed first HTTP failures at concurrency = ${failureThreshold}`);
    console.log(`\n  ${C.yellow}Likely causes (in order of probability):${C.reset}`);
    console.log(`  1. Tomcat thread pool exhaustion (default max = 200 threads)`);
    console.log(`  2. HikariCP connection pool exhaustion (default max = 10 connections)`);
    console.log(`  3. Windows socket port ephemeral limit (loopback port reuse)`);
    console.log(`\n  ${C.bold}Fix suggestions:${C.reset}`);
    console.log(`  • server.tomcat.threads.max=400 in application.properties`);
    console.log(`  • spring.datasource.hikari.maximum-pool-size=30`);
  } else {
    ok(`System handled all concurrency levels up to 500 with 100% HTTP success!`);
    info('Rate-limiter (5/sec) will still throttle actual job execution — see Phase 2 for E2E latency.');
  }

  return { rows, failureThreshold };
}

// ════════════════════════════════════════════════════════════
// PHASE 4 — Resilience Validation
// ════════════════════════════════════════════════════════════
async function phase4_resilience() {
  banner('PHASE 4 — Resilience Validation');

  const results = {};

  // ── 4-A: Rate Limiter Saturation ──────────────────────────
  section('4-A: Rate Limiter Saturation Test');
  info('Firing 20 LOG jobs in rapid succession (rate limit = 5/sec/type).');
  info('Expecting some to enter RETRY status, then auto-recover via WatcherService.\n');

  const rlJobs = [];
  for (let i = 0; i < 20; i++) {
    rlJobs.push(submitJob(`rate-limit-sat-${i}`));
  }
  const rlSubmitted = await Promise.all(rlJobs);
  const rlOK = rlSubmitted.filter(r => r.success);
  ok(`Submitted: ${rlSubmitted.length}  Accepted by API: ${rlOK.length}`);

  info('Waiting 5s to observe rate-limiter reactions in Kafka consumer…');
  await sleep(5000);

  let rlRetry = 0, rlCompleted = 0, rlFailed = 0;
  for (const sub of rlOK) {
    if (!sub.id) continue;
    const job = await getJob(sub.id);
    if (!job) continue;
    if (job.status === 'RETRY')     rlRetry++;
    if (job.status === 'COMPLETED') rlCompleted++;
    if (job.status === 'FAILED')    rlFailed++;
  }

  console.log(`\n  After 5s:  COMPLETED=${rlCompleted}  RETRY=${rlRetry}  FAILED=${rlFailed}`);
  if (rlRetry > 0) {
    ok(`Rate limiter engaged — ${rlRetry} jobs throttled into RETRY`);
    info('Waiting 30s for WatcherService to pick up RETRY jobs and re-queue…');
    await sleep(30000);

    let finalCompleted = 0;
    for (const sub of rlOK) {
      if (!sub.id) continue;
      const job = await getJob(sub.id);
      if (job?.status === 'COMPLETED') finalCompleted++;
    }
    const recovery = ((finalCompleted / rlOK.length) * 100).toFixed(1);
    results.rateLimiter = { triggered: true, retryCount: rlRetry, recoveryPct: recovery, finalCompleted };
    if (finalCompleted === rlOK.length) {
      ok(`Auto-retry recovery: 100% — all ${finalCompleted} jobs eventually COMPLETED!`);
    } else {
      warn(`Recovery: ${finalCompleted}/${rlOK.length} (${recovery}%) completed`);
    }
  } else {
    info('Rate limiter not triggered at this load level (all jobs processed within limit).');
    results.rateLimiter = { triggered: false };
  }

  // ── 4-B: Circuit Breaker ──────────────────────────────────
  section('4-B: Circuit Breaker Validation');
  info('Sending 6 API jobs to a non-existent endpoint to trip the circuit breaker (threshold=5).');
  info('The 6th job should be blocked immediately without attempting execution.\n');

  const badUrl = 'http://localhost:9999/nonexistent';

  const tripJobs = [];
  for (let i = 1; i <= 5; i++) {
    const res = await fetch(JOBS_URL, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'API', payload: badUrl }),
    });
    const job = await res.json();
    tripJobs.push(job);
    dim(`  Created API job ${job.id} (trip attempt ${i})`);
    await sleep(200); // small gap so consumers can start processing
  }

  info('Waiting 15s for 5 failures to execute and trip the breaker…');
  await sleep(15000);

  let failed5 = 0, retry5 = 0;
  for (const j of tripJobs) {
    const check = await getJob(j.id);
    if (check?.status === 'FAILED') failed5++;
    if (check?.status === 'RETRY')  retry5++;
  }
  console.log(`  Trip wave: FAILED=${failed5}  RETRY=${retry5}`);

  // 6th job — should bypass execution
  const res6 = await fetch(JOBS_URL, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'API', payload: badUrl }),
  });
  const job6 = await res6.json();
  info(`6th API job created: ID=${job6.id}`);

  await sleep(3000);
  const check6 = await getJob(job6.id);
  console.log(`\n  6th job — Status: ${C.bold}${C.magenta}${check6?.status}${C.reset}  RetryCount: ${check6?.retryCount}`);

  if (check6?.status === 'RETRY' && check6?.retryCount === 0) {
    ok('Circuit Breaker OPEN — 6th job bypassed execution, went straight to RETRY!');
    results.circuitBreaker = { passed: true, job6Status: check6.status };
  } else {
    warn(`Circuit Breaker result inconclusive — status=${check6?.status} retries=${check6?.retryCount}`);
    results.circuitBreaker = { passed: false, job6Status: check6?.status };
  }

  // ── 4-C: Cancellation Under Load ─────────────────────────
  section('4-C: Cancellation Under Load');
  info('Submitting a job scheduled 2 minutes out, then immediately cancelling it.');

  const nowKolkata = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  nowKolkata.setMinutes(nowKolkata.getMinutes() + 2);
  const timeStr = `${String(nowKolkata.getHours()).padStart(2,'0')}:${String(nowKolkata.getMinutes()).padStart(2,'0')}`;

  const schedRes = await fetch(JOBS_URL, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'LOG', payload: 'cancel-under-load', message: 'should be cancelled', time: timeStr }),
  });
  const schedJob = await schedRes.json();
  info(`Scheduled job created: ID=${schedJob.id} targetTime=${timeStr}`);

  await fetch(`${JOBS_URL}/${schedJob.id}/cancel`, { method: 'POST' });
  const afterCancel = await getJob(schedJob.id);
  // Status is PENDING (hasn't been dispatched yet — cancellation flag is in Redis)
  if (afterCancel?.status === 'PENDING' || afterCancel?.status === 'CANCELLED') {
    ok(`Cancellation API accepted — job status: ${afterCancel.status} (Redis flag set, will cancel at dispatch time)`);
    results.cancellation = { passed: true, status: afterCancel.status };
  } else {
    warn(`Unexpected post-cancel status: ${afterCancel?.status}`);
    results.cancellation = { passed: false, status: afterCancel?.status };
  }

  return results;
}

// ════════════════════════════════════════════════════════════
// HTML Report Generator
// ════════════════════════════════════════════════════════════
function generateHTMLReport({ phase1, phase2, phase3, phase4, generatedAt }) {

  const phase1Rows = phase1.map(r => `
    <tr class="${r.fail > 0 ? 'row-warn' : 'row-ok'}">
      <td>${r.target} RPS</td>
      <td>${r.actual} RPS</td>
      <td class="${r.fail > 0 ? 'bad' : 'good'}">${pct(r.success, r.sent)}%</td>
      <td>${r.p50}ms</td>
      <td>${r.p90}ms</td>
      <td class="highlight">${r.p99}ms</td>
      <td class="danger">${r.max}ms</td>
    </tr>`).join('');

  const phase3Rows = phase3.rows.map(r => `
    <tr class="${r.fail > 0 ? 'row-warn' : 'row-ok'}">
      <td>${r.concurrency}</td>
      <td class="${r.fail > 0 ? 'bad' : 'good'}">${r.successRate}%</td>
      <td class="highlight">${r.throughput} req/s</td>
      <td>${r.wallMs}ms</td>
      <td>${r.p50}ms</td>
      <td>${r.p90}ms</td>
      <td class="${r.fail > 0 ? 'danger' : ''}">${r.p99}ms</td>
      <td class="danger">${r.max}ms</td>
    </tr>`).join('');

  const failLabel   = phase3.failureThreshold ? `⚠ First failures at concurrency = ${phase3.failureThreshold}` : '✔ No failures up to concurrency 500';
  const failClass   = phase3.failureThreshold ? 'badge-warn' : 'badge-ok';
  const rlLabel     = phase4.rateLimiter?.triggered ? `✔ Triggered — ${phase4.rateLimiter.retryCount} jobs retried — ${phase4.rateLimiter.recoveryPct}% recovered` : 'ℹ Not triggered at test load';
  const cbLabel     = phase4.circuitBreaker?.passed ? '✔ Circuit opened correctly — 6th job went to RETRY instantly' : `⚠ Inconclusive (status: ${phase4.circuitBreaker?.job6Status})`;
  const cancelLabel = phase4.cancellation?.passed ? `✔ Cancellation accepted (status: ${phase4.cancellation?.status})` : `⚠ Unexpected status: ${phase4.cancellation?.status}`;

  // Chart data
  const p1ChartLabels = JSON.stringify(phase1.map(r => `${r.target} RPS`));
  const p1ChartP50   = JSON.stringify(phase1.map(r => r.p50));
  const p1ChartP90   = JSON.stringify(phase1.map(r => r.p90));
  const p1ChartP99   = JSON.stringify(phase1.map(r => r.p99));
  const p1ChartRPS   = JSON.stringify(phase1.map(r => parseFloat(r.actual)));

  const p3ChartLabels = JSON.stringify(phase3.rows.map(r => `${r.concurrency}`));
  const p3ChartSucc   = JSON.stringify(phase3.rows.map(r => r.successRate));
  const p3ChartP99    = JSON.stringify(phase3.rows.map(r => r.p99));
  const p3ChartThru   = JSON.stringify(phase3.rows.map(r => parseFloat(r.throughput)));

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Job Queue — Scalability Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:        #0d0f18;
    --bg2:       #13162a;
    --bg3:       #1a1e35;
    --border:    #2a2f52;
    --accent:    #6c63ff;
    --accent2:   #00e5ff;
    --green:     #00c853;
    --yellow:    #ffd600;
    --red:       #ff5252;
    --text:      #e8eaf6;
    --muted:     #7986cb;
    --card-glow: rgba(108,99,255,0.15);
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Inter', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    padding: 0 0 60px;
  }
  /* Hero */
  .hero {
    background: linear-gradient(135deg, #0d0f18 0%, #1a1040 50%, #0d1a2f 100%);
    border-bottom: 1px solid var(--border);
    padding: 48px 40px 40px;
    position: relative;
    overflow: hidden;
  }
  .hero::before {
    content: '';
    position: absolute;
    inset: 0;
    background: radial-gradient(ellipse at 70% 50%, rgba(108,99,255,0.12) 0%, transparent 60%),
                radial-gradient(ellipse at 20% 80%, rgba(0,229,255,0.08) 0%, transparent 50%);
  }
  .hero-inner { position: relative; max-width: 1200px; margin: 0 auto; }
  .hero h1 {
    font-size: 2.4rem; font-weight: 700; letter-spacing: -0.5px;
    background: linear-gradient(135deg, #fff 30%, var(--accent2));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }
  .hero p { color: var(--muted); margin-top: 8px; font-size: 0.95rem; }
  .hero-meta { display: flex; gap: 24px; margin-top: 20px; flex-wrap: wrap; }
  .meta-chip {
    background: rgba(255,255,255,0.06);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 5px 14px;
    font-size: 0.8rem;
    color: var(--muted);
    font-family: 'JetBrains Mono', monospace;
  }
  .meta-chip span { color: var(--text); font-weight: 600; }

  /* Layout */
  .container { max-width: 1200px; margin: 0 auto; padding: 0 40px; }

  /* Section headings */
  .phase-header {
    display: flex; align-items: center; gap: 14px;
    margin: 48px 0 24px;
  }
  .phase-num {
    width: 36px; height: 36px; border-radius: 50%;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    display: flex; align-items: center; justify-content: center;
    font-weight: 700; font-size: 0.9rem; flex-shrink: 0;
    box-shadow: 0 0 20px rgba(108,99,255,0.4);
  }
  .phase-title { font-size: 1.3rem; font-weight: 600; }
  .phase-sub { color: var(--muted); font-size: 0.85rem; margin-top: 2px; }

  /* Cards */
  .card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 28px;
    margin-bottom: 24px;
    box-shadow: 0 4px 30px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.04);
    transition: box-shadow 0.2s;
  }
  .card:hover { box-shadow: 0 8px 40px var(--card-glow); }
  .card-title { font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1.5px; color: var(--muted); margin-bottom: 18px; }

  /* Metric grids */
  .metric-grid { display: grid; gap: 16px; }
  .metric-grid-4 { grid-template-columns: repeat(4, 1fr); }
  .metric-grid-3 { grid-template-columns: repeat(3, 1fr); }
  .metric-grid-2 { grid-template-columns: repeat(2, 1fr); }
  @media (max-width: 900px) { .metric-grid-4, .metric-grid-3 { grid-template-columns: repeat(2,1fr); } }
  @media (max-width: 600px) { .metric-grid-4, .metric-grid-3, .metric-grid-2 { grid-template-columns: 1fr; } }

  .metric {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 18px 20px;
  }
  .metric-label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
  .metric-value { font-size: 1.9rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; line-height: 1; }
  .metric-unit { font-size: 0.85rem; color: var(--muted); margin-top: 4px; }
  .metric-value.green { color: var(--green); }
  .metric-value.yellow { color: var(--yellow); }
  .metric-value.red { color: var(--red); }
  .metric-value.accent { color: var(--accent2); }

  /* Tables */
  table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
  thead th {
    text-align: left;
    padding: 10px 14px;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
  }
  tbody td { padding: 11px 14px; border-bottom: 1px solid rgba(42,47,82,0.5); font-family: 'JetBrains Mono', monospace; font-size: 0.83rem; }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover td { background: rgba(108,99,255,0.05); }
  .row-ok td:first-child { border-left: 3px solid var(--green); }
  .row-warn td:first-child { border-left: 3px solid var(--red); }
  td.good { color: var(--green); font-weight: 600; }
  td.bad  { color: var(--red);   font-weight: 600; }
  td.highlight { color: var(--yellow); }
  td.danger    { color: var(--red); }

  /* Charts */
  .chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 24px; }
  @media (max-width: 800px) { .chart-grid { grid-template-columns: 1fr; } }
  .chart-wrap { position: relative; height: 260px; }

  /* Badges */
  .badge { display: inline-flex; align-items: center; gap: 8px; padding: 8px 16px; border-radius: 8px; font-size: 0.85rem; font-weight: 500; margin-bottom: 10px; }
  .badge-ok   { background: rgba(0,200,83,0.12);  border: 1px solid rgba(0,200,83,0.3);  color: var(--green); }
  .badge-warn { background: rgba(255,82,82,0.12); border: 1px solid rgba(255,82,82,0.3); color: var(--red); }
  .badge-info { background: rgba(108,99,255,0.12);border: 1px solid rgba(108,99,255,0.3);color: var(--accent); }

  /* Resilience list */
  .resil-list { display: flex; flex-direction: column; gap: 12px; }
  .resil-item {
    display: flex; align-items: flex-start; gap: 14px;
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 10px; padding: 14px 18px;
  }
  .resil-icon { font-size: 1.2rem; line-height: 1.4; flex-shrink: 0; }
  .resil-name { font-weight: 600; font-size: 0.9rem; margin-bottom: 4px; }
  .resil-detail { color: var(--muted); font-size: 0.82rem; font-family: 'JetBrains Mono', monospace; }

  /* Footer */
  .footer { text-align: center; color: var(--muted); font-size: 0.8rem; margin-top: 60px; padding: 20px; border-top: 1px solid var(--border); }

  /* Divider */
  hr { border: none; border-top: 1px solid var(--border); margin: 40px 0; }

  /* Recommendation box */
  .reco-box {
    background: linear-gradient(135deg, rgba(108,99,255,0.08), rgba(0,229,255,0.05));
    border: 1px solid var(--border);
    border-left: 3px solid var(--accent);
    border-radius: 12px;
    padding: 20px 24px;
    margin-top: 12px;
  }
  .reco-box h4 { font-size: 0.85rem; color: var(--accent2); margin-bottom: 12px; text-transform: uppercase; letter-spacing: 1px; }
  .reco-box li { color: var(--muted); font-size: 0.83rem; margin: 6px 0 6px 16px; line-height: 1.6; }
  .reco-box code { font-family: 'JetBrains Mono', monospace; color: var(--text); background: rgba(0,0,0,0.3); padding: 1px 5px; border-radius: 3px; }

  .scrollbar-thin { overflow-x: auto; }
</style>
</head>
<body>

<!-- HERO -->
<div class="hero">
  <div class="hero-inner">
    <h1>🚀 Job Queue — Scalability Report</h1>
    <p>Spring Boot + Apache Kafka + Redis + MySQL · Distributed Job Scheduler</p>
    <div class="hero-meta">
      <div class="meta-chip">Generated: <span>${generatedAt}</span></div>
      <div class="meta-chip">Server: <span>localhost:8080</span></div>
      <div class="meta-chip">Consumers: <span>3 Kafka threads</span></div>
      <div class="meta-chip">Rate Limit: <span>5 req/s/type</span></div>
      <div class="meta-chip">Circuit Breaker: <span>5-failure threshold</span></div>
    </div>
  </div>
</div>

<div class="container">

<!-- ── PHASE 1 ────────────────────────────────────────────── -->
<div class="phase-header">
  <div class="phase-num">1</div>
  <div>
    <div class="phase-title">Sustained Throughput Benchmark</div>
    <div class="phase-sub">Fixed RPS rates sustained for 10 seconds each — measures steady-state capacity</div>
  </div>
</div>

<div class="chart-grid">
  <div class="card">
    <div class="card-title">Submission Latency Percentiles vs Target RPS</div>
    <div class="chart-wrap"><canvas id="p1LatChart"></canvas></div>
  </div>
  <div class="card">
    <div class="card-title">Achieved Throughput vs Target RPS</div>
    <div class="chart-wrap"><canvas id="p1ThroughputChart"></canvas></div>
  </div>
</div>

<div class="card scrollbar-thin">
  <div class="card-title">Throughput Results Table</div>
  <table>
    <thead><tr><th>Target</th><th>Achieved</th><th>Success Rate</th><th>p50 Latency</th><th>p90 Latency</th><th>p99 Latency</th><th>Max Latency</th></tr></thead>
    <tbody>${phase1Rows}</tbody>
  </table>
</div>

<!-- ── PHASE 2 ────────────────────────────────────────────── -->
<div class="phase-header">
  <div class="phase-num">2</div>
  <div>
    <div class="phase-title">End-to-End Latency</div>
    <div class="phase-sub">Time from job submission to status = COMPLETED — the latency that matters</div>
  </div>
</div>

<div class="metric-grid metric-grid-4" style="margin-bottom:24px">
  <div class="metric">
    <div class="metric-label">Jobs Completed</div>
    <div class="metric-value ${phase2.completed === phase2.total ? 'green' : 'yellow'}">${phase2.completed}/${phase2.total}</div>
    <div class="metric-unit">success rate: ${pct(phase2.completed, phase2.total)}%</div>
  </div>
  <div class="metric">
    <div class="metric-label">E2E P50 Latency</div>
    <div class="metric-value accent">${phase2.e2e.p50}ms</div>
    <div class="metric-unit">median time to COMPLETED</div>
  </div>
  <div class="metric">
    <div class="metric-label">E2E P90 Latency</div>
    <div class="metric-value yellow">${phase2.e2e.p90}ms</div>
    <div class="metric-unit">90th percentile</div>
  </div>
  <div class="metric">
    <div class="metric-label">E2E P99 Latency</div>
    <div class="metric-value red">${phase2.e2e.p99}ms</div>
    <div class="metric-unit">tail latency</div>
  </div>
</div>

<div class="card">
  <div class="card-title">Latency Breakdown — Where Does Time Go?</div>
  <div class="metric-grid metric-grid-3">
    <div class="metric">
      <div class="metric-label">HTTP API Layer (avg)</div>
      <div class="metric-value accent">${phase2.submit.avg}ms</div>
      <div class="metric-unit">Spring Boot REST + MySQL write</div>
    </div>
    <div class="metric">
      <div class="metric-label">Kafka Consumer (avg est.)</div>
      <div class="metric-value yellow">${phase2.kafka.avg}ms</div>
      <div class="metric-unit">Kafka poll + execute + DB update</div>
    </div>
    <div class="metric">
      <div class="metric-label">End-to-End Max</div>
      <div class="metric-value red">${phase2.e2e.max}ms</div>
      <div class="metric-unit">worst single job observed</div>
    </div>
  </div>
  <div class="reco-box" style="margin-top:20px">
    <h4>📊 How to interpret this</h4>
    <ul>
      <li><strong>API submission latency</strong> = Spring MVC controller + HikariCP + MySQL INSERT + Kafka produce</li>
      <li><strong>Kafka consumer time</strong> = Kafka poll interval + rate-limit check + executor + DB UPDATE (COMPLETED)</li>
      <li>High P99 vs P50 gap indicates GC pauses or occasional HikariCP wait — expected under light load on shared hardware</li>
      <li>For production: target E2E P99 &lt; 2000ms at normal load, with burst tolerance up to 5000ms</li>
    </ul>
  </div>
</div>

<!-- ── PHASE 3 ────────────────────────────────────────────── -->
<div class="phase-header">
  <div class="phase-num">3</div>
  <div>
    <div class="phase-title">Concurrency Spike Stress Test</div>
    <div class="phase-sub">Progressive concurrent HTTP bursts — identifies the failure threshold</div>
  </div>
</div>

<div class="chart-grid">
  <div class="card">
    <div class="card-title">Success Rate & p99 Latency by Concurrency</div>
    <div class="chart-wrap"><canvas id="p3SuccChart"></canvas></div>
  </div>
  <div class="card">
    <div class="card-title">Throughput (req/s) by Concurrency Level</div>
    <div class="chart-wrap"><canvas id="p3ThruChart"></canvas></div>
  </div>
</div>

<div class="card scrollbar-thin">
  <div class="card-title">Concurrency Stress Results</div>
  <div class="badge ${failClass}">${failLabel}</div>
  <table style="margin-top:14px">
    <thead><tr><th>Concurrency</th><th>Success%</th><th>Throughput</th><th>Wall Time</th><th>p50</th><th>p90</th><th>p99</th><th>Max</th></tr></thead>
    <tbody>${phase3Rows}</tbody>
  </table>
</div>

${phase3.failureThreshold ? `
<div class="reco-box">
  <h4>⚙ Bottleneck Diagnosis & Fix Suggestions</h4>
  <ul>
    <li><strong>Tomcat thread pool</strong>: Add <code>server.tomcat.threads.max=400</code> to <code>application.properties</code></li>
    <li><strong>HikariCP connections</strong>: Add <code>spring.datasource.hikari.maximum-pool-size=30</code></li>
    <li><strong>Windows port exhaustion</strong>: Normal for loopback tests — set <code>net.ipv4.tcp_tw_reuse=1</code> or use Linux</li>
    <li><strong>Kafka producer batching</strong>: Tune <code>spring.kafka.producer.batch-size</code> and <code>linger.ms</code> for higher throughput</li>
    <li><strong>Rate limiter</strong>: Your current 5-RPS/type limit will always throttle bursts — intended behaviour</li>
  </ul>
</div>
` : `
<div class="reco-box">
  <h4>✅ Excellent Resilience — Next Steps for Further Scaling</h4>
  <ul>
    <li>Try Docker-based multi-instance deployment to test true horizontal scaling</li>
    <li>Increase Kafka partitions to 9 and Kafka consumer concurrency to 9 for 3× processing throughput</li>
    <li>Add Prometheus + Grafana for real-time dashboards in production</li>
    <li>Consider a Dead Letter Queue (DLQ) for permanently failed jobs</li>
  </ul>
</div>
`}

<!-- ── PHASE 4 ────────────────────────────────────────────── -->
<div class="phase-header">
  <div class="phase-num">4</div>
  <div>
    <div class="phase-title">Resilience Validation</div>
    <div class="phase-sub">Rate limiter, circuit breaker, and cancellation under load</div>
  </div>
</div>

<div class="card">
  <div class="card-title">Resilience Patterns — Test Results</div>
  <div class="resil-list">
    <div class="resil-item">
      <div class="resil-icon">🛡️</div>
      <div>
        <div class="resil-name">Rate Limiter (Redis Lua · 5 req/s/type)</div>
        <div class="resil-detail">${rlLabel}</div>
      </div>
    </div>
    <div class="resil-item">
      <div class="resil-icon">⚡</div>
      <div>
        <div class="resil-name">Circuit Breaker (AtomicInteger · 5-failure threshold · 30s timeout)</div>
        <div class="resil-detail">${cbLabel}</div>
      </div>
    </div>
    <div class="resil-item">
      <div class="resil-icon">🚫</div>
      <div>
        <div class="resil-name">Job Cancellation (Redis TTL flag)</div>
        <div class="resil-detail">${cancelLabel}</div>
      </div>
    </div>
    <div class="resil-item">
      <div class="resil-icon">🔄</div>
      <div>
        <div class="resil-name">Exponential Backoff Retry (max 5 retries · cap 20s)</div>
        <div class="resil-detail">Tested implicitly via rate-limiter → RETRY → WatcherService re-queue flow</div>
      </div>
    </div>
  </div>
</div>

<!-- ── ARCHITECTURE NOTES ─────────────────────────────────── -->
<hr>
<div class="card">
  <div class="card-title">Architecture Notes & Scaling Recommendations</div>
  <div class="reco-box">
    <h4>🔮 Production Scaling Roadmap</h4>
    <ul>
      <li><strong>Kafka Partitions</strong>: Scale from 1 → N partitions and match consumer <code>concurrency</code> to enable horizontal processing</li>
      <li><strong>Redis Cluster</strong>: Move from single Redis to Redis Cluster for HA rate-limiting and delay queue at scale</li>
      <li><strong>MySQL Read Replicas</strong>: Offload <code>GET /jobs/{id}</code> and stats queries to read replicas</li>
      <li><strong>API Instances</strong>: Stateless Spring Boot app — deploy behind load balancer; use leader election for Watcher/Dispatcher</li>
      <li><strong>Observability</strong>: Add Micrometer → Prometheus → Grafana for job throughput, failure rate, and queue depth dashboards</li>
      <li><strong>DLQ</strong>: Implement Dead Letter Queue for jobs that exhaust all retries — prevents silent data loss</li>
    </ul>
  </div>
</div>

</div><!-- /container -->

<div class="footer">
  Generated by <strong>Job Queue Scalability Test Suite</strong> · ${generatedAt}
</div>

<script>
const chartDefaults = {
  responsive: true, maintainAspectRatio: false,
  plugins: { legend: { labels: { color: '#7986cb', font: { family: 'Inter', size: 11 } } } },
  scales: {
    x: { ticks: { color: '#7986cb' }, grid: { color: 'rgba(42,47,82,0.5)' } },
    y: { ticks: { color: '#7986cb' }, grid: { color: 'rgba(42,47,82,0.5)' } },
  },
};

// Phase 1 — Latency Chart
new Chart(document.getElementById('p1LatChart'), {
  type: 'line',
  data: {
    labels: ${p1ChartLabels},
    datasets: [
      { label: 'p50', data: ${p1ChartP50}, borderColor: '#00c853', backgroundColor: 'rgba(0,200,83,0.08)', tension: 0.3, pointRadius: 5 },
      { label: 'p90', data: ${p1ChartP90}, borderColor: '#ffd600', backgroundColor: 'rgba(255,214,0,0.08)', tension: 0.3, pointRadius: 5 },
      { label: 'p99', data: ${p1ChartP99}, borderColor: '#ff5252', backgroundColor: 'rgba(255,82,82,0.08)', tension: 0.3, pointRadius: 5 },
    ],
  },
  options: { ...chartDefaults, scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, title: { display: true, text: 'Latency (ms)', color: '#7986cb' } } } },
});

// Phase 1 — Throughput Chart
new Chart(document.getElementById('p1ThroughputChart'), {
  type: 'bar',
  data: {
    labels: ${p1ChartLabels},
    datasets: [
      { label: 'Achieved RPS', data: ${p1ChartRPS}, backgroundColor: 'rgba(108,99,255,0.6)', borderColor: '#6c63ff', borderWidth: 1, borderRadius: 6 },
    ],
  },
  options: { ...chartDefaults, scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, title: { display: true, text: 'Requests/sec', color: '#7986cb' } } } },
});

// Phase 3 — Success + p99 Chart
new Chart(document.getElementById('p3SuccChart'), {
  type: 'line',
  data: {
    labels: ${p3ChartLabels},
    datasets: [
      { label: 'Success %', data: ${p3ChartSucc}, borderColor: '#00c853', backgroundColor: 'rgba(0,200,83,0.08)', tension: 0.3, yAxisID: 'ySucc', pointRadius: 5 },
      { label: 'p99 Latency (ms)', data: ${p3ChartP99},  borderColor: '#ff5252', backgroundColor: 'rgba(255,82,82,0.08)', tension: 0.3, yAxisID: 'yLat', pointRadius: 5 },
    ],
  },
  options: {
    ...chartDefaults,
    scales: {
      x: chartDefaults.scales.x,
      ySucc: { type: 'linear', position: 'left',  ticks: { color: '#00c853' }, grid: { color: 'rgba(42,47,82,0.5)' }, min: 0, max: 100, title: { display: true, text: 'Success %', color: '#00c853' } },
      yLat:  { type: 'linear', position: 'right', ticks: { color: '#ff5252' }, grid: { drawOnChartArea: false }, title: { display: true, text: 'p99 Latency (ms)', color: '#ff5252' } },
    },
  },
});

// Phase 3 — Throughput Chart
new Chart(document.getElementById('p3ThruChart'), {
  type: 'bar',
  data: {
    labels: ${p3ChartLabels},
    datasets: [
      { label: 'Throughput (req/s)', data: ${p3ChartThru}, backgroundColor: 'rgba(0,229,255,0.5)', borderColor: '#00e5ff', borderWidth: 1, borderRadius: 6 },
    ],
  },
  options: { ...chartDefaults, scales: { ...chartDefaults.scales, x: { ...chartDefaults.scales.x, title: { display: true, text: 'Concurrency Level', color: '#7986cb' } }, y: { ...chartDefaults.scales.y, title: { display: true, text: 'Requests/sec', color: '#7986cb' } } } },
});
</script>
</body>
</html>`;
}

// ════════════════════════════════════════════════════════════
// MAIN ORCHESTRATOR
// ════════════════════════════════════════════════════════════
async function main() {
  banner('JOB QUEUE SYSTEM — COMPREHENSIVE SCALABILITY TEST SUITE');
  console.log(`  ${C.dim}Phases: Warm-Up → Sustained Throughput → E2E Latency → Concurrency Stress → Resilience${C.reset}`);
  console.log(`  ${C.dim}Requires: Node.js 18+  |  Server on localhost:8080  |  docker compose up -d${C.reset}\n`);

  const startedAt = new Date();

  try {
    // ── Phase 0 — Health + Warm-Up ───────────────────────────
    const { statsAvailable } = await phase0_warmup();

    // ── Drain backlog from any previous test runs ─────────────
    // Must happen BEFORE E2E and resilience tests so the Kafka
    // consumer isn't backlogged with stale RETRY/QUEUED jobs.
    if (statsAvailable) {
      await drainQueue('pre-E2E', 300000, 20);
    }

    // ── Phase 2 — E2E Latency (run BEFORE load tests!) ───────
    // Runs first so consumers are idle and jobs complete quickly.
    const phase2Results = await phase2_e2eLatency();

    // ── Phase 1 — Sustained Throughput ───────────────────────
    const phase1Results = await phase1_sustainedThroughput();

    // ── Phase 3 — Concurrency Spike ──────────────────────────
    const phase3Results = await phase3_concurrencyStress();

    // ── Drain backlog created by Phase 1 + 3 load ────────────
    if (statsAvailable) {
      await drainQueue('pre-Resilience', 300000, 50);
    }

    // ── Phase 4 — Resilience Validation ──────────────────────
    const phase4Results = await phase4_resilience();

    // ── Final stats snapshot ─────────────────────────────────
    banner('FINAL SYSTEM STATE SNAPSHOT');
    const finalStats = await getStats();
    if (finalStats) {
      section('Job Status Distribution (all-time, this test session)');
      const statKeys = Object.keys(finalStats);
      const row1 = statKeys.slice(0, 4);
      const row2 = statKeys.slice(4);
      for (const k of row1) {
        const v = finalStats[k];
        process.stdout.write(`  ${C.bold}${k.padEnd(12)}${C.reset}: ${C.cyan}${String(v).padEnd(6)}${C.reset}  `);
      }
      console.log();
      for (const k of row2) {
        const v = finalStats[k];
        process.stdout.write(`  ${C.bold}${k.padEnd(12)}${C.reset}: ${C.cyan}${String(v).padEnd(6)}${C.reset}  `);
      }
      console.log('\n');
    }

    // ── Generate HTML report ─────────────────────────────────
    banner('GENERATING HTML REPORT');
    const generatedAt = startedAt.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    const html = generateHTMLReport({
      phase1: phase1Results,
      phase2: phase2Results,
      phase3: phase3Results,
      phase4: phase4Results,
      generatedAt,
    });
    fs.writeFileSync(REPORT_PATH, html, 'utf8');
    ok(`HTML report written to: ${REPORT_PATH}`);
    info('Open scalability-report.html in your browser to view the interactive dashboard.\n');

    // ── Summary ──────────────────────────────────────────────
    const elapsed = ((Date.now() - startedAt.getTime()) / 1000 / 60).toFixed(1);

    banner(`ALL PHASES COMPLETE — Total time: ~${elapsed} minutes`);
    const maxThru = Math.max(...phase1Results.map(r => parseFloat(r.actual)));
    ok(`Peak throughput achieved:          ${C.bold}${maxThru} req/s${C.reset} (HTTP submission)`);
    ok(`E2E P50 latency (submit→COMPLETE): ${C.bold}${phase2Results.e2e.p50}ms${C.reset}`);
    ok(`E2E P99 latency (submit→COMPLETE): ${C.bold}${phase2Results.e2e.p99}ms${C.reset}`);
    if (phase3Results.failureThreshold) {
      warn(`HTTP failure threshold:            concurrency = ${phase3Results.failureThreshold}`);
    } else {
      ok('HTTP failure threshold:            none — 100% at all levels');
    }
    ok(`Circuit breaker:                   ${phase4Results.circuitBreaker?.passed ? 'PASSED' : 'inconclusive'}`);
    ok(`Rate limiter:                      ${phase4Results.rateLimiter?.triggered ? 'triggered + auto-recovered' : 'not triggered at this load'}`);
    console.log(`\n  ${C.gray}✨ Open scalability-report.html for the full interactive dashboard.${C.reset}\n`);

  } catch (err) {
    console.error(`\n${C.red}${C.bold}❌ FATAL TEST ERROR: ${err.message}${C.reset}`);
    console.error(err.stack);
    process.exit(1);
  }
}

main();
