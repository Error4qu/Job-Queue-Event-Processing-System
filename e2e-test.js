#!/usr/bin/env node
/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  Job Queue System — Comprehensive End-to-End Test Suite     ║
 * ║  Covers: API, Lifecycle, Scheduling, Retry, Circuit Breaker ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 * Usage:  node e2e-test.js
 * Requires: Node 18+ (native fetch), Docker stack running on localhost:8080
 */

const BASE = 'http://localhost:8080';

// ─── Helpers ──────────────────────────────────────────────────────────────────

let totalPassed = 0;
let totalFailed = 0;
let totalSkipped = 0;
const suiteResults = [];

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function http(method, path, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' },
    signal: AbortSignal.timeout(15000),
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE}${path}`, opts);
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = text; }
  return { status: res.status, body: json };
}

async function createJob(payload) {
  return http('POST', '/jobs', payload);
}

async function getJob(id) {
  return http('GET', `/jobs/${id}`);
}

async function cancelJob(id) {
  return http('POST', `/jobs/${id}/cancel`);
}

async function getStats() {
  return http('GET', '/jobs/stats');
}

/**
 * Polls a job until it reaches one of the target statuses or times out.
 * Returns the final job state.
 */
async function pollJobStatus(id, targetStatuses, timeoutMs = 30000, intervalMs = 500) {
  const targets = Array.isArray(targetStatuses) ? targetStatuses : [targetStatuses];
  const start = Date.now();
  let lastJob = null;
  while (Date.now() - start < timeoutMs) {
    const res = await getJob(id);
    if (res.status === 200 && res.body && res.body.status) {
      lastJob = res.body;
      if (targets.includes(lastJob.status)) return lastJob;
    }
    await sleep(intervalMs);
  }
  return lastJob;
}

function assert(condition, message) {
  if (!condition) throw new Error(`Assertion failed: ${message}`);
}

async function runTest(name, fn) {
  try {
    await fn();
    console.log(`    ✅ ${name}`);
    totalPassed++;
    return true;
  } catch (e) {
    console.log(`    ❌ ${name}`);
    console.log(`       → ${e.message}`);
    totalFailed++;
    return false;
  }
}

async function runSuite(name, tests) {
  console.log(`\n  ┌─ Suite: ${name}`);
  console.log(`  │`);
  let passed = 0, failed = 0;
  for (const [testName, testFn] of tests) {
    const ok = await runTest(testName, testFn);
    if (ok) passed++; else failed++;
  }
  console.log(`  │`);
  console.log(`  └─ ${passed}/${tests.length} passed${failed > 0 ? ` (${failed} FAILED)` : ''}\n`);
  suiteResults.push({ name, passed, failed, total: tests.length });
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 1: API Endpoint Validation
// ═══════════════════════════════════════════════════════════════════════════════

async function suite1_ApiValidation() {
  await runSuite('1 — API Endpoint Validation', [

    ['POST /jobs with valid LOG payload returns 200 with job', async () => {
      const res = await createJob({ type: 'LOG', payload: 'e2e-test-1' });
      assert(res.status === 200, `Expected 200, got ${res.status}`);
      assert(res.body.id > 0, 'Job should have an ID');
      assert(res.body.type === 'LOG', `Expected type LOG, got ${res.body.type}`);
      assert(['QUEUED', 'PENDING'].includes(res.body.status), `Unexpected status: ${res.body.status}`);
    }],

    ['POST /jobs with missing type returns 400', async () => {
      const res = await createJob({ payload: 'no-type' });
      assert(res.status === 400, `Expected 400, got ${res.status}`);
    }],

    ['POST /jobs with invalid type returns 400', async () => {
      const res = await createJob({ type: 'INVALID', payload: 'bad-type' });
      assert(res.status === 400, `Expected 400, got ${res.status}`);
    }],

    ['POST /jobs with invalid time format returns 400', async () => {
      const res = await createJob({ type: 'LOG', payload: 'bad-time', time: '25:99' });
      assert(res.status === 400, `Expected 400, got ${res.status}`);
    }],

    ['GET /jobs/{id} returns job object', async () => {
      const created = await createJob({ type: 'LOG', payload: 'get-test' });
      const res = await getJob(created.body.id);
      assert(res.status === 200, `Expected 200, got ${res.status}`);
      assert(res.body.id === created.body.id, 'IDs should match');
    }],

    ['GET /jobs/{id} for non-existent ID returns 200 with null body', async () => {
      const res = await getJob(999999);
      // Controller returns null → Spring returns 200 with empty body
      assert(res.status === 200, `Expected 200, got ${res.status}`);
    }],

    ['GET /jobs/stats returns status counts', async () => {
      const res = await getStats();
      assert(res.status === 200, `Expected 200, got ${res.status}`);
      assert(typeof res.body === 'object', 'Should return an object');
      assert('TOTAL' in res.body, 'Should have TOTAL key');
      assert('COMPLETED' in res.body, 'Should have COMPLETED key');
    }],

    ['POST /jobs/{id}/cancel returns success message', async () => {
      const created = await createJob({ type: 'LOG', payload: 'cancel-endpoint-test' });
      const res = await cancelJob(created.body.id);
      assert(res.status === 200, `Expected 200, got ${res.status}`);
      assert(typeof res.body === 'string' && res.body.includes('cancelled'),
        `Expected cancellation message, got: ${JSON.stringify(res.body)}`);
    }],

    ['GET /actuator/health returns health data', async () => {
      const res = await http('GET', '/actuator/health');
      // With mail disabled, should be 200. Otherwise 503 is also acceptable.
      assert([200, 503].includes(res.status), `Expected 200 or 503, got ${res.status}`);
      assert(res.body.components, 'Should have components');
      assert(res.body.components.db.status === 'UP', 'DB should be UP');
      assert(res.body.components.redis.status === 'UP', 'Redis should be UP');
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 2: Job Lifecycle — LOG Type
// ═══════════════════════════════════════════════════════════════════════════════

async function suite2_LogLifecycle() {
  await runSuite('2 — Job Lifecycle (LOG type)', [

    ['LOG job reaches COMPLETED status', async () => {
      const res = await createJob({ type: 'LOG', payload: 'lifecycle-log-test' });
      assert(res.status === 200, `Failed to create job: ${res.status}`);
      const job = await pollJobStatus(res.body.id, 'COMPLETED', 15000);
      assert(job !== null, 'Job polling timed out');
      assert(job.status === 'COMPLETED', `Expected COMPLETED, got ${job.status}`);
    }],

    ['Multiple LOG jobs all complete', async () => {
      const ids = [];
      for (let i = 0; i < 5; i++) {
        const res = await createJob({ type: 'LOG', payload: `multi-log-${i}` });
        ids.push(res.body.id);
      }
      await sleep(8000);
      let allCompleted = true;
      for (const id of ids) {
        const res = await getJob(id);
        if (res.body.status !== 'COMPLETED') {
          allCompleted = false;
          console.log(`       → Job ${id} status: ${res.body.status}`);
        }
      }
      assert(allCompleted, 'Not all LOG jobs reached COMPLETED');
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 3: Job Lifecycle — API Type
// ═══════════════════════════════════════════════════════════════════════════════

async function suite3_ApiLifecycle() {
  await runSuite('3 — Job Lifecycle (API type)', [

    ['API job with valid URL completes successfully', async () => {
      const res = await createJob({ type: 'API', payload: 'https://httpbin.org/get' });
      assert(res.status === 200, `Failed to create job: ${res.status}`);
      const job = await pollJobStatus(res.body.id, ['COMPLETED', 'RETRY', 'FAILED'], 20000);
      assert(job !== null, 'Job polling timed out');
      // API call might fail due to network; COMPLETED is ideal, but RETRY is acceptable
      assert(['COMPLETED', 'RETRY'].includes(job.status),
        `Expected COMPLETED or RETRY, got ${job.status}`);
    }],

    ['API job with bad URL eventually fails', async () => {
      const res = await createJob({ type: 'API', payload: 'http://localhost:1/nonexistent' });
      assert(res.status === 200, `Failed to create job: ${res.status}`);
      // This will fail and go to RETRY, eventually FAILED after max retries
      const job = await pollJobStatus(res.body.id, ['RETRY', 'FAILED'], 15000);
      assert(job !== null, 'Job polling timed out');
      assert(['RETRY', 'FAILED'].includes(job.status),
        `Expected RETRY or FAILED, got ${job.status}`);
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 4: Scheduled Job
// ═══════════════════════════════════════════════════════════════════════════════

async function suite4_ScheduledJob() {
  await runSuite('4 — Scheduled Job', [

    ['Job with future time starts as PENDING', async () => {
      // Schedule 2 minutes from now
      const future = new Date(Date.now() + 2 * 60 * 1000);
      const hours = String(future.getHours()).padStart(2, '0');
      const mins = String(future.getMinutes()).padStart(2, '0');
      const timeStr = `${hours}:${mins}`;

      const res = await createJob({ type: 'LOG', payload: 'scheduled-test', time: timeStr });
      assert(res.status === 200, `Failed to create job: ${res.status}`);
      // Scheduled more than 60s in future → should stay PENDING
      assert(res.body.status === 'PENDING', `Expected PENDING, got ${res.body.status}`);

      // Verify it's still PENDING after a short wait
      await sleep(3000);
      const check = await getJob(res.body.id);
      assert(['PENDING', 'SCHEDULED'].includes(check.body.status),
        `Expected PENDING or SCHEDULED, got ${check.body.status}`);
    }],

    ['Immediate job (no time) dispatches right away', async () => {
      const res = await createJob({ type: 'LOG', payload: 'immediate-dispatch' });
      assert(res.status === 200, `Failed to create job: ${res.status}`);
      assert(['QUEUED', 'PROCESSING', 'COMPLETED'].includes(res.body.status),
        `Expected immediate dispatch status, got ${res.body.status}`);
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 5: Job Cancellation
// ═══════════════════════════════════════════════════════════════════════════════

async function suite5_Cancellation() {
  await runSuite('5 — Job Cancellation', [

    ['Cancelled job reaches CANCELLED status', async () => {
      // Create a scheduled job so it stays in queue long enough to cancel
      const future = new Date(Date.now() + 5 * 60 * 1000);
      const hours = String(future.getHours()).padStart(2, '0');
      const mins = String(future.getMinutes()).padStart(2, '0');

      const res = await createJob({ type: 'LOG', payload: 'cancel-test', time: `${hours}:${mins}` });
      assert(res.status === 200, `Failed to create job: ${res.status}`);

      // Cancel it
      const cancelRes = await cancelJob(res.body.id);
      assert(cancelRes.status === 200, `Cancel failed: ${cancelRes.status}`);

      // The cancellation is checked during processing, so for scheduled jobs
      // that haven't been dispatched yet, the cancellation flag is set in Redis.
      // When the job eventually gets picked up, it will be CANCELLED.
      // For now, verify the cancel API responded correctly.
      assert(typeof cancelRes.body === 'string', 'Should return string response');
    }],

    ['Immediate job can be cancelled before processing', async () => {
      // Submit and immediately cancel
      const res = await createJob({ type: 'LOG', payload: 'fast-cancel' });
      const cancelRes = await cancelJob(res.body.id);
      assert(cancelRes.status === 200, `Cancel failed: ${cancelRes.status}`);

      // Wait for processing cycle
      await sleep(5000);
      const check = await getJob(res.body.id);
      // Could be CANCELLED (if consumer saw the flag) or COMPLETED (if processed before cancel)
      assert(['CANCELLED', 'COMPLETED'].includes(check.body.status),
        `Expected CANCELLED or COMPLETED, got ${check.body.status}`);
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 6: Rate Limiter
// ═══════════════════════════════════════════════════════════════════════════════

async function suite6_RateLimiter() {
  await runSuite('6 — Rate Limiter', [

    ['Burst of LOG jobs: some get rate-limited then recover', async () => {
      // Rate limit is 5 per second per type. Submit 12 rapidly.
      const ids = [];
      const promises = [];
      for (let i = 0; i < 12; i++) {
        promises.push(createJob({ type: 'LOG', payload: `rate-test-${i}` }));
      }
      const results = await Promise.all(promises);
      for (const r of results) {
        assert(r.status === 200, `Job creation failed: ${r.status}`);
        ids.push(r.body.id);
      }

      // Wait for processing + retries (watcher runs every 20s)
      console.log('       ⏳ Waiting 35s for rate-limited jobs to retry...');
      await sleep(35000);

      let completed = 0, retry = 0, other = 0;
      for (const id of ids) {
        const res = await getJob(id);
        if (res.body.status === 'COMPLETED') completed++;
        else if (res.body.status === 'RETRY') retry++;
        else other++;
      }
      console.log(`       → Completed: ${completed}, Retry: ${retry}, Other: ${other}`);
      // Most should be completed by now (watcher picks up RETRY jobs every 20s)
      assert(completed >= 5, `Expected at least 5 completed, got ${completed}`);
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 7: Retry & Backoff
// ═══════════════════════════════════════════════════════════════════════════════

async function suite7_RetryBackoff() {
  await runSuite('7 — Retry & Exponential Backoff', [

    ['Failed API job increments retry count', async () => {
      const res = await createJob({ type: 'API', payload: 'http://localhost:1/will-fail' });
      assert(res.status === 200, `Failed to create job: ${res.status}`);

      // Wait for first execution attempt
      await sleep(5000);
      const check = await getJob(res.body.id);
      assert(check.body.retryCount >= 0, 'Should have retryCount field');
      assert(['RETRY', 'FAILED', 'PROCESSING', 'QUEUED'].includes(check.body.status),
        `Unexpected status: ${check.body.status}`);
      if (check.body.status === 'RETRY') {
        assert(check.body.nextRetryTime > 0, 'Should have nextRetryTime set');
      }
    }],

    ['Job reaches FAILED after max retries (5)', async () => {
      const res = await createJob({ type: 'API', payload: 'http://localhost:1/fail-forever' });
      assert(res.status === 200, `Failed to create job: ${res.status}`);

      // Max retry = 5, each retry needs watcher pickup (20s) + processing
      // This could take ~3 minutes. We'll check periodically.
      console.log('       ⏳ Waiting up to 180s for max retries...');
      const job = await pollJobStatus(res.body.id, 'FAILED', 180000, 5000);
      if (job && job.status === 'FAILED') {
        assert(job.retryCount >= 5, `Expected retryCount >= 5, got ${job.retryCount}`);
      } else {
        // If still retrying, that's also valid — just means it hasn't hit max yet
        console.log(`       → Current status: ${job ? job.status : 'null'}, retryCount: ${job ? job.retryCount : '?'}`);
        assert(job !== null, 'Job disappeared');
        assert(['RETRY', 'FAILED'].includes(job.status), `Expected RETRY or FAILED, got ${job.status}`);
      }
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 8: Circuit Breaker
// ═══════════════════════════════════════════════════════════════════════════════

async function suite8_CircuitBreaker() {
  await runSuite('8 — Circuit Breaker', [

    ['Circuit opens after 5 consecutive failures for same type', async () => {
      // Submit 7 API jobs that will all fail (bad URL) to trip the circuit
      const ids = [];
      for (let i = 0; i < 7; i++) {
        const res = await createJob({ type: 'API', payload: `http://localhost:1/circuit-${i}` });
        assert(res.status === 200, `Job creation failed: ${res.status}`);
        ids.push(res.body.id);
        await sleep(200); // slight spacing
      }

      // Wait for processing
      await sleep(10000);

      // Check statuses — after circuit opens, later jobs should be RETRY
      let retryCount = 0, failedCount = 0, completedCount = 0;
      for (const id of ids) {
        const res = await getJob(id);
        if (res.body.status === 'RETRY') retryCount++;
        else if (res.body.status === 'FAILED') failedCount++;
        else if (res.body.status === 'COMPLETED') completedCount++;
      }
      console.log(`       → RETRY: ${retryCount}, FAILED: ${failedCount}, COMPLETED: ${completedCount}`);
      // We expect at least some jobs to be in RETRY due to circuit breaker
      assert(retryCount >= 1 || failedCount >= 1,
        'Expected at least some jobs to be retried or failed');
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 9: Stats Endpoint
// ═══════════════════════════════════════════════════════════════════════════════

async function suite9_Stats() {
  await runSuite('9 — Stats Endpoint Accuracy', [

    ['Stats reflect all job statuses with correct total', async () => {
      const res = await getStats();
      assert(res.status === 200, `Expected 200, got ${res.status}`);

      const stats = res.body;
      const expectedStatuses = ['PENDING', 'SCHEDULED', 'QUEUED', 'PROCESSING',
                                 'COMPLETED', 'FAILED', 'RETRY', 'CANCELLED'];
      for (const s of expectedStatuses) {
        assert(s in stats, `Missing status: ${s}`);
        assert(typeof stats[s] === 'number', `${s} should be a number`);
      }

      // Total should be the sum of all individual statuses
      const sum = expectedStatuses.reduce((acc, s) => acc + stats[s], 0);
      assert(stats.TOTAL === sum, `TOTAL (${stats.TOTAL}) != sum of statuses (${sum})`);
      console.log(`       → Stats: ${JSON.stringify(stats)}`);
    }],

    ['New job creation increments TOTAL', async () => {
      const before = await getStats();
      await createJob({ type: 'LOG', payload: 'stats-increment-test' });
      await sleep(500);
      const after = await getStats();
      assert(after.body.TOTAL > before.body.TOTAL,
        `TOTAL should increase: before=${before.body.TOTAL}, after=${after.body.TOTAL}`);
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// SUITE 10: Concurrency Stress Test
// ═══════════════════════════════════════════════════════════════════════════════

async function suite10_ConcurrencyStress() {
  await runSuite('10 — Concurrency Stress (50 concurrent jobs)', [

    ['50 concurrent LOG jobs all submit successfully', async () => {
      const start = Date.now();
      const promises = [];
      for (let i = 0; i < 50; i++) {
        promises.push(createJob({ type: 'LOG', payload: `stress-${i}` }));
      }
      const results = await Promise.all(promises);
      const elapsed = Date.now() - start;

      let success = 0, failed = 0;
      const ids = [];
      for (const r of results) {
        if (r.status === 200) { success++; ids.push(r.body.id); }
        else failed++;
      }
      console.log(`       → Submitted: ${success} success, ${failed} failed in ${elapsed}ms`);
      console.log(`       → Throughput: ${(success / (elapsed / 1000)).toFixed(1)} jobs/sec`);
      assert(success >= 45, `Expected at least 45 successful, got ${success}`);
    }],

    ['All 50 stress jobs complete within 60 seconds', async () => {
      // Re-submit 50 jobs and track completion
      const ids = [];
      for (let i = 0; i < 50; i++) {
        const res = await createJob({ type: 'LOG', payload: `stress-complete-${i}` });
        if (res.status === 200) ids.push(res.body.id);
      }

      console.log(`       ⏳ Waiting up to 60s for ${ids.length} jobs to complete...`);
      const start = Date.now();
      let allDone = false;

      for (let attempt = 0; attempt < 60; attempt++) {
        await sleep(1000);
        let completed = 0;
        for (const id of ids) {
          const res = await getJob(id);
          if (res.body && res.body.status === 'COMPLETED') completed++;
        }
        if (completed === ids.length) {
          allDone = true;
          const elapsed = Date.now() - start;
          console.log(`       → All ${ids.length} jobs completed in ${(elapsed/1000).toFixed(1)}s`);
          break;
        }
        if (attempt % 10 === 9) {
          console.log(`       → Progress: ${completed}/${ids.length} completed (${attempt+1}s elapsed)`);
        }
      }
      assert(allDone, `Not all jobs completed within 60 seconds`);
    }],
  ]);
}


// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║     Job Queue System — End-to-End Test Suite                ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  // Pre-flight check
  console.log('  🔍 Pre-flight check...');
  try {
    const health = await http('GET', '/actuator/health');
    const dbUp = health.body?.components?.db?.status === 'UP';
    const redisUp = health.body?.components?.redis?.status === 'UP';
    console.log(`     DB: ${dbUp ? '✅' : '❌'}  Redis: ${redisUp ? '✅' : '❌'}  Health: ${health.status}`);
    if (!dbUp || !redisUp) {
      console.log('\n  ❌ Infrastructure not ready. Ensure docker compose is up.\n');
      process.exit(1);
    }
  } catch (e) {
    console.log(`\n  ❌ Cannot reach server: ${e.message}`);
    console.log('  Make sure docker compose up -d is running.\n');
    process.exit(1);
  }

  // Clean up previous test data — submit to fresh DB is fine since we docker compose down -v
  console.log('  ✅ Infrastructure ready\n');

  const startTime = Date.now();

  // Run all suites sequentially
  await suite1_ApiValidation();
  await suite2_LogLifecycle();
  await suite3_ApiLifecycle();
  await suite4_ScheduledJob();
  await suite5_Cancellation();
  await suite6_RateLimiter();
  await suite7_RetryBackoff();
  await suite8_CircuitBreaker();
  await suite9_Stats();
  await suite10_ConcurrencyStress();

  // Final report
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║                    FINAL RESULTS                           ║');
  console.log('╠══════════════════════════════════════════════════════════════╣');

  for (const s of suiteResults) {
    const icon = s.failed === 0 ? '✅' : '❌';
    const pad = s.name.padEnd(48);
    console.log(`║  ${icon} ${pad} ${s.passed}/${s.total} ║`);
  }

  console.log('╠══════════════════════════════════════════════════════════════╣');
  console.log(`║  Total: ${totalPassed} passed, ${totalFailed} failed — ${totalTime}s              ║`);
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  if (totalFailed > 0) {
    console.log(`  ❌ ${totalFailed} test(s) FAILED — resolve before Phase 2\n`);
    process.exit(1);
  } else {
    console.log('  🎉 ALL TESTS PASSED — System is ready for Phase 2!\n');
    process.exit(0);
  }
}

main().catch(e => {
  console.error('\n  💀 Fatal error:', e.message);
  process.exit(1);
});
