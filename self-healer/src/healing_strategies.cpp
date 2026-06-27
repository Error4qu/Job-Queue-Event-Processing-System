#include "healing_strategies.hpp"

#include <utility>

namespace self_healer {

namespace {

HealingPlan makePlan(std::string name, std::string action) {
    HealingPlan plan;
    plan.strategyName = std::move(name);
    plan.action = std::move(action);
    plan.dryRun = true;
    return plan;
}

} // namespace

bool WatcherRecoveryStrategy::supports(const Incident& incident) const {
    return incident.type == IncidentType::WatcherGap;
}

HealingPlan WatcherRecoveryStrategy::createPlan(const Incident&) const {
    HealingPlan plan = makePlan("WatcherRecoveryStrategy", "RECOVER_MISSED_SCHEDULE_WINDOWS");
    plan.steps = {
        "Query jobs with PENDING or SCHEDULED status whose scheduleTime is in the missed window.",
        "Reinsert missing jobs into the Redis delay queue using their scheduleTime.",
        "Query RETRY jobs whose nextRetryTime is in the missed window and reinsert them into Redis.",
        "Keep all database status transitions idempotent."
    };
    plan.verificationChecks = {
        "Redis sorted set contains recovered job IDs.",
        "No recovered job is already COMPLETED, FAILED, or CANCELLED.",
        "Dispatcher emits Kafka publish logs for due recovered jobs."
    };
    return plan;
}

bool RateLimitBackoffStrategy::supports(const Incident& incident) const {
    return incident.type == IncidentType::RateLimitedJob;
}

HealingPlan RateLimitBackoffStrategy::createPlan(const Incident&) const {
    HealingPlan plan = makePlan("RateLimitBackoffStrategy", "WAIT_FOR_RETRY_WINDOW");
    plan.steps = {
        "Confirm the job is in RETRY status with nextRetryTime set.",
        "Avoid immediate republish because rate limiting is an intentional safety control.",
        "Let WatcherService requeue the job when nextRetryTime becomes due."
    };
    plan.verificationChecks = {
        "Retry job moves from RETRY to SCHEDULED.",
        "Job eventually moves to COMPLETED or FAILED.",
        "Rate limiter warning volume drops below threshold."
    };
    return plan;
}

bool CircuitBreakerStrategy::supports(const Incident& incident) const {
    return incident.type == IncidentType::CircuitOpen;
}

HealingPlan CircuitBreakerStrategy::createPlan(const Incident&) const {
    HealingPlan plan = makePlan("CircuitBreakerStrategy", "PAUSE_AND_ESCALATE_JOB_TYPE");
    plan.steps = {
        "Do not force execution while the circuit is open.",
        "Group recent failures by job type and executor.",
        "Check downstream dependency health for the affected job type.",
        "Allow half-open recovery after the configured timeout."
    };
    plan.verificationChecks = {
        "Circuit transitions to half-open after timeout.",
        "A successful execution resets failure count.",
        "No duplicate job execution is observed."
    };
    return plan;
}

bool JobFailureStrategy::supports(const Incident& incident) const {
    return incident.type == IncidentType::JobExecutionFailed;
}

HealingPlan JobFailureStrategy::createPlan(const Incident&) const {
    HealingPlan plan = makePlan("JobFailureStrategy", "VERIFY_RETRY_OR_FINAL_STATE");
    plan.steps = {
        "Fetch the failed job from MySQL by jobId.",
        "If retryCount is below max retry, confirm status is RETRY and nextRetryTime is set.",
        "If retryCount reached the limit, confirm status is FAILED.",
        "Do not replay EMAIL jobs automatically without idempotency confirmation."
    };
    plan.verificationChecks = {
        "Job status is RETRY or FAILED.",
        "Retry jobs are later picked up by WatcherService.",
        "Failure reason is captured for operator review."
    };
    return plan;
}

bool RedisReconciliationStrategy::supports(const Incident& incident) const {
    return incident.type == IncidentType::RedisDispatchAnomaly;
}

HealingPlan RedisReconciliationStrategy::createPlan(const Incident&) const {
    HealingPlan plan = makePlan("RedisReconciliationStrategy", "RECONCILE_REDIS_DELAY_QUEUE");
    plan.steps = {
        "Fetch job state from MySQL.",
        "If job is SCHEDULED and still due, reinsert it into the Redis sorted set.",
        "If job is terminal, keep it removed from Redis.",
        "If job is QUEUED, verify Kafka publish before considering republish."
    };
    plan.verificationChecks = {
        "Redis contains only non-terminal scheduled jobs.",
        "Kafka receives due queued jobs once.",
        "Database state remains the source of truth."
    };
    return plan;
}

HealingPlanner::HealingPlanner(std::vector<std::shared_ptr<HealingStrategy>> strategies)
    : strategies(std::move(strategies)) {}

HealingPlan HealingPlanner::plan(const Incident& incident) const {
    for (const auto& strategy : strategies) {
        if (strategy->supports(incident)) {
            return strategy->createPlan(incident);
        }
    }

    HealingPlan fallback = makePlan("FallbackStrategy", "ALERT_OPERATOR");
    fallback.steps = {"Capture incident evidence and request manual review."};
    fallback.verificationChecks = {"Operator acknowledges the incident."};
    return fallback;
}

HealingPlanner StrategyFactory::createDefaultPlanner() {
    return HealingPlanner({
        std::make_shared<WatcherRecoveryStrategy>(),
        std::make_shared<RateLimitBackoffStrategy>(),
        std::make_shared<CircuitBreakerStrategy>(),
        std::make_shared<JobFailureStrategy>(),
        std::make_shared<RedisReconciliationStrategy>()
    });
}

} // namespace self_healer
