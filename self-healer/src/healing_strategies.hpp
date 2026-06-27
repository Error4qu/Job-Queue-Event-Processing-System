#pragma once

#include "domain.hpp"

#include <memory>
#include <vector>

namespace self_healer {

/** Selects a safe repair plan for a matching incident. */
class HealingStrategy {
public:
    virtual ~HealingStrategy() = default;

    /** Returns true when this strategy can handle the incident. */
    virtual bool supports(const Incident& incident) const = 0;

    /** Builds a dry-run healing plan for the incident. */
    virtual HealingPlan createPlan(const Incident& incident) const = 0;
};

/** Plans recovery for missed WatcherService scheduling windows. */
class WatcherRecoveryStrategy final : public HealingStrategy {
public:
    bool supports(const Incident& incident) const override;
    HealingPlan createPlan(const Incident& incident) const override;
};

/** Plans safe handling for jobs delayed by rate limiting. */
class RateLimitBackoffStrategy final : public HealingStrategy {
public:
    bool supports(const Incident& incident) const override;
    HealingPlan createPlan(const Incident& incident) const override;
};

/** Plans investigation for open circuit breaker events. */
class CircuitBreakerStrategy final : public HealingStrategy {
public:
    bool supports(const Incident& incident) const override;
    HealingPlan createPlan(const Incident& incident) const override;
};

/** Plans retry-state verification for failed jobs. */
class JobFailureStrategy final : public HealingStrategy {
public:
    bool supports(const Incident& incident) const override;
    HealingPlan createPlan(const Incident& incident) const override;
};

/** Plans Redis delay queue reconciliation for dispatcher anomalies. */
class RedisReconciliationStrategy final : public HealingStrategy {
public:
    bool supports(const Incident& incident) const override;
    HealingPlan createPlan(const Incident& incident) const override;
};

/** Coordinates strategy selection for incidents. */
class HealingPlanner {
public:
    explicit HealingPlanner(std::vector<std::shared_ptr<HealingStrategy>> strategies);

    /** Selects the first matching strategy and returns its plan. */
    HealingPlan plan(const Incident& incident) const;

private:
    std::vector<std::shared_ptr<HealingStrategy>> strategies;
};

/** Factory that creates the default ordered strategy list. */
class StrategyFactory {
public:
    /** Builds the default planner for job queue incidents. */
    static HealingPlanner createDefaultPlanner();
};

} // namespace self_healer
