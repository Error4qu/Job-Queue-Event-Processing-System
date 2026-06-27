#pragma once

#include "domain.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace self_healer {

/** Defines a log detector that can extract an incident or delegate to the next detector. */
class LogDetector {
public:
    virtual ~LogDetector() = default;

    /** Attempts to detect an incident from a single log line. */
    virtual std::optional<Incident> detect(const std::string& line) const = 0;
};

/** Implements chain-of-responsibility behavior for log detectors. */
class ChainedLogDetector : public LogDetector {
public:
    /** Adds the next detector in the chain. */
    void setNext(std::shared_ptr<LogDetector> nextDetector);

protected:
    /** Delegates detection to the next detector when this one does not match. */
    std::optional<Incident> detectNext(const std::string& line) const;

private:
    std::shared_ptr<LogDetector> next;
};

/** Detects WatcherService gap recovery events. */
class WatcherGapDetector final : public ChainedLogDetector {
public:
    std::optional<Incident> detect(const std::string& line) const override;
};

/** Detects Redis-backed rate limiter pressure. */
class RateLimitDetector final : public ChainedLogDetector {
public:
    std::optional<Incident> detect(const std::string& line) const override;
};

/** Detects open circuit breaker events. */
class CircuitOpenDetector final : public ChainedLogDetector {
public:
    std::optional<Incident> detect(const std::string& line) const override;
};

/** Detects job execution failures emitted by the Kafka consumer. */
class JobFailureDetector final : public ChainedLogDetector {
public:
    std::optional<Incident> detect(const std::string& line) const override;
};

/** Detects dispatcher anomalies while moving jobs from Redis to Kafka. */
class RedisDispatchDetector final : public ChainedLogDetector {
public:
    std::optional<Incident> detect(const std::string& line) const override;
};

/** Factory that wires the detector chain in priority order. */
class DetectorFactory {
public:
    /** Builds the default detector chain for the job queue logs. */
    static std::shared_ptr<LogDetector> createDefaultChain();
};

} // namespace self_healer
