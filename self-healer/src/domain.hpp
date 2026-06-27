#pragma once

#include <map>
#include <string>
#include <vector>

namespace self_healer {

/** Represents the operational category detected from an application log line. */
enum class IncidentType {
    WatcherGap,
    RateLimitedJob,
    CircuitOpen,
    JobExecutionFailed,
    RedisDispatchAnomaly,
    Unknown
};

/** Represents the risk level of a detected incident. */
enum class Severity {
    Low,
    Medium,
    High,
    Critical
};

/** Carries a normalized incident extracted from one or more log lines. */
struct Incident {
    IncidentType type{IncidentType::Unknown};
    Severity severity{Severity::Low};
    std::string title;
    std::string jobId;
    std::string jobType;
    std::string evidence;
    std::map<std::string, std::string> attributes;
};

/** Describes a safe healing decision for a detected incident. */
struct HealingPlan {
    std::string strategyName;
    std::string action;
    bool dryRun{true};
    std::vector<std::string> steps;
    std::vector<std::string> verificationChecks;
};

/** Converts an incident type to a stable machine-readable string. */
inline std::string toString(IncidentType type) {
    switch (type) {
        case IncidentType::WatcherGap: return "WATCHER_GAP";
        case IncidentType::RateLimitedJob: return "RATE_LIMITED_JOB";
        case IncidentType::CircuitOpen: return "CIRCUIT_OPEN";
        case IncidentType::JobExecutionFailed: return "JOB_EXECUTION_FAILED";
        case IncidentType::RedisDispatchAnomaly: return "REDIS_DISPATCH_ANOMALY";
        default: return "UNKNOWN";
    }
}

/** Converts a severity to a stable machine-readable string. */
inline std::string toString(Severity severity) {
    switch (severity) {
        case Severity::Low: return "LOW";
        case Severity::Medium: return "MEDIUM";
        case Severity::High: return "HIGH";
        case Severity::Critical: return "CRITICAL";
        default: return "LOW";
    }
}

} // namespace self_healer
