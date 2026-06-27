#include "detectors.hpp"

#include <regex>

namespace self_healer {

namespace {

std::string capture(const std::string& line, const std::regex& pattern, std::size_t index) {
    std::smatch match;
    if (std::regex_search(line, match, pattern) && match.size() > index) {
        return match[index].str();
    }
    return {};
}

std::string captureFirst(const std::string& line, const std::regex& pattern) {
    std::smatch match;
    if (!std::regex_search(line, match, pattern)) {
        return {};
    }
    for (std::size_t i = 1; i < match.size(); ++i) {
        if (match[i].matched) {
            return match[i].str();
        }
    }
    return {};
}

bool contains(const std::string& line, const std::string& token) {
    return line.find(token) != std::string::npos;
}

Incident baseIncident(IncidentType type, Severity severity, std::string title, const std::string& line) {
    Incident incident;
    incident.type = type;
    incident.severity = severity;
    incident.title = std::move(title);
    incident.evidence = line;
    return incident;
}

} // namespace

void ChainedLogDetector::setNext(std::shared_ptr<LogDetector> nextDetector) {
    next = std::move(nextDetector);
}

std::optional<Incident> ChainedLogDetector::detectNext(const std::string& line) const {
    if (!next) {
        return std::nullopt;
    }
    return next->detect(line);
}

std::optional<Incident> WatcherGapDetector::detect(const std::string& line) const {
    if (!contains(line, "Watcher gap detected")) {
        return detectNext(line);
    }

    return baseIncident(
        IncidentType::WatcherGap,
        Severity::High,
        "Watcher missed one or more scheduling windows",
        line
    );
}

std::optional<Incident> RateLimitDetector::detect(const std::string& line) const {
    if (!contains(line, "Rate limit hit") && !contains(line, "Rate limit exceeded")) {
        return detectNext(line);
    }

    Incident incident = baseIncident(
        IncidentType::RateLimitedJob,
        Severity::Medium,
        "Job processing is being throttled by the rate limiter",
        line
    );
    incident.jobId = capture(line, std::regex(R"(jobId=(\d+))"), 1);
    incident.jobType = capture(line, std::regex(R"(type=([A-Z]+))"), 1);
    return incident;
}

std::optional<Incident> CircuitOpenDetector::detect(const std::string& line) const {
    if (!contains(line, "Circuit OPEN")) {
        return detectNext(line);
    }

    Incident incident = baseIncident(
        IncidentType::CircuitOpen,
        Severity::Critical,
        "Circuit breaker is open for a job type",
        line
    );
    incident.jobId = capture(line, std::regex(R"(jobId=(\d+))"), 1);
    incident.jobType = capture(line, std::regex(R"(type=([A-Z]+))"), 1);
    return incident;
}

std::optional<Incident> JobFailureDetector::detect(const std::string& line) const {
    if (!contains(line, "Job failed jobId=")) {
        return detectNext(line);
    }

    Incident incident = baseIncident(
        IncidentType::JobExecutionFailed,
        Severity::High,
        "Job execution failed in the consumer",
        line
    );
    incident.jobId = capture(line, std::regex(R"(jobId=(\d+))"), 1);
    incident.attributes["reason"] = capture(line, std::regex(R"(reason=([^,]+))"), 1);
    return incident;
}

std::optional<Incident> RedisDispatchDetector::detect(const std::string& line) const {
    const bool missing = contains(line, "not found in DB, removing from Redis");
    const bool invalidState = contains(line, "Skipping job") && contains(line, "invalid state");
    const bool dispatchError = contains(line, "Error dispatching job");

    if (!missing && !invalidState && !dispatchError) {
        return detectNext(line);
    }

    Incident incident = baseIncident(
        IncidentType::RedisDispatchAnomaly,
        Severity::High,
        "Dispatcher could not safely move a Redis job to Kafka",
        line
    );
    incident.jobId = captureFirst(line, std::regex(R"(job\s+(\d+)|Job\s+(\d+))"));
    if (incident.jobId.empty()) {
        incident.jobId = capture(line, std::regex(R"(jobId=(\d+))"), 1);
    }
    return incident;
}

std::shared_ptr<LogDetector> DetectorFactory::createDefaultChain() {
    auto watcherGap = std::make_shared<WatcherGapDetector>();
    auto rateLimit = std::make_shared<RateLimitDetector>();
    auto circuitOpen = std::make_shared<CircuitOpenDetector>();
    auto jobFailure = std::make_shared<JobFailureDetector>();
    auto redisDispatch = std::make_shared<RedisDispatchDetector>();

    watcherGap->setNext(rateLimit);
    rateLimit->setNext(circuitOpen);
    circuitOpen->setNext(jobFailure);
    jobFailure->setNext(redisDispatch);

    return watcherGap;
}

} // namespace self_healer
