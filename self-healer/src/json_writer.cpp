#include "json_writer.hpp"

#include <sstream>

namespace self_healer {

namespace {

std::string escapeJson(const std::string& value) {
    std::ostringstream escaped;
    for (char ch : value) {
        switch (ch) {
            case '\\': escaped << "\\\\"; break;
            case '"': escaped << "\\\""; break;
            case '\n': escaped << "\\n"; break;
            case '\r': escaped << "\\r"; break;
            case '\t': escaped << "\\t"; break;
            default: escaped << ch; break;
        }
    }
    return escaped.str();
}

void writeStringArray(std::ostringstream& out, const std::vector<std::string>& values) {
    out << '[';
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << ',';
        }
        out << '"' << escapeJson(values[i]) << '"';
    }
    out << ']';
}

} // namespace

std::string JsonWriter::write(const Incident& incident, const HealingPlan& plan) {
    std::ostringstream out;
    out << '{';
    out << "\"incident\":{";
    out << "\"type\":\"" << toString(incident.type) << "\",";
    out << "\"severity\":\"" << toString(incident.severity) << "\",";
    out << "\"title\":\"" << escapeJson(incident.title) << "\",";
    out << "\"jobId\":\"" << escapeJson(incident.jobId) << "\",";
    out << "\"jobType\":\"" << escapeJson(incident.jobType) << "\",";
    out << "\"evidence\":\"" << escapeJson(incident.evidence) << "\"";
    out << "},";
    out << "\"plan\":{";
    out << "\"strategy\":\"" << escapeJson(plan.strategyName) << "\",";
    out << "\"action\":\"" << escapeJson(plan.action) << "\",";
    out << "\"dryRun\":" << (plan.dryRun ? "true" : "false") << ',';
    out << "\"steps\":";
    writeStringArray(out, plan.steps);
    out << ',';
    out << "\"verificationChecks\":";
    writeStringArray(out, plan.verificationChecks);
    out << '}';
    out << '}';
    return out.str();
}

} // namespace self_healer
