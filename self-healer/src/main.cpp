#include "detectors.hpp"
#include "healing_strategies.hpp"
#include "json_writer.hpp"

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

using namespace self_healer;

namespace {

/** Holds command-line settings for the self-healer process. */
struct AppConfig {
    std::string logFile;
    bool follow{false};
};

AppConfig parseArgs(int argc, char** argv) {
    AppConfig config;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--log-file" && i + 1 < argc) {
            config.logFile = argv[++i];
        } else if (arg == "--follow") {
            config.follow = true;
        } else if (arg == "--help") {
            std::cout << "Usage: self-healer [--log-file path] [--follow]\n";
            std::exit(0);
        }
    }
    return config;
}

void processLine(const std::string& line, const LogDetector& detector, const HealingPlanner& planner) {
    const auto incident = detector.detect(line);
    if (!incident.has_value()) {
        return;
    }

    const HealingPlan plan = planner.plan(*incident);
    std::cout << JsonWriter::write(*incident, plan) << '\n';
}

void runStream(std::istream& input, const LogDetector& detector, const HealingPlanner& planner) {
    std::string line;
    while (std::getline(input, line)) {
        processLine(line, detector, planner);
    }
}

int runFile(const AppConfig& config, const LogDetector& detector, const HealingPlanner& planner) {
    std::ifstream file(config.logFile);
    if (!file.is_open()) {
        std::cerr << "Unable to open log file: " << config.logFile << '\n';
        return 1;
    }

    std::string line;
    while (true) {
        while (std::getline(file, line)) {
            processLine(line, detector, planner);
        }

        if (!config.follow) {
            break;
        }

        file.clear();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    return 0;
}

} // namespace

int main(int argc, char** argv) {
    const AppConfig config = parseArgs(argc, argv);
    const auto detector = DetectorFactory::createDefaultChain();
    const HealingPlanner planner = StrategyFactory::createDefaultPlanner();

    if (config.logFile.empty()) {
        runStream(std::cin, *detector, planner);
        return 0;
    }

    return runFile(config, *detector, planner);
}
