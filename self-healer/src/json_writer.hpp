#pragma once

#include "domain.hpp"

#include <string>

namespace self_healer {

/** Serializes incidents and healing plans as compact JSON lines. */
class JsonWriter {
public:
    /** Returns one JSON object containing the incident and selected plan. */
    static std::string write(const Incident& incident, const HealingPlan& plan);
};

} // namespace self_healer
