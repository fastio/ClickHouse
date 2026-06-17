#pragma once

#include <cstdint>
#include <memory>
#include <unordered_set>

namespace knowhere::feder::diskann
{

struct FederVisitInfo
{
    void AddTopCandidateInfo(uint64_t, float) {}
    void AddTopCandidateNeighbor(uint64_t, uint64_t, float) {}
};

struct FederResult
{
    FederVisitInfo visit_info_;
    std::unordered_set<uint64_t> id_set_;
};

using FederResultUniq = std::shared_ptr<FederResult>;

}
