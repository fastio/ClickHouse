#pragma once

namespace knowhere
{

struct DummyObserver
{
    void Observe(double) {}
};

inline DummyObserver knowhere_diskann_bitset_ratio;

}
