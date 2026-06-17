#pragma once

class MallocExtension
{
public:
    static MallocExtension * instance()
    {
        static MallocExtension extension;
        return &extension;
    }

    void ReleaseFreeMemory() {}
};
