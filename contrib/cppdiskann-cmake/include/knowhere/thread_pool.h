#pragma once

#include <algorithm>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace knowhere
{

class ThreadPool
{
public:
    static std::shared_ptr<ThreadPool> GetGlobalBuildThreadPool()
    {
        static auto pool = std::make_shared<ThreadPool>();
        return pool;
    }

    static std::shared_ptr<ThreadPool> GetGlobalSearchThreadPool()
    {
        static auto pool = std::make_shared<ThreadPool>();
        return pool;
    }

    static size_t GetGlobalBuildThreadPoolSize()
    {
        return std::max<size_t>(1, std::thread::hardware_concurrency());
    }

    static ThreadPool CreateFIFO(size_t, const std::string &)
    {
        return ThreadPool{};
    }

    size_t size() const
    {
        return std::max<size_t>(1, std::thread::hardware_concurrency());
    }

    template <typename Func>
    auto push(Func && func)
    {
        using Result = std::invoke_result_t<Func>;
        if constexpr (std::is_void_v<Result>)
        {
            return std::async(
                std::launch::async,
                [fn = std::forward<Func>(func)]() mutable
                {
                    fn();
                    return folly::Unit{};
                });
        }
        else
        {
            return std::async(std::launch::async, std::forward<Func>(func));
        }
    }
};

inline size_t GetGlobalBuildThreadPoolSize()
{
    return std::max<size_t>(1, std::thread::hardware_concurrency());
}

template <typename Future>
void WaitAllSuccess(std::vector<Future> & futures)
{
    for (auto & future : futures)
        future.get();
}

}
