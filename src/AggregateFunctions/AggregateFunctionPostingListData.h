#include <Common/RadixSort.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/PostingsContainer.h>

#pragma clang optimize off
#if defined(__clang__)
#  pragma clang diagnostic push
#  if __has_warning("-Wunused-member-function")
#    pragma clang diagnostic ignored "-Wunused-member-function"
#  endif
#endif
namespace DB {
struct Settings;

namespace
{
struct PostingListDataSorted
{
    using PostingsContainerType = PostingsContainer32;
    using PostingListDataType = PostingsContainerType::ValueType;

    PostingsContainer64 container;

    ALWAYS_INLINE void add(PostingListDataType && element, Arena *)
    {
        container.add(element);
    }

    ALWAYS_INLINE void merge(const PostingListDataSorted & rhs, Arena *)
    {
        PostingsContainer64 out_container;
        mergePostingsContainers(out_container, container, rhs.container);
        container = std::move(out_container);
    }
    ALWAYS_INLINE void insertResultInto(IColumn & to)
    {
        auto & result_array = assert_cast<ColumnArray &>(to);
        auto & result_array_offsets = result_array.getOffsets();
        result_array_offsets.push_back(result_array_offsets.back() + container.size());

        if (container.empty())
            return;

        auto & result_array_data = assert_cast<ColumnVector<PostingListDataType> &>(result_array.getData()).getData();

        size_t result_array_data_insert_begin = result_array_data.size();
        result_array_data.resize(result_array_data_insert_begin + container.size());

        size_t idx = 0;
        for (auto it = container.begin(); it != container.end(); ++it, idx++)
        {
            result_array_data[result_array_data_insert_begin + idx] = *it;
        }
    }

    void serialize(WriteBuffer &, std::optional<size_t> version) const
    {
        chassert(version);
    }

    void deserialize(ReadBuffer &, std::optional<size_t> version, Arena *)
    {
        chassert(version);
    }
};

using PostingListData = PostingListDataSorted;

}
}
