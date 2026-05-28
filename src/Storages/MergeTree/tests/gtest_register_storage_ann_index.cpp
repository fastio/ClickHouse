#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

/// Forward declaration of the gate helper exposed at namespace scope by
/// `registerStorageANNIndex.cpp` for test-only access.
void checkANNIndexExperimentalGate(ContextPtr context, LoadingStrictnessLevel mode);

}


namespace
{

DB::ContextMutablePtr makeTestContext(bool gate_value)
{
    auto context = DB::Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_ann_index", gate_value);
    return context;
}

}


/// Default setting value (false) must reject CREATE-mode invocations even
/// when callers bypass `InterpreterCreateQuery`.
TEST(GatedFactoryTest, RejectsWithoutGate)
{
    auto context = makeTestContext(/*gate_value=*/false);

    try
    {
        DB::checkANNIndexExperimentalGate(context, DB::LoadingStrictnessLevel::CREATE);
        FAIL() << "expected SUPPORT_IS_DISABLED, no exception thrown";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::SUPPORT_IS_DISABLED);
    }
}


/// Setting flipped on -> gate becomes a no-op so the rest of the factory
/// callback can run.
TEST(GatedFactoryTest, AcceptsWithGate)
{
    auto context = makeTestContext(/*gate_value=*/true);

    EXPECT_NO_THROW(DB::checkANNIndexExperimentalGate(context, DB::LoadingStrictnessLevel::CREATE));
}


/// `ATTACH` and metadata-loading paths must not be gated, otherwise persisted
/// `ANNIndex` tables can prevent server startup after restart.
TEST(GatedFactoryTest, AttachModeBypassesGate)
{
    auto context = makeTestContext(/*gate_value=*/false);

    EXPECT_NO_THROW(DB::checkANNIndexExperimentalGate(context, DB::LoadingStrictnessLevel::ATTACH));
}
