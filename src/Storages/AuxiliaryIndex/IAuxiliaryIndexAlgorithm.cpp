#include <Storages/AuxiliaryIndex/IAuxiliaryIndexAlgorithm.h>

#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/IDataPartStorage.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <fmt/format.h>


namespace DB
{

AlgorithmPartCompatibility IAuxiliaryIndexAlgorithm::checkPartCompatibility(const IDataPartStorage & storage) const
{
    const String expected_version = getAlgorithmVersion();
    const String expected_params_hash = getBuildParamsHash();
    if (expected_version.empty() || expected_params_hash.empty())
        return {};

    static constexpr auto fingerprint_file = "algorithm_private_fingerprint.json";
    if (!storage.existsFile(fingerprint_file))
        return {false, fmt::format("missing {}", fingerprint_file)};

    try
    {
        auto reader = storage.readFile(fingerprint_file, ReadSettings{}, std::nullopt);
        String body;
        readStringUntilEOF(body, *reader);

        Poco::JSON::Parser parser;
        const auto parsed = parser.parse(body);
        const auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
        if (!obj)
            return {false, fmt::format("{} is not a JSON object", fingerprint_file)};

        if (!obj->has("algorithm_version"))
            return {false, fmt::format("{} missing algorithm_version", fingerprint_file)};
        const auto actual_version = obj->getValue<std::string>("algorithm_version");
        if (actual_version != expected_version)
            return {false, fmt::format(
                "algorithm_version mismatch in {}: expected {}, got {}",
                fingerprint_file,
                expected_version,
                actual_version)};

        if (!obj->has("params_hash"))
            return {false, fmt::format("{} missing params_hash", fingerprint_file)};
        const auto actual_params_hash = obj->getValue<std::string>("params_hash");
        if (actual_params_hash != expected_params_hash)
            return {false, fmt::format(
                "params_hash mismatch in {}: expected {}, got {}",
                fingerprint_file,
                expected_params_hash,
                actual_params_hash)};
    }
    catch (...)
    {
        return {false, getCurrentExceptionMessage(false)};
    }

    return {};
}

}
