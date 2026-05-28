#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class ASTCreateQuery;

/// Runs the pre-flight checks required before `StorageFactory::get` can
/// materialise a REFLECTION. On `CREATE` mode all nine checks fire; on
/// `ATTACH` mode only the handful that catch staleness against the current
/// source table are run. Every failure throws with a ready-to-copy remediation
/// `SQL` example.
void validateAuxiliaryIndexPrerequisites(
    const ASTCreateQuery & create,
    ContextPtr context,
    LoadingStrictnessLevel mode);

}
