#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** `TYPE <family>('<impl>'[, <build_params>...])` clause of a materialized
  * index declaration, e.g. `TYPE ann('MockAnn', M = 16, ef = 200)`.
  *
  * `family` selects the algorithm family (routed via a dedicated registry);
  * `impl` picks a concrete implementation inside that family; build parameters
  * are an arbitrary expression list (positional or `name = value` pairs).
  */
class ASTMaterializedIndexDeclaration : public IAST
{
public:
    String family;
    String impl;

    String getID(char) const override { return "MaterializedIndexType"; }
    ASTPtr clone() const override;

    ASTPtr getBuildParams() const;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

private:
    static constexpr size_t build_params_idx = 0;
};

}
