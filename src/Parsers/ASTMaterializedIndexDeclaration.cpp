#include <Parsers/ASTMaterializedIndexDeclaration.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

ASTPtr ASTMaterializedIndexDeclaration::clone() const
{
    auto res = make_intrusive<ASTMaterializedIndexDeclaration>();
    res->family = family;
    res->impl = impl;

    if (auto params = getBuildParams())
        res->children.push_back(params->clone());

    return res;
}

ASTPtr ASTMaterializedIndexDeclaration::getBuildParams() const
{
    if (children.size() <= build_params_idx)
        return nullptr;
    return children[build_params_idx];
}

void ASTMaterializedIndexDeclaration::formatImpl(
    WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    ostr << family << "(" << quoteString(impl);

    if (auto params = getBuildParams())
    {
        if (auto * list = params->as<ASTExpressionList>(); list && !list->children.empty())
        {
            ostr << ", ";
            params->format(ostr, s, state, frame);
        }
    }

    ostr << ")";
}

}
