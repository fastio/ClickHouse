#include <Interpreters/ReflectionJobLog.h>

namespace DB
{

ColumnsDescription ReflectionJobLogElement::getColumnsDescription()
{
    return ReflectionJob::getLogColumnsDescription();
}

NamesAndAliases ReflectionJobLogElement::getNamesAndAliases()
{
    return ReflectionJob::getCommonAliasesForLog();
}

void ReflectionJobLogElement::appendToBlock(MutableColumns & columns) const
{
    ReflectionJob::appendLogRow(columns, row);
}

}
