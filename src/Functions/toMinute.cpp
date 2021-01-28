#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToMinute = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMinuteImpl>;

void registerFunctionToMinute(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToMinute>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToMinute>("MINUTE", FunctionFactory::CaseInsensitive);
}

}


