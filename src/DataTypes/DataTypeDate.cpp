#include <DataTypes/DataTypeDate.h>
#include <DataTypes/Serializations/SerializationDate.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

bool DataTypeDate::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeDate::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationDate>();
}

void registerDataTypeDate(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("Date", [] { return DataTypePtr(std::make_shared<DataTypeDate>()); }, DataTypeFactory::Case::Insensitive);
}

}
