#include <boost/bind.hpp>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnNested.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/WriteBufferFromString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/DataTypes/DataTypeNested.h>

#include <Poco/StringTokenizer.h>


namespace DB
{


DataTypeNested::DataTypeNested(NamesAndTypesListPtr nested_) : nested(nested_)
{
	offsets = new DataTypeFromFieldType<ColumnArray::Offset_t>::Type;
}


std::string DataTypeNested::concatenateNestedName(const std::string & nested_table_name, const std::string & nested_field_name)
{
	return nested_table_name + "." + nested_field_name;
}


std::pair<std::string, std::string> DataTypeNested::splitNestedName(const std::string & nested_name)
{
	Poco::StringTokenizer tokenizer(nested_name, ".");
	return std::make_pair(tokenizer[0], tokenizer[1]);
}


bool DataTypeNested::isNestedName(const std::string & nested_name)
{
	Poco::StringTokenizer tokenizer(nested_name, ".");
	return tokenizer.count() == 2 && tokenizer[0] != "" && tokenizer[1] != "";
}


std::string DataTypeNested::extractNestedTableName(const std::string & nested_name)
{
	Poco::StringTokenizer tokenizer(nested_name, ".");
	return tokenizer[0];
}


std::string DataTypeNested::getName() const
{
	std::string res;
	WriteBufferFromString out(res);	
	
	writeCString("Nested(", out);
	
	for (NamesAndTypesList::const_iterator it = nested->begin(); it != nested->end(); ++it)
	{
		if (it != nested->begin())
			writeCString(", ", out);
		writeString(it->first, out);
		writeChar(' ', out);
		writeString(it->second->getName(), out);
	}
	
	writeChar(')', out);
	
	return res;
}

	
void DataTypeNested::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Method serializeBinary(const Field &, WriteBuffer &) is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeNested::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	throw Exception("Method deserializeBinary(Field &, ReadBuffer &) is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeNested::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnNested & column_nested = dynamic_cast<const ColumnNested &>(column);
	const ColumnNested::Offsets_t & offsets = column_nested.getOffsets();

	size_t nested_offset = offset ? offsets[offset - 1] : 0;
	size_t nested_limit = limit && (offset + limit < offsets.size())
		? offsets[offset + limit - 1] - nested_offset
		: 0;

	NamesAndTypesList::const_iterator it = nested->begin();
	for (size_t i = 0; i < nested->size(); ++i, ++it)
		it->second->serializeBinary(*column_nested.getData()[i], ostr, nested_offset, nested_limit);
}


void DataTypeNested::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnNested & column_nested = dynamic_cast<ColumnNested &>(column);
	ColumnNested::Offsets_t & offsets = column_nested.getOffsets();

	/// Должно быть считано согласованное с offsets количество значений.
	size_t nested_limit = offsets.empty() ? 0 : offsets.back();
	
	NamesAndTypesList::const_iterator it = nested->begin();
	for (size_t i = 0; i < nested->size(); ++i, ++it)
	{
		it->second->deserializeBinary(*column_nested.getData()[i], istr, nested_limit);
		if (column_nested.getData()[i]->size() != nested_limit)
			throw Exception("Cannot read all nested column values", ErrorCodes::CANNOT_READ_ALL_DATA);
	}
}


void DataTypeNested::serializeOffsets(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnNested & column_nested = dynamic_cast<const ColumnNested &>(column);
	const ColumnNested::Offsets_t & offsets = column_nested.getOffsets();
	size_t size = offsets.size();

	if (!size)
		return;

	size_t end = limit && (offset + limit < size)
		? offset + limit
		: size;

	if (offset == 0)
	{
		writeIntBinary(offsets[0], ostr);
		++offset;
	}

	for (size_t i = offset; i < end; ++i)
		writeIntBinary(offsets[i] - offsets[i - 1], ostr);
}


void DataTypeNested::deserializeOffsets(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnNested & column_nested = dynamic_cast<ColumnNested &>(column);
	ColumnNested::Offsets_t & offsets = column_nested.getOffsets();
	offsets.resize(limit);

	size_t i = 0;
	ColumnNested::Offset_t current_offset = 0;
	while (i < limit && !istr.eof())
	{
		ColumnNested::Offset_t current_size = 0;
		readIntBinary(current_size, istr);
		current_offset += current_size;
		offsets[i] = current_offset;
		++i;
	}

	offsets.resize(i);
}


void DataTypeNested::serializeText(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Method get is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeNested::deserializeText(Field & field, ReadBuffer & istr) const
{
	throw Exception("Method get is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeNested::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	serializeText(field, ostr);
}


void DataTypeNested::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	deserializeText(field, istr);
}


void DataTypeNested::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	serializeText(field, ostr);
}


void DataTypeNested::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	deserializeText(field, istr);
}


void DataTypeNested::serializeTextJSON(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Method get is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


ColumnPtr DataTypeNested::createColumn() const
{
	Columns columns;
	columns.reserve(nested->size());
	for (NamesAndTypesList::const_iterator it = nested->begin(); it != nested->end(); ++it)
		columns.push_back(it->second->createColumn());
	return new ColumnNested(columns);
}


ColumnPtr DataTypeNested::createConstColumn(size_t size, const Field & field) const
{
	throw Exception("Method createConstColumn is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

}
