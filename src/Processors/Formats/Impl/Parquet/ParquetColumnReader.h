#pragma once

#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>

namespace parquet
{

class PageReader;
class ColumnChunkMetaData;
class DataPageV1;
class DataPageV2;

}

namespace DB
{

struct RepeatedColumn
{
    ColumnWithTypeAndName detailed_column;
    Columns lists_offsets;
}

class ParquetColumnReader
{
public:
    virtual RepeatedColumn readBatch(UInt64 rows_num, const String & name) = 0;

    virtual ~ParquetColumnReader() = default;
};

using ParquetColReaderPtr = std::unique_ptr<ParquetColumnReader>;
using ParquetColReaders = std::vector<ParquetColReaderPtr>;


class ParquetTupleColReader : public ParquetColumnReader
{
public:
    ParquetTupleColReader(
        DataTypeTuplePtr ch_tuple_type_,
        ParquetColReaders sub_column_readers_)
        : ch_tuple_type(ch_tuple_type_)
        , sub_column_readers(std::move(sub_column_readers_))
    {
        assert(sub_column_readers.size() == ch_tuple_type->getElementNames().size());
    }

    RepeatedColumn readBatch(UInt64 rows_num, const String & name) override
    {
        assert(!sub_column_readers.empty());
        MutableColumns cols(sub_column_readers.size());
        auto readOne = [&](size_t idx)
        {
            auto sub_type = ch_tuple_type->getElementNames()[idx];
            auto rep_col = sub_column_readers[idx]->readBatch(rows_num, sub_type);
            cols[idx] = castColumn(rep_col.detailed_column, ch_tuple_type->getElement(idx))->assumeMutable();
            return rep_col.lists_offsets;
        };

        auto lists_offsets = readOne(0);
        for (size_t i = 1; i < sub_column_readers.size(); i++)
        {
            readOne(i);
        }
        return {{ColumnTuple::create(std::move(cols)), ch_tuple_type, name}, std::move(lists_offsets)};
    }

private:
    DataTypeTuplePtr ch_tuple_type;
    ParquetColReaders sub_column_readers;
};

class ParquetArrayColReader : public ParquetColumnReader
{
public:
    ParquetArrayColReader(
        DataTypeArrayPtr ch_array_type_,
        ParquetColReaderPtr nested_column_reader_)
        : ch_tuple_type(ch_tuple_type_)
        , nested_column_reader(std::move(nested_column_reader_))
    {}

    RepeatedColumn readBatch(UInt64 rows_num, const String & name) override
    {
        auto rep_column = nested_column_reader->readBatch(rows_num, ch_array_type->getNestedType());
        auto col_array = ColumnArray::create(rep_column.detailed_column, rep_column.lists_offsets.back());
        rep_column.lists_offsets.pop_back();
        return {col_array, std::move(rep_column.lists_offsets)};
    }

private:
    DataTypeArrayPtr ch_array_type;
    ParquetColReaderPtr nested_column_reader;
};

}
