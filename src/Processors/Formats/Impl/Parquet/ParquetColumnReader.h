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
    Columns offsets_layers;
}

class ParquetColumnReader
{
public:
    virtual RepeatedColumn readBatch(UInt64 rows_num, const String & name) = 0;

    virtual ~ParquetColumnReader() = default;
};

using ParquetColReaderPtr = std::unique_ptr<ParquetColumnReader>;
using ParquetColReaders = std::vector<ParquetColReaderPtr>;

}
