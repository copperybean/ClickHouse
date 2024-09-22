#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/Serializations/ISerialization.h>

#include "ParquetColumnReader.h"
#include "ParquetDataValuesReader.h"

namespace parquet
{

class ColumnDescriptor;

}


namespace DB
{

template <typename TColumn>
class ParquetLeafColReader : public ParquetColumnReader
{
public:
    ParquetLeafColReader(
        const parquet::ColumnDescriptor & col_descriptor_,
        DataTypePtr base_type_,
        std::unique_ptr<parquet::ColumnChunkMetaData> meta_,
        std::unique_ptr<parquet::PageReader> reader_,
        std::vector<Int16> lists_def_levels_);

    RepeatedColumn readBatch(UInt64 rows_num, const String & name) override;

private:
    const parquet::ColumnDescriptor & col_descriptor;
    DataTypePtr base_data_type; // TODO ch_data_type
    std::unique_ptr<parquet::ColumnChunkMetaData> col_chunk_meta;
    std::unique_ptr<parquet::PageReader> parquet_page_reader;
    std::unique_ptr<ParquetDataValuesReader> data_values_reader;
    std::unique_ptr<RleValuesReader> rep_level_reader;

    MutableColumnPtr column;
    std::unique_ptr<LazyNullMap> null_map;
    std::vector<Int16> lists_def_levels;
    std::vector<Offsets> lists_offsets;
    std::vector<UInt32> lists_offsets_update_indice;

    ColumnPtr dictionary;

    UInt64 reading_rows_num = 0;
    UInt32 cur_page_values = 0;
    bool reading_low_cardinality = false;

    Poco::Logger * log;

    void resetColumn(UInt64 rows_num);
    void degradeDictionary();
    ColumnWithTypeAndName releaseColumn(const String & name);

    void readPage();
    void readPageV1(const parquet::DataPageV1 & page);
    void readPageV2(const parquet::DataPageV2 & page);
    
    /// The first value in returned pair is the rows number of root column actually read,
    /// while the second value is the correspongding values number of leaf column
    std::pair<UInt32, UInt32> readRepeatedOffsets(UInt64 rows_num);

    void updateRepeatedOffsets(const PaddedPODArray<RangedShortElem> & ranged_def_levels);

    std::unique_ptr<ParquetDataValuesReader> createDictReader(
        std::unique_ptr<RleValuesReader> def_level_reader, std::unique_ptr<RleValuesReader> rle_data_reader);
};

}
