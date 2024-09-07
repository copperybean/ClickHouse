#include "ParquetRecordReader.h"

#include <bit>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>

#include <arrow/status.h>
#include <boost/algorithm/string.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/column_reader.h>
#include <parquet/properties.h>

#include "ParquetLeafColReader.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int PARQUET_EXCEPTION;
}

#define THROW_PARQUET_EXCEPTION(s)                                            \
    do                                                                        \
    {                                                                         \
        try { (s); }                                                          \
        catch (const ::parquet::ParquetException & e)                         \
        {                                                                     \
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Parquet exception: {}", e.what());   \
        }                                                                     \
    } while (false)

namespace
{

using DataTypeTuplePtr = std::shared_ptr<const DataTypeTuple>;

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

    ColumnWithTypeAndName readBatch(UInt64 rows_num, const String & name) override
    {
        auto col = ColumnTuple::create(sub_column_readers.size());
        for (size_t i = 0; i < sub_column_readers.size(); i++)
        {
            col->getColumnPtr(i) = sub_column_readers[i]->readBatch(rows_num, ch_tuple_type->getElementNames()[i]).column;
        }
        return {std::move(col), ch_tuple_type, name};
    }

private:
    DataTypeTuplePtr ch_tuple_type;
    ParquetColReaders sub_column_readers;
};






std::unique_ptr<parquet::ParquetFileReader> createFileReader(
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
    parquet::ReaderProperties reader_properties,
    std::shared_ptr<parquet::FileMetaData> metadata = nullptr)
{
    std::unique_ptr<parquet::ParquetFileReader> res;
    THROW_PARQUET_EXCEPTION(res = parquet::ParquetFileReader::Open(
            std::move(arrow_file),
            reader_properties,
            metadata));
    return res;
}

class TupleColReaderFactory : public IParquetColReaderFactory
{
public:
    TupleColReaderFactory(
        ParquetColReaderFactories reader_factories_,
        DataTypeTuplePtr ch_tuple_type_)
        : reader_factories(std::move(reader_factories_))
        , ch_tuple_type(ch_tuple_type_)
        {
            assert(reader_factories.size() == ch_tuple_type->getElements().size());
        }

    std::unique_ptr<ParquetColumnReader> makeReader(parquet::RowGroupReader & row_group_reader) override
    {
        ParquetColReaders readers;
        readers.reserve(reader_factories.size());
        for (size_t i = 0; i < reader_factories.size(); i++)
        {
            readers.emplace_back(reader_factories[i]->makeReader(row_group_reader));
        }
        return std::make_unique<ParquetTupleColReader>(ch_tuple_type, std::move(readers));
    }

private:

    ParquetColReaderFactories reader_factories;
    DataTypeTuplePtr ch_tuple_type;
};

class LeafColReaderFactory : public IParquetColReaderFactory
{
public:
    LeafColReaderFactory(
        int col_index_,
        const parquet::ArrowReaderProperties & arrow_properties_,
        const parquet::ColumnDescriptor & col_descriptor_,
        DataTypePtr ch_type_)
        : col_index(col_index_)
        , arrow_properties(arrow_properties_)
        , col_descriptor(col_descriptor_)
        , ch_type(std::move(ch_type_)) {}

    std::unique_ptr<ParquetColumnReader> makeReader(parquet::RowGroupReader & row_group_reader) override;

private:
    int col_index;
    const parquet::ArrowReaderProperties & arrow_properties;
    const parquet::ColumnDescriptor & col_descriptor;
    DataTypePtr ch_type;


    UInt32 getScaleFromLogicalTimestamp(parquet::LogicalType::TimeUnit::unit tm_unit);
    UInt32 getScaleFromArrowTimeUnit(arrow::TimeUnit::type tm_unit);

    std::unique_ptr<ParquetColumnReader> fromInt32(parquet::RowGroupReader & row_group_reader);
    std::unique_ptr<ParquetColumnReader> fromInt64(parquet::RowGroupReader & row_group_reader);
    std::unique_ptr<ParquetColumnReader> fromByteArray(parquet::RowGroupReader & row_group_reader);
    std::unique_ptr<ParquetColumnReader> fromFLBA(parquet::RowGroupReader & row_group_reader);

    std::unique_ptr<ParquetColumnReader> fromInt32INT(
        parquet::RowGroupReader & row_group_reader, const parquet::IntLogicalType & int_type);
    std::unique_ptr<ParquetColumnReader> fromInt64INT(
        parquet::RowGroupReader & row_group_reader, const parquet::IntLogicalType & int_type);

    template<class DataType>
    auto makeLeafReader(parquet::RowGroupReader & row_group_reader)
    {
        return std::make_unique<ParquetLeafColReader<typename DataType::ColumnType>>(
            col_descriptor, std::make_shared<DataType>(),
            row_group_reader.metadata()->ColumnChunk(col_index),
            row_group_reader.GetColumnPageReader(col_index));
    }

    template<class DecimalType>
    auto makeDecimalLeafReader(parquet::RowGroupReader & row_group_reader)
    {
        auto data_type = std::make_shared<DataTypeDecimal<DecimalType>>(
            col_descriptor.type_precision(), col_descriptor.type_scale());
        return std::make_unique<ParquetLeafColReader<ColumnDecimal<DecimalType>>>(
            col_descriptor, std::move(data_type),
            row_group_reader.metadata()->ColumnChunk(col_index),
            row_group_reader.GetColumnPageReader(col_index));
    }

    std::unique_ptr<ParquetColumnReader> throwUnsupported(std::string msg = "")
    {
        throw Exception(
            ErrorCodes::PARQUET_EXCEPTION,
            "Unsupported logical type: {} and physical type: {} for field `{}`{}",
            col_descriptor.logical_type()->ToString(), col_descriptor.physical_type(), col_descriptor.name(), msg);
    }
};

UInt32 LeafColReaderFactory::getScaleFromLogicalTimestamp(parquet::LogicalType::TimeUnit::unit tm_unit)
{
    switch (tm_unit)
    {
        case parquet::LogicalType::TimeUnit::MILLIS:
            return 3;
        case parquet::LogicalType::TimeUnit::MICROS:
            return 6;
        case parquet::LogicalType::TimeUnit::NANOS:
            return 9;
        default:
            throwUnsupported(PreformattedMessage::create(", invalid timestamp unit: {}", tm_unit));
            return 0;
    }
}

UInt32 LeafColReaderFactory::getScaleFromArrowTimeUnit(arrow::TimeUnit::type tm_unit)
{
    switch (tm_unit)
    {
        case arrow::TimeUnit::MILLI:
            return 3;
        case arrow::TimeUnit::MICRO:
            return 6;
        case arrow::TimeUnit::NANO:
            return 9;
        default:
            throwUnsupported(PreformattedMessage::create(", invalid arrow time unit: {}", tm_unit));
            return 0;
    }
}

std::unique_ptr<ParquetColumnReader> LeafColReaderFactory::fromInt32(parquet::RowGroupReader & row_group_reader)
{
    switch (col_descriptor.logical_type()->type())
    {
        case parquet::LogicalType::Type::INT:
            return fromInt32INT(row_group_reader, dynamic_cast<const parquet::IntLogicalType &>(*col_descriptor.logical_type()));
        case parquet::LogicalType::Type::NONE:
            return makeLeafReader<DataTypeInt32>(row_group_reader);
        case parquet::LogicalType::Type::DATE:
            return makeLeafReader<DataTypeDate32>(row_group_reader);
        case parquet::LogicalType::Type::DECIMAL:
            return makeDecimalLeafReader<Decimal32>(row_group_reader);
        default:
            return throwUnsupported();
    }
}

std::unique_ptr<ParquetColumnReader> LeafColReaderFactory::fromInt64(parquet::RowGroupReader & row_group_reader)
{
    switch (col_descriptor.logical_type()->type())
    {
        case parquet::LogicalType::Type::INT:
            return fromInt64INT(
                row_group_reader, dynamic_cast<const parquet::IntLogicalType &>(*col_descriptor.logical_type()));
        case parquet::LogicalType::Type::NONE:
            return makeLeafReader<DataTypeInt64>(row_group_reader);
        case parquet::LogicalType::Type::TIMESTAMP:
        {
            const auto & tm_type = dynamic_cast<const parquet::TimestampLogicalType &>(*col_descriptor.logical_type());
            auto read_type = std::make_shared<DataTypeDateTime64>(getScaleFromLogicalTimestamp(tm_type.time_unit()));
            return std::make_unique<ParquetLeafColReader<ColumnDecimal<DateTime64>>>(
                col_descriptor, std::move(read_type),
                row_group_reader.metadata()->ColumnChunk(col_index),
                row_group_reader.GetColumnPageReader(col_index));
        }
        case parquet::LogicalType::Type::DECIMAL:
            return makeDecimalLeafReader<Decimal64>(row_group_reader);
        default:
            return throwUnsupported();
    }
}

std::unique_ptr<ParquetColumnReader> LeafColReaderFactory::fromByteArray(parquet::RowGroupReader & row_group_reader)
{
    switch (col_descriptor.logical_type()->type())
    {
        case parquet::LogicalType::Type::STRING:
        case parquet::LogicalType::Type::NONE:
            return makeLeafReader<DataTypeString>(row_group_reader);
        default:
            return throwUnsupported();
    }
}

std::unique_ptr<ParquetColumnReader> LeafColReaderFactory::fromFLBA(parquet::RowGroupReader & row_group_reader)
{
    switch (col_descriptor.logical_type()->type())
    {
        case parquet::LogicalType::Type::DECIMAL:
        {
            if (col_descriptor.type_length() > 0)
            {
                if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal128)))
                    return makeDecimalLeafReader<Decimal128>(row_group_reader);
                else if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal256)))
                    return makeDecimalLeafReader<Decimal256>(row_group_reader);
            }

            return throwUnsupported(PreformattedMessage::create(
                ", invalid type length: {}", col_descriptor.type_length()));
        }
        default:
            return throwUnsupported();
    }
}

std::unique_ptr<ParquetColumnReader> LeafColReaderFactory::fromInt32INT(
    parquet::RowGroupReader & row_group_reader, const parquet::IntLogicalType & int_type)
{
    switch (int_type.bit_width())
    {
        case 32:
        {
            if (int_type.is_signed())
                return makeLeafReader<DataTypeInt32>(row_group_reader);
            else
                return makeLeafReader<DataTypeUInt32>(row_group_reader);
        }
        default:
            return throwUnsupported(PreformattedMessage::create(", bit width: {}", int_type.bit_width()));
    }
}

std::unique_ptr<ParquetColumnReader> LeafColReaderFactory::fromInt64INT(
    parquet::RowGroupReader & row_group_reader, const parquet::IntLogicalType & int_type)
{
    switch (int_type.bit_width())
    {
        case 64:
        {
            if (int_type.is_signed())
                return makeLeafReader<DataTypeInt64>(row_group_reader);
            else
                return makeLeafReader<DataTypeUInt64>(row_group_reader);
        }
        default:
            return throwUnsupported(PreformattedMessage::create(", bit width: {}", int_type.bit_width()));
    }
}

// refer: GetArrowType method in schema_internal.cc of arrow
std::unique_ptr<ParquetColumnReader> LeafColReaderFactory::makeReader(parquet::RowGroupReader & row_group_reader)
{
    switch (col_descriptor.physical_type())
    {
        case parquet::Type::BOOLEAN:
            break;
        case parquet::Type::INT32:
            return fromInt32(row_group_reader);
        case parquet::Type::INT64:
            return fromInt64(row_group_reader);
        case parquet::Type::INT96:
        {
            DataTypePtr read_type = ch_type;
            if (!isDateTime64(ch_type))
            {
                auto scale = getScaleFromArrowTimeUnit(arrow_properties.coerce_int96_timestamp_unit());
                read_type = std::make_shared<DataTypeDateTime64>(scale);
            }
            return std::make_unique<ParquetLeafColReader<ColumnDecimal<DateTime64>>>(
                col_descriptor, read_type,
                row_group_reader.metadata()->ColumnChunk(col_index),
                row_group_reader.GetColumnPageReader(col_index));
        }
        case parquet::Type::FLOAT:
            return makeLeafReader<DataTypeFloat32>(row_group_reader);
        case parquet::Type::DOUBLE:
            return makeLeafReader<DataTypeFloat64>(row_group_reader);
        case parquet::Type::BYTE_ARRAY:
            return fromByteArray(row_group_reader);
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return fromFLBA(row_group_reader);
        default:
            break;
    }

    return throwUnsupported();
}

class GroupNodeHelper
{
public:
    GroupNodeHelper(const parquet::schema::GroupNode & group_node_, bool case_insensitive_column_matching_)
    : group_node(group_node_), case_insensitive_column_matching(case_insensitive_column_matching_)
    {
        if (case_insensitive_column_matching)
        {
            for (int i = 0; i < group_node.field_count(); ++i)
            {
                auto field_name = group_node.field(i)->name();
                boost::to_lower(field_name);
                field_index_map[field_name] = i;
            }
        }
    }

    int fieldIndex(String field_name)
    {
        if (case_insensitive_column_matching)
        {
            boost::to_lower(field_name);
            auto it = field_index_map.find(field_name); 
            if (it == field_index_map.end())
                return -1;
            else
                return it->second;
        }
        else
        {
            return group_node.FieldIndex(field_name);
        }
    }

private:
    std::unordered_map<String, int> field_index_map;

    const parquet::schema::GroupNode & group_node;
    bool case_insensitive_column_matching;
};

class ParquetSchemaParser
{
public:
    ParquetSchemaParser(
        const parquet::SchemaDescriptor * schema_,
        const Block & header_,
        const FormatSettings & format_settings_,
        const parquet::ArrowReaderProperties & arrow_properties_)
        : schema(schema_), header(header_), format_settings(format_settings_), arrow_properties(arrow_properties_) {}

    ParquetColReaderFactories makeColReaderFactories();

    const std::vector<int> & usedColIndice() { return parquet_col_indice; }

private:
    const parquet::SchemaDescriptor * schema;
    const Block & header;
    const FormatSettings & format_settings;
    parquet::ArrowReaderProperties arrow_properties;

    std::vector<int> parquet_col_indice;

    ParquetColReaderFactoryPtr makeColReaderFactory(
        const parquet::schema::NodePtr node, DataTypePtr ch_type, std::string field_path);
    ParquetColReaderFactoryPtr makeFactoryFromGroup(
        const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, std::string field_path);
    ParquetColReaderFactoryPtr makeFactoryFromList(
        const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, std::string field_path);
    ParquetColReaderFactoryPtr makeFactoryFromMap(
        const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, std::string field_path);
};

ParquetColReaderFactories ParquetSchemaParser::makeColReaderFactories()
{
    ParquetColReaderFactories ret;
    auto gnode_helper = GroupNodeHelper(
        *schema->group_node(), format_settings.parquet.case_insensitive_column_matching);
    for (const auto & col_with_name : header)
    {
        auto field_idx = gnode_helper.fieldIndex(col_with_name.name);
        if (field_idx < 0)
        {
            // TODO null as default
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "no column with '{}' in parquet file", col_with_name.name);
        }
        auto factory = makeColReaderFactory(
            schema->group_node()->field(field_idx), col_with_name.type, col_with_name.name);
        ret.emplace_back(std::move(factory));
    }
    return ret;
}

ParquetColReaderFactoryPtr ParquetSchemaParser::makeColReaderFactory(
    const parquet::schema::NodePtr node, DataTypePtr ch_type, std::string field_path)
{
    if (node->is_group())
    {
        if (node->logical_type()->is_list()) {
            return makeFactoryFromList(static_cast<const parquet::schema::GroupNode &>(*node), ch_type, field_path);
        } else if (node->logical_type()->is_map()) {
            return makeFactoryFromMap(static_cast<const parquet::schema::GroupNode &>(*node), ch_type, field_path);
        }
        // std::shared_ptr<ArrowType> type;
        if (node->is_repeated()) {
        }
        else
        {
            return makeFactoryFromGroup(static_cast<const parquet::schema::GroupNode &>(*node), ch_type, field_path);
        }
    }
    else
    {
        // TODO repeated
        auto col_idx = schema->ColumnIndex(*node);
        parquet_col_indice.push_back(col_idx);
        return std::make_unique<LeafColReaderFactory>(
            col_idx,
            arrow_properties,
            *schema->Column(col_idx),
            ch_type);
    }
    return nullptr;
}

ParquetColReaderFactoryPtr ParquetSchemaParser::makeFactoryFromGroup(
    const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, std::string field_path)
{
    const auto ch_tuple_type = typeid_cast<DataTypeTuplePtr>(ch_type);
    if (!ch_tuple_type)
    {
        throw Exception(
            ErrorCodes::PARQUET_EXCEPTION, "can not read group field {} to {}", field_path, ch_type->getFamilyName());
    }
    if (!ch_tuple_type->haveExplicitNames())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "only explicit names supported for tuple field {}", field_path);
    }

    ParquetColReaderFactories factories;
    auto gnode_helper = GroupNodeHelper(group_node, format_settings.parquet.case_insensitive_column_matching);
    for (size_t i = 0; i < ch_tuple_type->getElements().size(); i++)
    {
        const auto & field_name = ch_tuple_type->getElementNames()[i];
        auto sub_field_path = field_path + "." + field_name;
        auto field_idx = gnode_helper.fieldIndex(field_name);
        if (field_idx < 0)
        {
            // TODO null as default
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "column path {} not found", sub_field_path);
        }
        factories.emplace_back(makeColReaderFactory(
            group_node.field(field_idx), ch_tuple_type->getElement(i), sub_field_path));
    }
    return std::make_unique<TupleColReaderFactory>(std::move(factories), std::move(ch_tuple_type));
}

ParquetColReaderFactoryPtr ParquetSchemaParser::makeFactoryFromList(
    const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, std::string field_path)
{
    if (group_node.field_count() != 1)
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "LIST-annotated group {} must have a single child.", field_path);
    }
    if (group_node.is_repeated())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "LIST-annotated group {} must not be repeated.", field_path);
    }
    const auto ch_array_type = typeid_cast<const DataTypeArray *>(ch_type.get());
    if (!ch_array_type)
    {
        throw Exception(
            ErrorCodes::PARQUET_EXCEPTION, "can not read list field {} to {}", field_path, ch_type->getFamilyName());
    }
    return nullptr;
}

ParquetColReaderFactoryPtr ParquetSchemaParser::makeFactoryFromMap(
    const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, std::string field_path)
{
    if (group_node.field_count() != 1)
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "MAP-annotated group {} must have a single child.", field_path);
    }
    if (group_node.is_repeated())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "MAP-annotated group {} must not be repeated.", field_path);
    }

    const auto & kv_node = *group_node.field(0);

    if (!kv_node.is_repeated())
    {
        static constexpr auto msg = "Non-repeated key value in a MAP-annotated group {} are not supported.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, field_path);
    }

    if (!kv_node.is_group())
    {
        static constexpr auto msg = "Key-value node must be a group in a MAP-annotated group {}.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, field_path);
    }

    const auto & kv_group_node = static_cast<const parquet::schema::GroupNode &>(kv_node);
    if (kv_group_node.field_count() != 1 && kv_group_node.field_count() != 2)
    {
        static constexpr auto msg = "Key-value map node must have 1 or 2 child elements in a MAP-annotated group {}. Found {}";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, field_path, kv_group_node.field_count());
    }

    if (kv_group_node.field_count() == 1)
    {
        return makeFactoryFromGroup(group_node, ch_type, field_path);
    }
    return nullptr;
}

} // anonymous namespace

ParquetRecordReader::ParquetRecordReader(
    Block header_,
    const parquet::ArrowReaderProperties & arrow_properties,
    const parquet::ReaderProperties & reader_properties,
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
    const FormatSettings & format_settings,
    std::vector<int> row_groups_indices_,
    std::shared_ptr<parquet::FileMetaData> metadata)
    : file_reader(createFileReader(std::move(arrow_file), reader_properties, std::move(metadata)))
    , header(std::move(header_))
    , max_block_size(format_settings.parquet.max_block_size)
    , row_groups_indices(std::move(row_groups_indices_))
    , left_rows(getTotalRows(*file_reader->metadata()))
{
    log = &Poco::Logger::get("ParquetRecordReader");

    std::vector<int> parquet_col_indice;
    ParquetSchemaParser parser(file_reader->metadata()->schema(), header, format_settings, arrow_properties);
    col_reader_factories = parser.makeColReaderFactories();
    if (arrow_properties.pre_buffer())
    {
        THROW_PARQUET_EXCEPTION(file_reader->PreBuffer(
            row_groups_indices,
            parser.usedColIndice(),
            arrow_properties.io_context(),
            arrow_properties.cache_options()));
    }
}

Chunk ParquetRecordReader::readChunk()
{
    if (!left_rows)
    {
        return Chunk{};
    }
    if (!cur_row_group_left_rows)
    {
        loadNextRowGroup();
    }

    Columns columns(header.columns());
    auto num_rows_read = std::min(max_block_size, cur_row_group_left_rows);
    for (size_t i = 0; i < header.columns(); i++)
    {
        columns[i] = castColumn(
            column_readers[i]->readBatch(num_rows_read, header.getByPosition(i).name),
            header.getByPosition(i).type);
    }
    left_rows -= num_rows_read;
    cur_row_group_left_rows -= num_rows_read;

    return Chunk{std::move(columns), num_rows_read};
}

void ParquetRecordReader::loadNextRowGroup()
{
    Stopwatch watch(CLOCK_MONOTONIC);
    auto row_group_reader = file_reader->RowGroup(row_groups_indices[next_row_group_idx]);

    column_readers.clear();
    for (auto & factory : col_reader_factories)
    {
        column_readers.emplace_back(factory->makeReader(*row_group_reader));
    }

    auto duration = watch.elapsedNanoseconds() / 1e6;
    LOG_DEBUG(log, "begin to read row group {} consumed {} ms", row_groups_indices[next_row_group_idx], duration);

    ++next_row_group_idx;
    cur_row_group_left_rows = row_group_reader->metadata()->num_rows();
}

Int64 ParquetRecordReader::getTotalRows(const parquet::FileMetaData & meta_data)
{
    Int64 res = 0;
    for (auto idx : row_groups_indices)
    {
        res += meta_data.RowGroup(idx)->num_rows();
    }
    return res;
}

}
