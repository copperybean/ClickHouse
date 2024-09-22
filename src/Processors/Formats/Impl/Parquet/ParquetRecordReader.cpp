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

bool hasStructListName(const std::string & name) {
    // Special case mentioned in the format spec:
    //   If the name is array or ends in _tuple, this should be a list of struct
    //   even for single child elements.
    return name == "array" || endsWith(name, "_tuple");
}

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

class ArrayColReaderFactory : public IParquetColReaderFactory
{
public:
    ArrayColReaderFactory(
        ParquetColReaderFactoryPtr nested_reader_factory_,
        DataTypeArrayPtr ch_array_type_)
        : nested_reader_factory(std::move(nested_reader_factory_))
        , ch_array_type(ch_array_type_)
        {}

    std::unique_ptr<ParquetColumnReader> makeReader(parquet::RowGroupReader & row_group_reader) override
    {
        auto nested_reader = nested_reader_factory->makeReader(row_group_reader);
        return std::make_unique<ParquetArrayColReader>(ch_array_type, std::move(nested_reader));
    }

private:

    ParquetColReaderFactoryPtr nested_reader_factory;
    DataTypeTuplePtr ch_array_type;
};

class LeafColReaderFactory : public IParquetColReaderFactory
{
public:
    LeafColReaderFactory(
        int col_index_,
        const parquet::ArrowReaderProperties & arrow_properties_,
        const parquet::ColumnDescriptor & col_descriptor_,
        DataTypePtr ch_type_,
        std::vector<Int16> lists_def_levels_)
        : col_index(col_index_)
        , arrow_properties(arrow_properties_)
        , col_descriptor(col_descriptor_)
        , ch_type(std::move(ch_type_))
        , lists_def_levels(std::move(lists_def_levels_)) {}

    std::unique_ptr<ParquetColumnReader> makeReader(parquet::RowGroupReader & row_group_reader) override;

private:
    int col_index;
    const parquet::ArrowReaderProperties & arrow_properties;
    const parquet::ColumnDescriptor & col_descriptor;
    DataTypePtr ch_type;
    std::vector<Int16> lists_dev_levels;


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
    auto makeLeafReader(parquet::RowGroupReader & row_group_reader, DataTypePtr data_type = std::make_shared<DataType>())
    {
        // auto chunkMeta = row_group_reader.metadata()->ColumnChunk(col_index);
        // auto repeat_ratio = chunkMeta->num_values() * 1.0 / row_group_reader.metadata()->num_rows(); 
        return std::make_unique<ParquetLeafColReader<typename DataType::ColumnType>>(
            col_descriptor, std::move(data_type), std::move(chunk_meta),
            row_group_reader.GetColumnPageReader(col_index), lists_def_levels);
    }

    template<class DecimalType>
    auto makeDecimalLeafReader(parquet::RowGroupReader & row_group_reader)
    {
        auto data_type = std::make_shared<DataTypeDecimal<DecimalType>>(
            col_descriptor.type_precision(), col_descriptor.type_scale());
        return makeLeafReader<DataTypeDecimal<DecimalType>>(row_group_reader, std::move(data_type));
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
            return makeLeafReader<DataTypeDecimal<DateTime64>>(row_group_reader, std::move(read_type));
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
            return makeLeafReader<DataTypeDecimal<DateTime64>>(row_group_reader, std::move(read_type));
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
    struct VisitContext
    {
        std::string field_path;
        Int16 def_level;
        std::vector<Int16> lists_def_levels;

        void appendChild(const parquet::schema::NodePtr node)
        {
            if (node->is_repeated())
            {
                def_level++;
                lists_def_levels.push_back(new_def_level);
            }
            else if (node->is_optional())
            {
                def_level++;
            }
            field_path += ("." + node->name());
        }

        VisitContext copyWithChild(const parquet::schema::NodePtr node)
        {
            auto copy = *this;
            copy.appendChild(node);
            return copy;
        }
    };

    const parquet::SchemaDescriptor * schema;
    const Block & header;
    const FormatSettings & format_settings;
    parquet::ArrowReaderProperties arrow_properties;

    std::vector<int> parquet_col_indice;

    ParquetColReaderFactoryPtr makeColReaderFactory(
        const parquet::schema::NodePtr node, DataTypePtr ch_type, VisitContext & context);
    ParquetColReaderFactoryPtr makeFactoryFromGroup(
        const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, VisitContext & context);
    ParquetColReaderFactoryPtr makeFactoryFromList(
        const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, VisitContext & context);
    ParquetColReaderFactoryPtr makeFactoryFromMap(
        const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, VisitContext & context);
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
        VisitContext context{col_with_name.name, 0, {}};
        auto factory = makeColReaderFactory(
            schema->group_node()->field(field_idx), col_with_name.type, context);
        ret.emplace_back(std::move(factory));
    }
    return ret;
}

ParquetColReaderFactoryPtr ParquetSchemaParser::makeColReaderFactory(
    const parquet::schema::NodePtr node, DataTypePtr ch_type, VisitContext & context)
{
    if (node->is_group())
    {
        if (node->logical_type()->is_list()) {
            return makeFactoryFromList(static_cast<const parquet::schema::GroupNode &>(*node), ch_type, context);
        } else if (node->logical_type()->is_map()) {
            return makeFactoryFromMap(static_cast<const parquet::schema::GroupNode &>(*node), ch_type, context);
        }
        // std::shared_ptr<ArrowType> type;
        if (node->is_repeated()) {
        }
        else
        {
            return makeFactoryFromGroup(static_cast<const parquet::schema::GroupNode &>(*node), ch_type, context);
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
            ch_type,
            context.lists_def_levels);
    }
    return nullptr;
}

ParquetColReaderFactoryPtr ParquetSchemaParser::makeFactoryFromGroup(
    const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, VisitContext & context)
{
    const auto ch_tuple_type = typeid_cast<DataTypeTuplePtr>(ch_type);
    if (!ch_tuple_type)
    {
        constexpr auto msg = "can not read group field {} to {}";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path, ch_type->getFamilyName());
    }
    if (!ch_tuple_type->haveExplicitNames())
    {
        constexpr auto msg = "only explicit names supported for tuple field {}";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path);
    }

    ParquetColReaderFactories factories;
    auto gnode_helper = GroupNodeHelper(group_node, format_settings.parquet.case_insensitive_column_matching);
    for (size_t i = 0; i < ch_tuple_type->getElements().size(); i++)
    {
        const auto & field_name = ch_tuple_type->getElementNames()[i];
        auto field_idx = gnode_helper.fieldIndex(field_name);
        if (field_idx < 0)
        {
            // TODO null as default
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "column path {} not found", sub_context.field_path);
        }
        auto childNode = group_node.field(field_idx);
        auto sub_context = context.copyWithChild(childNode);
        factories.emplace_back(makeColReaderFactory(childNode, ch_tuple_type->getElement(i), sub_context));
    }
    return std::make_unique<TupleColReaderFactory>(std::move(factories), std::move(ch_tuple_type));
}

ParquetColReaderFactoryPtr ParquetSchemaParser::makeFactoryFromList(
    const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, VisitContext & context)
{
    if (group_node.field_count() != 1)
    {
        constexpr auto msg = "LIST-annotated group {} must have a single child.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path);
    }
    if (group_node.is_repeated())
    {
        constexpr auto msg = "LIST-annotated group {} must not be repeated.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path);
    }
    const auto ch_array_type = typeid_cast<DataTypeArrayPtr>(ch_type);
    if (!ch_array_type)
    {
        constexpr auto msg = "can not read list field {} to {}";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path, ch_type->getFamilyName());
    }

    const parquet::schema::NodePtr list_node = group_node.field(0);
    if (!list_node->is_repeated())
    {
        constexpr auto msg = "Non-repeated node {} in a LIST-annotated group are not supported.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path);
    }
    context.appendChild(list_node);

    if (list_node->is_group())
    {
        // Resolve 3-level encoding, which normally looks like: {group_node}.list[repeated].element
        const auto & list_group = static_cast<const parquet::schema::GroupNode &>(*list_node);
        if (list_group.field_count() == 1 && !hasStructListName(list_group.name()))
        {
            context.appendChild(list_group.field(0));
            auto nested_factory = makeColReaderFactory(group_node.field(0), ch_array_type->getNestedType(), context);
            return std::make_unique<ArrayColReaderFactory>(nested_factory, ch_array_type);
        }
        else
        {
            auto nested_factory = makeFactoryFromGroup(list_group, ch_array_type->getNestedType(), context);
            return std::make_unique<ArrayColReaderFactory>(nested_factory, ch_array_type);
        }
    }
}

ParquetColReaderFactoryPtr ParquetSchemaParser::makeFactoryFromMap(
    const parquet::schema::GroupNode & group_node, DataTypePtr ch_type, VisitContext & context)
{
    if (group_node.field_count() != 1)
    {
        constexpr auto msg = "MAP-annotated group {} must have a single child.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path);
    }
    if (group_node.is_repeated())
    {
        constexpr auto msg = "MAP-annotated group {} must not be repeated.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path);
    }

    const auto & kv_node = *group_node.field(0);

    if (!kv_node.is_repeated())
    {
        constexpr auto msg = "Non-repeated key value in a MAP-annotated group {} are not supported.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path);
    }

    if (!kv_node.is_group())
    {
        constexpr auto msg = "Key-value node must be a group in a MAP-annotated group {}.";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path);
    }

    const auto & kv_group_node = static_cast<const parquet::schema::GroupNode &>(kv_node);
    if (kv_group_node.field_count() != 1 && kv_group_node.field_count() != 2)
    {
        constexpr auto msg = "Key-value map node must have 1 or 2 child elements in a MAP-annotated group {}. Found {}";
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, msg, context.field_path, kv_group_node.field_count());
    }

    if (kv_group_node.field_count() == 1)
    {
        return makeFactoryFromGroup(group_node, ch_type, context);
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
            column_readers[i]->readBatch(num_rows_read, header.getByPosition(i).name).detailed_column,
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
