#include "TaskExecutor.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Formats/registerFormats.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTFunction.h>
#include <Poco/Util/Application.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageFile.h>
#include <TableFunctions/registerTableFunctions.h>
#include <loggers/Loggers.h>
#include <sys/time.h>

#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Formats/IOutputFormat.h>

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

namespace DB
{
class QueryPipeline;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_IDENTIFIER;
}

static const std::string DEFAULT_DB = "TASK_EXECUTOR_DB";
static const std::string DEFAULT_TABLE = "TASK_EXECUTOR_TABLE";

TaskExecutor::TaskExecutor() = default;

void TaskExecutor::init(int /*argc*/, char ** /*argv*/) {
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerFormats();

    // according to programs/server/Server.cpp, method main
    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::LOCAL);
    global_context->setPath("/tmp/");
}

long TaskExecutor::executePlan(const TaskPlan::PhysicalPlan & plan)
{
    struct timeval t1;
    gettimeofday(&t1, nullptr);
    auto pipe = read(plan.scan_node);

    struct timeval t11;
    gettimeofday(&t11, nullptr);

    // according to ISourceStep::updatePipeline
    auto pipeline = std::make_unique<QueryPipeline>();
    // according to ReadFromPreparedSource::initializePipeline
    pipeline->init(std::move(pipe));

    struct timeval t12;
    gettimeofday(&t12, nullptr);

    auto before_agg_actions = makeBeforeAggActions(plan);

    struct timeval t121;
    gettimeofday(&t121, nullptr);
    auto before_agg_expression = std::make_shared<ExpressionActions>(
        before_agg_actions,
        BuildQueryPipelineSettings::fromContext(global_context).getActionsSettings());

    struct timeval t122;
    gettimeofday(&t122, nullptr);
    pipeline->addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, before_agg_expression);
    });

    struct timeval t13;
    gettimeofday(&t13, nullptr);

    auto transform_params = makeAggTransformParams(plan, pipeline->getHeader(), before_agg_actions);
    pipeline->addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<AggregatingTransform>(header, transform_params);
    });

    struct timeval t14;
    gettimeofday(&t14, nullptr);

    auto before_order_by_actions = makeBeforeOrderByActions(transform_params->params.aggregates);
    // according to ExpressionStep::transformPipeline
    auto before_order_by_expression = std::make_shared<ExpressionActions>(
        before_order_by_actions,
        BuildQueryPipelineSettings::fromContext(global_context).getActionsSettings());
    pipeline->addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, before_order_by_expression);
    });

    struct timeval t15;
    gettimeofday(&t15, nullptr);

    // according to method executeQueryImpl in executeQuery.cpp, but it seems that statement is not necessary
    pipeline->resize(1);

    struct timeval t16;
    gettimeofday(&t16, nullptr);

    // according to method executeQuery in executeQuery.cpp
    pipeline->addSimpleTransform([](const Block & header)
    {
        return std::make_shared<MaterializingTransform>(header);
    });
    struct timeval t2;
    gettimeofday(&t2, nullptr);

    // according to executeQuery.cpp
    // WriteBufferFromFileDescriptor write_buf(STDOUT_FILENO);
    WriteBufferFromOwnString write_buf;
    auto out = global_context->getOutputFormatParallelIfPossible("TSV", write_buf, pipeline->getHeader());
    out->setAutoFlush();
    pipeline->setOutputFormat(std::move(out));

    struct timeval t3;
    gettimeofday(&t3, nullptr);

    auto executor = pipeline->execute();
    executor->execute(1);

    struct timeval t4;
    gettimeofday(&t4, nullptr);

    std::cout << "File: " << plan.scan_node.splits[0].file_path << " Times: ";
#if defined(OS_LINUX)
    printf("(t1 %ld_%6ld, t11 %ld_%6ld, t12 %ld_%6ld, ",
           t1.tv_sec, t1.tv_usec, t11.tv_sec, t11.tv_usec, t12.tv_sec, t12.tv_usec);
    printf("t121 %ld_%6ld, t122 %ld_%6ld, ",
           t121.tv_sec, t121.tv_usec, t122.tv_sec, t122.tv_usec);
    printf("t13 %ld_%6ld, t14 %ld_%6ld, t15 %ld_%6ld, t16 %ld_%6ld, ",
           t13.tv_sec, t13.tv_usec, t14.tv_sec, t14.tv_usec, t15.tv_sec, t15.tv_usec, t16.tv_sec, t16.tv_usec);
    printf("t2 %ld_%6ld, t3 %ld_%6ld, t4 %ld_%6ld)",
           t2.tv_sec, t2.tv_usec, t3.tv_sec, t3.tv_usec, t4.tv_sec, t4.tv_usec);
#endif
    std::cout << " The execute result: " << write_buf.str() << std::endl;

    return std::stol(write_buf.str());
}

long TaskExecutor::executePlan(const JSON & json_plan)
{
    TaskPlan::PhysicalPlan plan;
    plan.jsonInit(json_plan);
    return executePlan(plan);
}

int TaskExecutor::main(const std::vector<std::string> & args)
{
    std::string path = "/Users/zhangzhihong02/study/bigdata/github/ClickHouse/data/clickhouse/store/2a3/2a32849d-3489-4aa6-aa32-849d3489daa6/data.Parquet";
    if (args.size() > 0 && args[0].starts_with("/")) {
        path = args[0];
    }
    TaskPlan::PhysicalPlan plan
    {
        {
            {
                {"a", "Int32"},
                {"b", "Int32"},
                {"c", "String"},
                {"d", "Int32"},
            },
            {
                {path, 0, 0}
            },
            {"b"}
        },
        {
            "plus(b, 1)",
            "plus",
            "",
            "function",
            {
                {
                    "b",
                    "",
                    "",
                    "column",
                    {}
                },
                {
                    "1",
                    "",
                    "Int32",
                    "constant",
                    {}
                }
            },
        },
        "sum"
    };

    if (args.size() > 0 && args[0].starts_with("{")) {
        plan.jsonInit(JSON(args[0]));
    }

    executePlan(plan);

    return Application::EXIT_OK;
}

TaskExecutor::~TaskExecutor() {
    if (global_context)
        global_context->shutdown(); /// required for properly exception handling
}

Pipe TaskExecutor::read(TaskPlan::ScanNode node)
{
    SelectQueryInfo query_info;

    // according to InterpreterCreateQuery.cpp, method getColumnsDescription
    ColumnsDescription columnsDes;
    for (const auto & f : node.fields) {
        columnsDes.add(makeColumn(f.name, f.data_type));
    }

    // according to StorageFile.cpp, method registerStorageFile
    StorageFile::CommonArguments storage_args
    {
        WithContext(global_context),
        StorageID(
            DEFAULT_DB, DEFAULT_TABLE, UUIDHelpers::generateV4()),
        "Parquet",
        {},
        {},
        columnsDes,
        ConstraintsDescription(),
        "",
    };

    // according to InterpreterSelectQuery::executeFetchColumns
    std::vector<std::string> files;
    files.reserve(node.splits.size());
    for (const auto & s : node.splits) {
        files.push_back(s.file_path);
    }
    auto storage = StorageFile::create(files, storage_args);

    auto pipe = storage->read(
        node.filter_field_names,
        // according to InterpreterSelectQuery::InterpreterSelectQuery
        storage->getInMemoryMetadataPtr(),
        // according to IStorage::read, query info only used when empty pipe got
        query_info,
        global_context,
        QueryProcessingStage::Enum::FetchColumns,
        global_context->getSettingsRef().max_block_size,
        // according to StorageFile::read, num streams should be same with file number
        1);
    return pipe;
}

const ActionsDAG::Node & TaskExecutor::fillActions(
    TaskPlan::ExpressionNode expression_node, ActionsDAGPtr actions)
{
    // according to ScopeStack::addFunction int ActionsVisitor.cpp
    ActionsDAG::NodeRawConstPtrs children;
    children.reserve(expression_node.children.size());

    for (const auto & child : expression_node.children)
    {
        children.push_back(&fillActions(child, actions));
    }

    if (expression_node.node_type == "constant")
    {
        // according to ExpressionElementParsers.cpp line 1487
        Field res = getField(expression_node.data_type, expression_node.name);
        auto literal = std::make_shared<ASTLiteral>(res);

        // according to ActionsMatcher::visit in ActionsVisitor.cpp of line 1062
        DataTypePtr type = applyVisitor(FieldToDataType(), literal->value);
        const auto value = convertFieldToType(literal->value, *type);
        ColumnWithTypeAndName column;
        column.name = "1";
        column.column = type->createColumnConst(1, value);
        column.type = type;
        const ActionsDAG::Node & literal_node = actions->addColumn(column);
        actions->getIndex().push_back(&literal_node);
        return literal_node;
    }
    if (expression_node.node_type == "column")
    {
        // according to ActionsMatcher::visit
        for (const auto & node : actions->getIndex()) {
            if (node->type == ActionsDAG::ActionType::INPUT && node->result_name == expression_node.name) {
                return *node;
            }
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "column '{}' has not been added", expression_node.name);

    }
    if (expression_node.node_type == "function")
    {
        // according to ActionsMatcher::visit in ActionsVisitor.cpp of line 765
        auto function_builder = FunctionFactory::instance().get(
            expression_node.function_name, global_context);
        const ActionsDAG::Node & function_node = actions->addFunction(
            function_builder, children, expression_node.name);
        actions->getIndex().push_back(&function_node);
        return function_node;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown node type '{}'", expression_node.node_type);
}

ActionsDAGPtr TaskExecutor::makeBeforeAggActions(const TaskPlan::PhysicalPlan & plan)
{
    NamesAndTypesList init_columns;
    for (const auto & name : plan.scan_node.filter_field_names)
    {
        for (const auto & field : plan.scan_node.fields)
        {
            if (name == field.name)
            {
                init_columns.push_back({name, getDataType(field.data_type)});
            }
        }
    }
    // according to StorageSystemTables::StorageSystemTables and
    // ExpressionActionsChain::lastStep
    auto actions = std::make_shared<ActionsDAG>(init_columns);
    fillActions(plan.before_agg_node, actions);

    // according to ExpressionActionsChain::finalize
    NameSet required_names;
    required_names.insert(plan.before_agg_node.name);
    // according to ExpressionActionsStep::finalize
    actions->removeUnusedActions(required_names);
    return actions;
}

ActionsDAGPtr TaskExecutor::makeBeforeOrderByActions(const AggregateDescriptions & agg_descriptions)
{
    NamesAndTypesList columns;
    // according to ending part of ExpressionAnalyzer::analyzeAggregation
    for (const auto & desc : agg_descriptions)
        columns.emplace_back(desc.column_name, desc.function->getReturnType());
    return std::make_shared<ActionsDAG>(columns);
}

AggregatingTransformParamsPtr TaskExecutor::makeAggTransformParams(
    const TaskPlan::PhysicalPlan & plan, const Block & header, ActionsDAGPtr before_agg_actions)
{
    // according to ExpressionAnalyzer::makeAggregateDescriptions
    AggregateDescription aggregate;
    aggregate.column_name = plan.agg_function_name + "(" + plan.before_agg_node.name + ")";
    aggregate.argument_names = {plan.before_agg_node.name};
    for (const auto & name : aggregate.argument_names)
        aggregate.arguments.push_back(header.getPositionByName(name));
    DataTypes types(aggregate.argument_names.size());
    for (size_t i = 0; i < aggregate.argument_names.size(); ++i) {
        auto name = aggregate.argument_names[i];
        const auto * dag_node = before_agg_actions->tryFindInIndex(name);
        if (!dag_node)
        {
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown identifier '{}' in aggregate function",
                            name);
        }

        types[i] = dag_node->result_type;
    }
    AggregateFunctionProperties properties;
    aggregate.function = AggregateFunctionFactory::instance().get(
        plan.agg_function_name, types, aggregate.parameters, properties);

    // according to InterpreterSelectQuery::executeAggregation
    const Settings & settings = global_context->getSettingsRef();
    Aggregator::Params params(header, {}, {aggregate},
                              false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                              settings.group_by_two_level_threshold,
                              settings.group_by_two_level_threshold_bytes,
                              settings.max_bytes_before_external_group_by,
                              settings.empty_result_for_aggregation_by_empty_set,
                              global_context->getTemporaryVolume(),
                              settings.max_threads,
                              settings.min_free_disk_space_for_temporary_data);

    // according to AggregatingStep::transformPipeline
    return std::make_shared<AggregatingTransformParams>(std::move(params), true);
}

DataTypePtr TaskExecutor::getDataType(std::string type_name)
{
    if (type_name == "Int32") {
        return std::make_shared<DataTypeInt32>();
    } else if (type_name == "Int64") {
        return std::make_shared<DataTypeInt64>();
    } else if (type_name == "Float32") {
        return std::make_shared<DataTypeFloat32>();
    } else if (type_name == "Float64") {
        return std::make_shared<DataTypeFloat64>();
    } else {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type '{}'", type_name);
    }
}

Field TaskExecutor::getField(std::string type_name, std::string value)
{
    std::stringstream ss(value);
    if (type_name == "Int32") {
        Int32 v;
        ss >> v;
        return static_cast<Int32>(v);
    } else if (type_name == "Int64") {
        Int64 v;
        ss >> v;
        return static_cast<Int64>(v);
    } else if (type_name == "Float32") {
        Float32 v;
        ss >> v;
        return static_cast<Float32>(v);
    } else if (type_name == "Float64") {
        Float64 v;
        ss >> v;
        return static_cast<Float64>(v);
    } else {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type '{}'", type_name);
    }
}

ColumnDescription TaskExecutor::makeColumn(const String & name, const String & type)
{
    // Normally, columns are parsed from disk and ASTCreateQuery instance generated,
    // then converted to ColumnsDescription in DatabaseOnDisk.cpp, method createTableFromAST
    ColumnDescription column;
    column.name = name;
    auto ast_type = std::make_shared<ASTFunction>();
    ast_type->name = type;
    ast_type->no_empty_args = true;
    column.type = DataTypeFactory::instance().get(ast_type);
    return column;
}

namespace TaskPlan
{

void PhysicalPlan::jsonInit(const JSON & json)
{
    // for scan node
    scan_node.jsonInit(json["scan_node"]);
    before_agg_node.jsonInit(json["before_agg_node"]);
    agg_function_name = json["agg_function_name"].getString();
}

void Field::jsonInit(const JSON & json)
{
    name = json["name"].getString();
    data_type = json["data_type"].getString();
}

void Split::jsonInit(const JSON & json)
{
    file_path = json["file_path"].getString();
    // in json preference, there is not an exactly same type with long,
    // so the precision loss may be existed
    start_pos = json["start_pos"].getInt();
    length = json["length"].getInt();
}

void ScanNode::jsonInit(const JSON & json)
{
    JSON json_arr = json["fields"];
    for (size_t i = 0; i < json_arr.size(); i++) {
        DB::TaskPlan::Field field;
        field.jsonInit(json_arr[i]);
        fields.push_back(field);
    }
    json_arr = json["splits"];
    for (size_t i = 0; i < json_arr.size(); i++) {
        DB::TaskPlan::Split split;
        split.jsonInit(json_arr[i]);
        splits.push_back(split);
    }
    json_arr = json["filter_field_names"];
    for (size_t i = 0; i < json_arr.size(); i++) {
        filter_field_names.push_back(json_arr[i].getString());
    }
}

void ExpressionNode::jsonInit(const JSON & json)
{
    name = json["name"].getString();
    function_name = json["function_name"].getString();
    data_type = json["data_type"].getString();
    node_type = json["node_type"].getString();

    JSON json_arr = json["children"];
    for (size_t i = 0; i < json_arr.size(); i++)
    {
        ExpressionNode node;
        node.jsonInit(json_arr[i]);
        children.push_back(node);
    }
}

}

}

int mainEntryClickHouseExecutor(int argc, char ** argv)
{

    DB::TaskExecutor executor;
    try
    {
        executor.init(argc, argv);
        return executor.run();
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
