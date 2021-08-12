#pragma once

#include <Poco/Util/Application.h>
#include <common/JSON.h>
#include <loggers/Loggers.h>

#include <vector>

namespace DB
{

namespace TaskPlan
{

struct Field
{
    std::string name;
    // names should be appeared in src/common/types.h
    std::string data_type;

    void jsonInit(const JSON & json);
};

struct Split
{
    std::string file_path;
    long start_pos;
    long length;

    void jsonInit(const JSON & json);
};

struct ScanNode
{
    std::vector<Field> fields;
    std::vector<Split> splits;
    std::vector<std::string> filter_field_names;

    void jsonInit(const JSON & json);
};

struct ExpressionNode
{
    std::string name;
    // such as plus for "+"
    std::string function_name;
    // names should be appeared in src/common/types.h
    std::string data_type;
    // valid values: constant, column, function
    std::string node_type;
    std::vector<ExpressionNode> children;

    void jsonInit(const JSON & json);
};

struct PhysicalPlan
{
    ScanNode scan_node;
    ExpressionNode before_agg_node;
    std::string agg_function_name;

    void jsonInit(const JSON & json);
};

};

class TaskExecutor : public Poco::Util::Application, public Loggers
{
public:
    TaskExecutor();

    void init(int argc, char ** argv);

    // technically, a list or stream of batch rows should be returned
    // currently, returning a long value old used for test
    long executePlan(const TaskPlan::PhysicalPlan & plan);
    long executePlan(const JSON & json_plan);

    int main(const std::vector<std::string> & args) override;

    ~TaskExecutor() override;

private:
    SharedContextHolder shared_context;
    ContextPtr global_context;

    Pipe read(TaskPlan::ScanNode node);
    const ActionsDAG::Node & fillActions(TaskPlan::ExpressionNode expression_node, ActionsDAGPtr actions);
    ActionsDAGPtr makeBeforeAggActions(const TaskPlan::PhysicalPlan & plan);
    ActionsDAGPtr makeBeforeOrderByActions(const AggregateDescriptions & agg_descriptions);
    AggregatingTransformParamsPtr makeAggTransformParams(
        const TaskPlan::PhysicalPlan & plan, const Block & header, ActionsDAGPtr before_agg_actions);
    DataTypePtr getDataType(std::string type_name);
    Field getField(std::string type_name, std::string value);
    ColumnDescription makeColumn(const String & name, const String & type);
};

}
