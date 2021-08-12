set (CLICKHOUSE_EXECUTOR_SOURCES
       	"${CMAKE_CURRENT_SOURCE_DIR}/TaskExecutor.cpp"
       	"${CMAKE_CURRENT_SOURCE_DIR}/jni.cpp")

include_directories("/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include")
include_directories("/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/include/darwin")
# include_directories("$ENV{JAVA_HOME}/include")
# if (OS_DARWIN)
#     include_directories("$ENV{JAVA_HOME}/include/darwin")
# endif()
# if (OS_LINUX)
#     include_directories("$ENV{JAVA_HOME}/include/linux")
# endif()

set (CLICKHOUSE_EXECUTOR_LINK
        PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_dictionaries
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
        )

clickhouse_program_add(executor)

if(NOT CLICKHOUSE_ONE_SHARED)
    target_link_libraries(clickhouse-executor-lib PRIVATE clickhouse-server-lib)
endif()
