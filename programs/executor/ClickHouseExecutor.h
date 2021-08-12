#ifdef __clang__
#   pragma clang diagnostic ignored "-Wreserved-id-macro"
#endif

#include <jni.h>

#ifndef _Included_TaskExecutor_jni
#define _Included_TaskExecutor_jni
#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jlong JNICALL Java_org_apache_spark_sql_execution_clickhouse_ClickHouseExecutor_executePlan
    (JNIEnv *, jobject, jstring);
JNIEXPORT jboolean JNICALL Java_org_apache_spark_sql_execution_clickhouse_ClickHouseExecutor_initialize
    (JNIEnv *, jobject);
#ifdef __cplusplus
}
#endif
#endif

