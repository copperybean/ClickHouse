#include "ClickHouseExecutor.h"
#include "TaskExecutor.h"

#include <common/JSON.h>

#ifdef __cplusplus
extern "C"
{
#endif

static DB::TaskExecutor taskExecutor;

char* jstring2char(JNIEnv* env, jstring jstr)
{
    char* rtn = nullptr;
    jclass clsstring = env->FindClass("java/lang/String");
    jstring strencode = env->NewStringUTF("utf-8");
    jmethodID mid = env->GetMethodID(clsstring, "getBytes", "(Ljava/lang/String;)[B");
    jbyteArray barr = static_cast<jbyteArray>(env->CallObjectMethod(jstr, mid, strencode));
    jsize alen = env->GetArrayLength(barr);
    jboolean isCopy(0);
    jbyte* ba = env->GetByteArrayElements(barr, &isCopy);
    if (alen > 0)
    {
        rtn = static_cast<char*>(malloc(alen + 1));
        memcpy(rtn, ba, alen);
        rtn[alen] = 0;
    }
    env->ReleaseByteArrayElements(barr, ba, 0);
    return rtn;
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_sql_execution_clickhouse_ClickHouseExecutor_initialize
    (JNIEnv *, jobject)
{
    taskExecutor.init(0, nullptr);
    return true;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_sql_execution_clickhouse_ClickHouseExecutor_executePlan
    (JNIEnv * env, jobject, jstring json_str_plan)
{
    DB::TaskPlan::PhysicalPlan plan;
    char * str = jstring2char(env, json_str_plan);
    JSON json(str, str + strlen(str));
    return taskExecutor.executePlan(json);
}

#ifdef __cplusplus
}
#endif
