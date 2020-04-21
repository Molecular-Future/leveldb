#include <jni.h>
#include "ldb4j.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
/*
 * Class:     org_csc_backend_bc_leveldb_jni_LDBNative
 * Method:    init
 * Signature: ()V
 */

leveldb::WriteOptions syncWrite;

leveldb::ReadOptions  fastRead;
leveldb::ReadOptions  fillRead;
jbyteArray NULL_OBJ ;

JNIEXPORT void JNICALL Java_org_csc_backend_bc_1leveldb_jni_LDBNative_init
    (JNIEnv *env, jobject caller){
    // printf("level db 4j .init\n");

    syncWrite.sync = true;
    fastRead.fill_cache = false;
    fillRead.fill_cache = true;
   	NULL_OBJ = (jbyteArray)env->NewGlobalRef(NULL);
}

/*
 * Class:     org_csc_backend_bc_leveldb_jni_LDBNative
 * Method:    openDB
 * Signature: (Lorg/csc/backend/bc_leveldb/api/TransactionConfig/Option;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_csc_backend_bc_1leveldb_jni_LDBNative_openDB
(JNIEnv *env, jobject caller, jobject option,jstring jdbname){
	jclass cls = env->GetObjectClass(option);

	jboolean create_if_missing = env->GetBooleanField(option, env->GetFieldID(cls, "create_if_missing", "Z"));
	jboolean error_if_exists = env->GetBooleanField(option, env->GetFieldID(cls, "error_if_exists", "Z"));
	jboolean paranoid_checks = env->GetBooleanField(option, env->GetFieldID(cls, "paranoid_checks", "Z"));
	jboolean reuse_logs = env->GetBooleanField(option, env->GetFieldID(cls, "reuse_logs", "Z"));

	jint max_open_files  = env->GetIntField(option, env->GetFieldID(cls, "max_open_files", "I"));
	jint block_restart_interval  = env->GetIntField(option, env->GetFieldID(cls, "block_restart_interval", "I"));
	jint compression  = env->GetIntField(option, env->GetFieldID(cls, "compression", "I"));

	jlong max_file_size = env->GetLongField(option, env->GetFieldID(cls, "max_file_size", "J"));
	jlong write_buffer_size = env->GetLongField(option, env->GetFieldID(cls, "write_buffer_size", "J"));
	jlong block_size = env->GetLongField(option, env->GetFieldID(cls, "block_size", "J"));

	const char *dbname = env->GetStringUTFChars( jdbname, 0);

   // use your string

      
//     printf("hello.dbname=%s,create_if_missing:%d, \
// error_if_exists:%d, \
// paranoid_checks:%d, \
// reuse_logs:%d, \
// max_open_files:%d, \
// block_restart_interval:%d, \
// compression:%d, \
// max_file_size:%ld, \
// write_buffer_size:%ld, \
// block_size:%ld.\n",
// 			dbname,create_if_missing,error_if_exists,paranoid_checks,reuse_logs,
// 			max_open_files,block_restart_interval,compression,
// 			max_file_size,write_buffer_size,block_size);

	leveldb::Options options;
	options.create_if_missing = create_if_missing;
	options.error_if_exists=error_if_exists;
	options.paranoid_checks=paranoid_checks;
	options.reuse_logs=reuse_logs;
	options.max_open_files=max_open_files;
	options.block_restart_interval=block_restart_interval;
	if(compression==0){
		options.compression=leveldb::kNoCompression;	
	}else{
		options.compression=leveldb::kSnappyCompression;	
	}
	
	options.max_file_size= (size_t)max_file_size;
	options.write_buffer_size= (size_t)write_buffer_size;
	options.block_size= (size_t)block_size;

	leveldb::DB *dbptr = NULL;

    leveldb::Status status=leveldb::DB::Open(options,std::string(dbname),&dbptr);

    if(!status.ok()){
        jclass LDBException = env->FindClass("org/csc/backend/bc_leveldb/jni/LDBException");
        env->ThrowNew(LDBException,status.ToString().c_str());
    }

    env->ReleaseStringUTFChars( jdbname, dbname);
    return (jlong)dbptr;
}

/*
 * Class:     org_csc_backend_bc_leveldb_jni_LDBNative
 * Method:    closeDB
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_csc_backend_bc_1leveldb_jni_LDBNative_closeDB
(JNIEnv *env, jobject caller, jlong dbinst){
	if(dbinst!=0){
		leveldb::DB *dbptr = (leveldb::DB *)dbinst;
		delete dbptr;
	}
}


/*
 * Class:     org_csc_backend_bc_leveldb_jni_LDBNative
 * Method:    syncPut
 * Signature: (J[B[B)I
 */
JNIEXPORT jint JNICALL Java_org_csc_backend_bc_1leveldb_jni_LDBNative_syncPut
(JNIEnv *env, jobject caller, jlong dbinst, jbyteArray jkey, jbyteArray jval){

	if(dbinst==0){
		return -1;
	}
	jbyte* keyPtr = env->GetByteArrayElements( jkey, NULL);
	jsize keyLen = env->GetArrayLength(jkey);

	jbyte* valPtr = env->GetByteArrayElements( jval, NULL);
	jsize valLen = env->GetArrayLength(jval);

	leveldb::DB *dbptr = (leveldb::DB *)dbinst;
	leveldb::Status s = dbptr->Put(syncWrite,leveldb::Slice((const char*)keyPtr,keyLen),leveldb::Slice((const char*)valPtr,valLen));

	env->ReleaseByteArrayElements(jkey, keyPtr, 0);
	env->ReleaseByteArrayElements(jval, valPtr, 0);

    return s.getCode();
}

/*
 * Class:     org_csc_backend_bc_leveldb_jni_LDBNative
 * Method:    syncDelete
 * Signature: (J[B)I
 */
JNIEXPORT jint JNICALL Java_org_csc_backend_bc_1leveldb_jni_LDBNative_syncDelete
(JNIEnv *env, jobject caller,jlong dbinst, jbyteArray jkey){

	if(dbinst==0){
		return -1;
	}

    jbyte* keyPtr = env->GetByteArrayElements( jkey, NULL);
	jsize keyLen = env->GetArrayLength(jkey);

	leveldb::DB *dbptr = (leveldb::DB *)dbinst;
	leveldb::Status s = dbptr->Delete(syncWrite,leveldb::Slice((const char*)keyPtr,keyLen));

	env->ReleaseByteArrayElements(jkey, keyPtr, 0);

    return s.getCode();
}

/*
 * Class:     org_csc_backend_bc_leveldb_jni_LDBNative
 * Method:    syncBatchPut
 * Signature: (J[[B[[B)I
 */
JNIEXPORT jint JNICALL Java_org_csc_backend_bc_1leveldb_jni_LDBNative_syncBatchPut
(JNIEnv *env, jobject caller, jlong dbinst,jobjectArray jkeys, jobjectArray jvals){

	if(dbinst==0){
		return -1;
	}
	jsize keysLen = env->GetArrayLength(jkeys);

	jsize valsLen = env->GetArrayLength(jvals);

	leveldb::DB *dbptr = (leveldb::DB *)dbinst;
	if(keysLen!=valsLen){
		return leveldb::Status::InvalidArgument(leveldb::Slice("keys and values len not equals")).getCode();
	}
	leveldb::WriteBatch batch;
	// printf("get keysLen=%ld\n",keysLen);
	for(int i=0;i<keysLen;i++){

		// printf("process:%d/%ld\n",i,keysLen);

		jbyteArray jkey =  (jbyteArray)env->GetObjectArrayElement(jkeys, i);
		if(jkey==NULL){
			continue;	
		}
		jbyteArray jval =  (jbyteArray)env->GetObjectArrayElement(jvals, i);
		if(jval==NULL){
			env->DeleteLocalRef(jkey);
			continue;
		}
		
		jbyte* keyPtr = env->GetByteArrayElements( jkey, NULL);
		jsize keyLen = env->GetArrayLength(jkey);

		jbyte* valPtr = env->GetByteArrayElements( jval, NULL);
		jsize valLen = env->GetArrayLength(jval);

		batch.Put(leveldb::Slice((const char*)keyPtr,keyLen),leveldb::Slice((const char*)valPtr,valLen));


		env->ReleaseByteArrayElements(jkey, keyPtr, 0);
		env->ReleaseByteArrayElements(jval, valPtr, 0);

		env->DeleteLocalRef(jkey);
		env->DeleteLocalRef(jval);
	}

	leveldb::Status s = dbptr->Write(syncWrite, &batch);

    return s.getCode();
}

/*
 * Class:     org_csc_backend_bc_leveldb_jni_LDBNative
 * Method:    fastGet
 * Signature: (J[B)Ljava/lang/String;
 */
JNIEXPORT jbyteArray JNICALL Java_org_csc_backend_bc_1leveldb_jni_LDBNative_fastGet
  (JNIEnv *env, jobject caller, jlong dbinst,jbyteArray jkey){

	if(dbinst==0){
		return NULL_OBJ;
	}
	jbyte* keyPtr = env->GetByteArrayElements( jkey, NULL);
	jsize keyLen = env->GetArrayLength(jkey);
	leveldb::DB *dbptr = (leveldb::DB *)dbinst;

	std::string value;

	leveldb::Status s = dbptr->Get(fastRead, leveldb::Slice((const char*)keyPtr,keyLen), &value);

	if(s.ok()){
		size_t len = value.length();
		jbyteArray result = env->NewByteArray(len);
		// memcpy(result,value.c_str(),len);
		env->SetByteArrayRegion(result, 0, len,(const jbyte *) value.c_str());

		// printf("get ok:%s\n",value.c_str());
		env->ReleaseByteArrayElements(jkey, keyPtr, 0);
		// env->DeleteLocalRef(result);
		return result;
	}
	else{
		// printf("get error:%s\n",s.ToString().c_str());
	}

	env->ReleaseByteArrayElements(jkey, keyPtr, 0);
	return NULL_OBJ;
}

/*
 * Class:     org_csc_backend_bc_leveldb_jni_LDBNative
 * Method:    fillGet
 * Signature: (J[B)Ljava/lang/String;
 */
JNIEXPORT jbyteArray JNICALL Java_org_csc_backend_bc_1leveldb_jni_LDBNative_fillGet
  (JNIEnv *env, jobject caller, jlong dbinst,jbyteArray jkey){

	if(dbinst==0){
		return NULL_OBJ;
	}
  	jbyte* keyPtr = env->GetByteArrayElements( jkey, NULL);
	jsize keyLen = env->GetArrayLength(jkey);
	leveldb::DB *dbptr = (leveldb::DB *)dbinst;

	std::string value;

	leveldb::Status s = dbptr->Get(fillRead, leveldb::Slice((const char*)keyPtr,keyLen), &value);

	if(s.ok()){
		size_t len = value.length();
		jbyteArray result = env->NewByteArray(len);
		// printf("get ok:%s\n",value.c_str());
		env->SetByteArrayRegion(result, 0, len, (const jbyte *)value.c_str());
		env->ReleaseByteArrayElements(jkey, keyPtr, 0);
		// env->DeleteLocalRef(result);
		return result;
	}
	else{
		// printf("get error:%s\n",s.ToString().c_str());
	}

	env->ReleaseByteArrayElements(jkey, keyPtr, 0);
	return NULL_OBJ;
}

