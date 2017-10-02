#ifndef _DEFINITIONS_H_
#define _DEFINITIONS_H_

#define HOST "localhost"
#define PORT "8080"

#define ACCESS_KEY_ID "05236"
#define SECRET_ACCESS_KEY "802562235"
#define SIGN_VER "v4"

#define BUCKET "testc"
#ifdef CURR_DIR
#define TEMP_DATA_DIR CURR_DIR "../temp_data/"
#else
#define TEMP_DATA_DIR "../temp_data/"
#endif

#define SMALL_TEST_FILE TEMP_DATA_DIR "testFile"
#define MED_TEST_FILE TEMP_DATA_DIR "testFile.medium"
#define LARGE_TEST_FILE TEMP_DATA_DIR "testFile.large"

#define METADATA_KEY "cmeta_key"
#define METADATA_VAL "cmeta_val"

#define CHUNK_SIZE 10485760

#endif  // _DEFINITIONS_H_
