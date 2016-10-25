//
//  dberror.h
//  testcode_2_0920
//
//  Created by Pingyu Xue on 9/21/16.
//  Copyright © 2016 Pingyu Xue. All rights reserved.
//


#ifndef dberror_h
#define dberror_h

#include <stdio.h>

/* module wide constants */
#define PAGE_SIZE 4096//4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_FILE_CLOSE_ERROR 3   // add by pingyu Xue   09/20
#define RC_FILE_REMOVE_ERROR 4  // add by Pingyu Xue   09/20
#define RC_CREATE_PAGE_FAIL 5  // add by Pingyu Xue    09/20
#define RC_WRITE_FAILED 6
#define RC_READ_NON_EXISTING_PAGE 7
#define RC_READ_FAILED 8
#define RC_UNESPECTED_ERROR 9
#define RC_mempage_null   10

#define RC_SET_POINTER_FAILED 100
#define RC_GET_NUMBER_OF_BYTES_FAILED 101

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205
#define RC_FILE_CLOSE_FAILED 206  // add by pingyu Xue 9/25

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303


#define RC_NO_SUCH_PAGE_IN_BUFF  401  // add by pingyu xue 9/25


#define RC_FUNC_Fseek_ERROR 501   // add by pingyu xue 09/21
#define RC_FUNC_Ftell_ERROR 502   // add by pingyu xue 09/21

#define RC_SHUT_DOWN_POOL_ERROR 601 // add by pingyu xue 10/25

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
do {			  \
RC_message=message;	  \
return rc;		  \
} while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
do {									\
int rc_internal = (code);						\
if (rc_internal != RC_OK)						\
{									\
char *message = errorMessage(rc_internal);			\
printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
free(message);							\
exit(1);							\
}									\
} while(0);


#endif /* dberror_h */

