//
//  storage_mgr.h
//  testcode_2_0920
//
//  Created by Pingyu Xue on 9/21/16.
//  Copyright © 2016 Pingyu Xue. All rights reserved.
//


#ifndef storage_mgr_h
#define storage_mgr_h

#include <stdio.h>
#include "dberror.h"

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
    char *fileName;
    int totalNumPages;
    int curPagePos;
    void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *fHandle);
extern RC destroyPageFile (char *fileName);

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);


#endif /* storage_mgr_h */
