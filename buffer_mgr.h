//
//  buffer_mgr.h
//  testcode_1_1010
//
//  Created by PingyuXue on 10/12/16.
//  Copyright © 2016 pingyuXue. All rights reserved.
//



#include <stdio.h>

#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
    RS_FIFO = 0,
    RS_LRU = 1,
    RS_CLOCK = 2,
    RS_LFU = 3,
    RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
    char *pageFile;
    int numPages;
    ReplacementStrategy strategy;
    struct pageFrame *mgmtData;   // changed by pingyuXue 10/25/2016
              // use this one to store the bookkeeping info your buffer
              // manager needs for a buffer pool
    
} BM_BufferPool;

typedef struct BM_PageHandle {
    PageNumber pageNum;
    char *data;
} BM_PageHandle;

//
//typedef struct pageFrame {
//    PageNumber pageNum; // frame page number in the file
//    int freq;
//    int fixCount;
//    int isDirty; // is this dirty
//    BM_PageHandle *pageContent;
//    struct pageFrame *prev;
//    struct pageFrame *next;
//} pageFrame;
//
///*The data structure used to hold frames in memory*/
//typedef struct doubleLinkedList {
//    pageFrame *head;
//    pageFrame *tail;
//    int capacity;
//    int currSize;
//    int minFreq;
//    pageFrame *pageNumToFrame;
//} doubleLinkedList;

//typedef struct Bufferpool{
//    BM_BufferPool bufferPool_bm;
//    int num_Read_IO;   // add by pingyuxue 10/25
//    int num_Write_IO;  // add by pingyuxue 10/25
//    
//    // unknow usage;
//    int setCompare;  //set time for the compare
//}BufferPool;
//
//typedef struct pageFrame{
//    BM_PageHandle pageHandle;
//    bool dirty;
//    int fixCounts;
//    int *strategy_set;
//    void *mgmTransferData; // for the information transfer into mgmtDat
//}pageFrame;


// convenience macros
#define MAKE_POOL()					\
((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);



#endif /* buffer_mgr_h */
