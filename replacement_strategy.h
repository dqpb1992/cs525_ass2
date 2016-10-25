//
//  replacement_strategy.h
//  testcode_1_1010
//
//  Created by 平宇薛 on 10/24/16.
//  Copyright © 2016 pingyuXue. All rights reserved.
//

#include <stdio.h>

#ifndef REPLACEMENT_STRATEGY_H
#define REPLACEMENT_STRATEGY_H
#include "storage_mgr.h"
#include "buffer_mgr.h"

/*The structure of a frame in memory. Used to
 implement an double linkedlist based eviction strategy.*/



// The strategy for management of buffer pool

/*Initialize the linkedHashMap data structure*/
doubleLinkedList *initializeStrategy(int totalNumPages, int capacity);

/*Put a page into the data structure as per strategy*/
int put(doubleLinkedList *list, BM_PageHandle *page, SM_FileHandle *fHandle, const ReplacementStrategy algorithm);

/*Get a page from the data structure as per strategy*/
pageFrame *get(doubleLinkedList *list, int pageNumber, const ReplacementStrategy algorithm);


#endif /* replacement_strategy_h */
