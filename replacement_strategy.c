
//
//  replacement_strategy.c
//  testcode_1_1010
//
//  Created by 平宇薛 on 10/24/16.
//  Copyright © 2016 pingyuXue. All rights reserved.
//

#include "replacement_strategy.h"


#include <stdlib.h>
#include <stdio.h>
#include "buffer_mgr.h"
#include "replacement_strategy.h"
#include "storage_mgr.h"
#define false 0
#define true 1
#define MAX 100000

/*Author: Hanqiao Lu
 *Date: 23th Oct
 *CWID: A20324072
 */

doubleLinkedList *initializeStrategy(int totalNumPages, int capacity) {
    doubleLinkedList *list = malloc(sizeof(doubleLinkedList));
    //How to do something like this??
    //------------------------------
    pageFrame frames[totalNumPages];
    for(int i = 0; i < totalNumPages; i++) {
        frames[i] = NULL;
    }
    list.pageNumToFrame = frames;
    //----------------------------
    list->head = NULL;
    list.tail = NULL;
    list.capacity = capacity;
    list.minFreq = MAX;
    return list;
}

pageFrame *createFrame(BM_PageHandle *page) {
    pageFrame *node = (pageFrame *) malloc(sizeof(pageFrame));
    node->pageContent = page;
    node->pageNum = page->pageNum;
    node->freq = 0;
    node->fixCount = 0;
    node->isDirty = false;
    node->prev = NULL;
    node->next = NULL;
    return node;
}

int fifo_put(doubleLinkedList *list, BM_PageHandle *page, SM_FileHandle *fHandle) {
    //case1: empty list
    if (list->tail == NULL) {
        list->tail = createFrame(page);
        list->head = list.tail;
        list->pageNumToFrame[page->pageNum] = list->tail;
        list->currSize += 1;
        return 1;
    }
    //case2: the frame is already there
    if (list.pageNumToFrame[page->pageNum] != NULL) {
        printf("This page has already been in the memory.");
        return 0;
    }
    //case3: the capacity is full
    if (list.currSize >= list.capacity) {
        fifo_eviction(list, fHandle);
    }
    //case4: not empty, have not been inserted, not out of capacity.
    pageFrame *newFrame = createFrame(page);
    list.tail.next = newFrame;
    newFrame.prev = list.tail;
    list.tail = newFrame;
    list.pageNumToFrame[page->pageNum] = newFrame;
    list.currSize += 1;
    printf("Added a new Frame %d successfully!", page.pageNum);
    return 1;
}

//Eviction strategy for fifo and lru
int fifo_eviction(doubleLinkedList *list, SM_FileHandle *fHandle) {
    //Find the Least Recent frame that fixCount is 0.
    pageFrame *curr = list.head;
    while (curr != NULL && curr.fixCount != 0) {
        curr = curr.next;
    }
    
    //All frames are pinned by client
    if (curr == NULL) {
        printf("No frames can be freed because all of them are in use!");
        return 0;
    }
    printf("Eviction of the frame %d", curr.pageNum);
    
    //If it is dirty, write it back to disk.
    if (curr.isDirty) {
        writeBlock(curr.pageNum, fHandle, curr.pageContent.data);
        printf("Dirty Data, write it into file!");
    }
    
    //Remove the frame from memory
    if (list.head == curr) {
        list.head = list.head.next;
        list.head.prev = NULL;
    } else if (list.tail == curr) {
        list.tail = list.tail.prev;
        list.tail.next = NULL;
    } else {
        curr.prev.next = curr.next;
        curr.next.prev = curr.prev;
    }
    free(curr);
    list.currSize -= 1;
    return 1;
}


pageFrame *fifo_get(doubleLinkedList *list, int pageNumber) {
    //If in the memory return it.
    if (list.pageNumToFrame[pageNumber]) {
        return list.pageNumToFrame[pageNumber];
    }
    return NULL;
}


int lru_put(doubleLinkedList *list, BM_PageHandle *page, SM_FileHandle *fHandle) {
    return fifo_put(list, page, fHandle);
}


pageFrame *lru_get(doubleLinkedList *list, int pageNumber) {
    if (list.pageNumToFrame[pageNumber]) {
        pageFrame *myFrame = list.pageNumToFrame[pageNumber];
        if (myFrame.prev == NULL) {
            myFrame.next.prev = NULL;
            list.head = myFrame.next;
            list.tail.next = myFrame;
            myFrame.prev = list.tail;
            myFrame.next = NULL;
            list.tail = myFrame;
        } else if (myFrame.next == NULL) {
            //Do Nothing
        } else {
            myFrame.prev.next = myFrame.next;
            myFrame.next.prev = myFrame.prev;
            myFrame.prev = list.tail;
            list.tail.next = myFrame;
            myFrame.next = NULL;
            list.tail = list.tail.next;
        }
        return myFrame;
    }
    return NULL;
}

int lfu_put(doubleLinkedList *list, BM_PageHandle *page, SM_FileHandle *fHandle) {
    //case1: empty list
    if (list->tail == NULL) {
        list->tail = createFrame(page);
        list->head = list.tail;
        list->pageNumToFrame[page->pageNum] = list->tail;
        list->currSize += 1;
        list.minFreq = list.tail.freq < list.minFreq ? list.tail.freq : list.minFreq;
        return 1;
    }
    //case2: the frame is already there
    if (list.pageNumToFrame[page->pageNum] != NULL) {
        printf("This page has already been in the memory.");
        return 0;
    }
    //case3: the capacity is full
    if (list.currSize >= list.capacity) {
        lfu_eviction(list, fHandle);
    }
    //case4: not empty, have not been inserted, not out of capacity.
    pageFrame *newFrame = createFrame(page);
    list.tail.next = newFrame;
    newFrame.prev = list.tail;
    list.tail = newFrame;
    list.pageNumToFrame[page->pageNum] = newFrame;
    list.currSize += 1;
    list.minFreq = list.tail.freq < list.minFreq ? list.tail.freq : list.minFreq;
    printf("Added a new Frame %d successfully!", page.pageNum);
    return 1;
}

//eviction strategy for lfu
int lfu_eviction(doubleLinkedList *list, SM_FileHandle *fHandle) {
    //Find the Least Recent frame that fixCount is 0.
    pageFrame *curr = list.head;
    pageFrame *target = NULL;
    while (curr != NULL) {
        if (curr.fixCount == 0 && curr.freq <= list.minFreq) {
            target = curr;
            break;
        }
        curr = curr.next;
    }
    //All frames are pinned by client
    if (target == NULL) {
        printf("No frames can be freed because all of them are in use!");
        return 0;
    }
    printf("Eviction of the frame %d", target.pageNum);
    
    //If it is dirty, write it back to disk.
    if (target.isDirty) {
        writeBlock(target.pageNum, fHandle, target.pageContent.data);
        printf("Dirty Data, write it into file!");
    }
    
    //Remove the frame from memory
    if (list.head == target) {
        list.head = list.head.next;
        list.head.prev = NULL;
    } else if (list.tail == target) {
        list.tail = list.tail.prev;
        list.tail.next = NULL;
    } else {
        curr.prev.next = curr.next;
        curr.next.prev = curr.prev;
    }
    free(target);
    list.currSize -= 1;
    return 1;
}


pageFrame *lfu_get(doubleLinkedList *list, int pageNumber) {
    if (list.pageNumToFrame[pageNumber]) {
        pageFrame *myFrame = list.pageNumToFrame[pageNumber];
        if (myFrame.prev == NULL) {
            myFrame.next.prev = NULL;
            list.head = myFrame.next;
            list.tail.next = myFrame;
            myFrame.prev = list.tail;
            myFrame.next = NULL;
            list.tail = myFrame;
        } else if (myFrame.next == NULL) {
            //Do Nothing
        } else {
            myFrame.prev.next = myFrame.next;
            myFrame.next.prev = myFrame.prev;
            myFrame.prev = list.tail;
            list.tail.next = myFrame;
            myFrame.next = NULL;
            list.tail = list.tail.next;
        }
        //Update frequency
        myFrame.freq += 1;
        list.minFreq = list.tail.freq < list.minFreq ? list.tail.freq : list.minFreq;
        return myFrame;
    }
    //If not found return NULL.
    return NULL;
}

/*Integration each put strategy*/
int put(doubleLinkedList *list, BM_PageHandle *page, SM_FileHandle *fHandle, const ReplacementStrategy algorithm) {
    int state = 0;
    switch(algorithm) {
        case 0:
            state = fifo_put(list, page, fHandle);
            break;
        case 1:
            state = lru_put(list, page, fHandle);
            break;
        case 3:
            state = lfu_put(list, page, fHandle);
            break;
        default:
            prinf("This strategy not implemented or not in the range.");
    }
    return state;
}

/*Get a page from the data structure as per strategy*/
pageFrame *get(doubleLinkedList *list, int pageNumber, const ReplacementStrategy algorithm) {
    pageFrame *frame = NULL;
    switch(algorithm) {
        case 0:
            frame = fifo_get(list, pageNumber);
            break;
        case 1:
            frame = lru_get(list, pageNumber);
            break;
        case 3:
            frame = lfu_get(list, pageNumber);
            break;
        default:
            prinf("This strategy not implemented or not in the range.");
    }
    return frame;
}


