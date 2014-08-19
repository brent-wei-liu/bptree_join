/*
 * Copyright (C) 2014 by The Regents of the Floridar International University
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Wei Liu <wliu015@cs.fiu.edu>
 * @date 5/12/2014
 */
 
#include "BTreeIndex.h"
#include <queue>
#include <string.h>
#include <climits>
using namespace std;

static PageId getRootPid(const char* page);
static void setRootPid(char* page, PageId pid);

static int getTreeHeight(const char* page);
static void setTreeHeight(char* page, int height);

static int getMinKey(const char* page);
static void setMinKey(char* page, int minKey);

static int getMaxKey(const char* page);
static void setMaxKey(char* page, int maxKey);

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    newPid = 0;
    rootPid = -1;
    treeHeight = -1;
    minKey = INT_MAX;
    maxKey = INT_MIN;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
  RC   rc;
  char page[PageFile::PAGE_SIZE];

  // open the page file
  if ((rc = pf.open(indexname, mode)) < 0) return rc;

  //
  // in the rest of this function, we set the rootPid and  treeHeight
  //

  newPid = pf.endPid();
  // if the end pid is zero, the file is empty.
  // set the end record id to (0, 0).
  if (pf.endPid() == 0) {
    rootPid = -1;
    treeHeight = 0;
    return 0;
  }

  if ((rc = pf.read(0, page)) < 0) {
    // an error occurred during page read
    rootPid  = -1;
    treeHeight = 0;
    pf.close();
    return rc;
  }

  // get rootPid and treeHeight in the first page
  rootPid = getRootPid(page);
  treeHeight = getTreeHeight(page);
  minKey = getMinKey(page);
  maxKey = getMaxKey(page);
  name = indexname;
  return 0;

}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
  RC rc = 0;
  char page[PageFile::PAGE_SIZE];
  memset(page,0,PageFile::PAGE_SIZE);
  //sprintf(page,"%d %d\n",rootPid,treeHeight);
  setRootPid(page, rootPid);
  setTreeHeight(page, treeHeight);
  setMinKey(page, minKey);
  setMaxKey(page, maxKey);

  if ((rc = pf.write(0, page)) < 0) return rc;

  if ( newPid != pf.endPid() ){
      printf("newPid != pf.endPid() error!\n");
      rc = -1;
  }
  rootPid = 0;
  treeHeight = 0;
  pf.close();
  return rc;
}


/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(KeyType key, const RecordId& rid)
{
  RC   rc;
  PageId pid;
  char page[PageFile::PAGE_SIZE];
  BTNode root;
  //printf("\n***************Insert key:"ANSI_COLOR_RED"%d"ANSI_COLOR_RESET" into Tree ******************\n",key);
  DEBUG('i',"\n************* Insert key:%d into Tree , RecordId={pid:%d, sid:%d} ******\n",key, rid.pid, rid.sid);

  if( rootPid == -1){
      BTNode lnode,rnode;
      lnode.isLeaf = rnode.isLeaf = true;
      root.initializeRoot(2,key,3);
      lnode.setNextNodePtr(3);
      rnode.setNextNodePtr(-1);

      rc = root.write(1,pf);
      if(rc != 0) goto ERROR;
      rc = lnode.write(2,pf);
      if(rc != 0) goto ERROR;
      rc = rnode.write(3,pf);
      if(rc != 0) goto ERROR;
      
      rnode.insertNonFull(key, rid, newPid, pf);
      rnode.write(pf);

      rootPid = 1; 
      treeHeight = 1;
      newPid = 4; 
      if(pf.endPid() != 4){
          printf("error: pf.endPid() = %d\n",pf.endPid());
          return -1;
      }
      if( key > maxKey )  maxKey = key;
      if( key < minKey )  minKey = key;
      return 0;
  }
  
  rc = root.read(rootPid, pf);
  if(rc != 0) goto ERROR;
  
  if( root.n == 2*root.getT() - 1){
      DEBUG('i',"New root:%d, height=%d\n",newPid, treeHeight + 1);
      //new root
      BTNode newRoot;
      newRoot.isLeaf = false;
      newRoot.n = 0;
      newRoot.pids[0] = rootPid;
      rootPid = newRoot.pid = newPid;
      newRoot.minKey = root.minKey;
      newRoot.maxKey = root.maxKey;
      newPid++;
      rc = newRoot.splitChild(0,newPid, pf);
      newPid ++;
      if(rc != 0) goto ERROR;
      rc = newRoot.insertNonFull(key, rid, newPid, pf);
      if(rc != 0) goto ERROR;
      rc = newRoot.write(rootPid,pf); 
      if(rc != 0) goto ERROR;
      treeHeight ++;
      
      if(DebugIsEnabled('i'))   printTree();
  }else{
      root.insertNonFull(key, rid, newPid, pf);
  }
  if( key > maxKey )  maxKey = key;
  if( key < minKey )  minKey = key;
  DEBUG('i',"\n**************** Insert Key End *************************\n\n",key, rid.pid, rid.sid);
  return 0;
ERROR:
  printf("error\n");
  return -1;
}

/*
 * Find the leaf-node index entry whose key value is larger than or 
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry 
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned 
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return error code. 0 if no error.
 */
RC BTreeIndex::locate(KeyType searchKey, IndexCursor& cursor) const
{
    RC rc = 0;
    BTNode root;

    DEBUG('s',"\n\n****************** SEARCH KEY:%d IN INDEX TREE **********************\n",searchKey);
    DEBUG('s',"rootPid:%d pageNum:%d treeHeight:%d\n\n",rootPid,  pf.endPid(), treeHeight);
    if( rootPid == -1){
        printf("Empty Tree.\n");
        goto ERROR;
    }
    rc = root.read(rootPid, pf);
    if(rc != 0) goto ERROR;
    rc = root.locate(searchKey, pf, cursor);
    if(rc != 0) goto ERROR;

    DEBUG('s',"\n\n***********SEARCH INDEX TREE END (pid:%d, sid:%d) *************\n",cursor.pid,cursor.eid);
    return 0;

ERROR:
    printf("error\n");
    return rc;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, KeyType& key, RecordId& rid) const
{
    RC rc;
    BTNode node;
    rc = node.read(cursor.pid, pf);
    if(rc != 0) goto ERROR;

    if(!node.isLeaf) goto ERROR;
    rc = node.readEntry( cursor.eid, key, rid);
    if(rc != 0) goto ERROR;

    cursor.eid ++;
    if(cursor.eid >= node.n){
        cursor.pid = node.getNextNodePtr();
        cursor.eid = 0;
    }
    return 0;
ERROR:
    printf("readForward error\n");
    return rc;
}
/*
 * Read the cursor of first key ib b+tree,
 * and move foward the cursor to the next entry.
 * @param cursor[OUT] the cursor pointing to the first leaf-node index entry in the b+tree
 * @return error code. 0 if no error
 */
RC BTreeIndex::getFirstKey(IndexCursor& cursor) const
{
    RC rc;
    BTNode node;
    rc = node.read(rootPid, pf);
    if(rc != 0) goto ERROR;

    while(!node.isLeaf) {
        rc = node.read(node.pids[0],pf);
        if(rc != 0) goto ERROR;
    }
    cursor.pid = node.pid;
    cursor.eid = 0;
    return 0;
ERROR:
    printf("readFirstKey error\n");
    return rc;
}


RC BTreeIndex::getBTNode(int pid, BTNode &node)
{
    RC rc;
    if(rootPid == -1) return -1;
    rc = node.read(pid, pf);
    if(rc != 0) goto ERROR;
    return 0;
ERROR:
    printf("getBTNode error %d\n", rc );
    return rc;
}
RC BTreeIndex::printTree()
{
    RC rc;
    int i;
    queue<BTNode> q;
    BTNode root;
    if(rootPid == -1) return -1;
    rc = root.read(rootPid, pf);
    if(rc != 0) goto ERROR;
    q.push(root);
    printf("\n\n****************** PRINT TREE: %s **********************\n",name.c_str());
    printf("rootPid:%d pageNum:%d treeHeight:%d minKey:%d maxKey:%d\n\n",rootPid,  pf.endPid(), treeHeight, minKey, maxKey);
    for(i=1; i< pf.endPid(); i++){
        BTNode s;
        rc = s.read(i, pf);
        if(rc != 0) goto ERROR;
        s.print();
    }
    printf("\n****************** BFS TREE**********************\n");
    while(!q.empty()){
        BTNode u = q.front();
        q.pop();
        if(u.isLeaf){
            printf("----------------->Leaf Node <----------------\n");
            u.print();
        }else{
            printf("----------------->Non Leaf Node <----------------\n");
            u.print();
            for(int i=0; i<=u.n; i++){
                BTNode v;
                rc = v.read(u.pids[i], pf);
                if(rc != 0) goto ERROR;
                q.push( v );
            }
        }
    }
    printf("\n****************** PRINT TREE END**********************\n");
    return 0;
ERROR:
    printf("printTree error %d\n",rc);
    return rc;
    
}
/*RC BTreeIndex::getRoot(int &pid, BTNode & root)
{

}
*/
static PageId getRootPid(const char* page)
{
  PageId rootPid;

  // the first four bytes of a page contains rootPid in the page
  memcpy(&rootPid, page, sizeof(PageId));
  return rootPid;
}


static void setRootPid(char* page, PageId pid)
{
  // the first four bytes of a page contains rootPid in the page
  memcpy(page, &pid, sizeof(PageId));
}

static int getTreeHeight(const char* page)
{
  int height;

  // the second four bytes of a page contains tree height in the page
  memcpy(&height, page+sizeof(PageId), sizeof(int));
  return height;
}


static void setTreeHeight(char* page, int height)
{
  // the second four bytes of a page contains tree height in the page
  memcpy(page+sizeof(PageId), &height, sizeof(int));
}

static int getMinKey(const char* page)
{
  KeyType minKey;
  // the third four bytes of a page contains min key in the page
  memcpy(&minKey, page+sizeof(PageId)+sizeof(int), sizeof(int));
  return minKey;
}


static void setMinKey(char* page, KeyType minKey)
{
  memcpy(page+sizeof(PageId)+sizeof(int), &minKey, sizeof(int));
}


static int getMaxKey(const char* page)
{
  KeyType maxKey;
  // the forth four bytes of a page contains max key in the page
  memcpy(&maxKey, page+sizeof(PageId)+sizeof(int)+sizeof(int), sizeof(int));
  return maxKey;
}


static void setMaxKey(char* page, KeyType maxKey)
{
  memcpy(page+sizeof(PageId)+sizeof(int)+sizeof(int), &maxKey, sizeof(int));
}


