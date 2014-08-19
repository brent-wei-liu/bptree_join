/**
 * Copyright (C) 2014 by The Regents of FIU
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Wei Liu <wliu015@cs.fiu.edu>
 * @date 5/12/2014
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include <climits>
#include <list>
#include <queue> 
#include <vector>
#include <algorithm>
#include "Bruinbase.h"
#include "SqlEngine.h"


using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}
RC SqlEngine::selectWithIndex(int attr, const string& table, 
        const vector<SelCond>& cond, 
        const BTreeIndex &idx, 
        const RecordFile &rf)
{
    bool isRangeQuery = true;
    KeyType EQKey;
    //range query
    // if KeyType is not int, we need to change this conversion
    KeyType  MinKey = INT_MIN;
    KeyType  MaxKey = INT_MAX;
    bool minEQ = true;
    bool maxEQ = true;
    KeyType searchKey, key, k;
    RecordId rid;
    string value;
    IndexCursor cursor;
    RC rc;


    for (unsigned i = 0; i < cond.size(); i++) {
        // compute the difference between the tuple value and the condition value
        k = atoi(cond[i].value);
        if(cond[i].attr == 1){
            // skip the tuple if any condition is not met
            switch (cond[i].comp) {
              case SelCond::EQ:
                isRangeQuery = false;
                // if KeyType is not int, we need to change this conversion
                EQKey = k;
                break;
              case SelCond::GT:
                if(MinKey < k) {
                    MinKey = k;
                    minEQ = false;
                }
                break;
              case SelCond::LT:
                if(MaxKey > k){
                    MaxKey = k;
                    maxEQ = false;
                }
                break;
              case SelCond::GE:
                if(MinKey < k) {
                    MinKey = k;
                    minEQ = true;
                }
                break;
              case SelCond::LE:
                if(MaxKey > k){
                    MaxKey = k;
                    maxEQ = true;
                }
                break;
            }
        }
    }
    //range qery
    if(isRangeQuery){
        DEBUG('s',"***************  Range Query, Start Key:%d , End Key:%d ****************\n",MinKey, MaxKey);

        rc = idx.locate(MinKey, cursor);
        if(rc != 0) goto EXIT;

        printf("|- KEY -|- VALUE -|\n");
        if(cursor.pid == -1)    goto EXIT;
        rc = idx.readForward(cursor, key, rid);
        if(rc != 0)  goto EXIT;
        if(MinKey == key && minEQ){
            // read the tuple
            if ((rc = rf.read(rid, key, value)) < 0) {
              fprintf(stderr, "Error: while reading a tuple from table.\n");
              goto EXIT;
            }
            printf("|  %d\t| %s\n",key, value.c_str());
        }
        rc = idx.readForward(cursor, key, rid);
        if(rc != 0)  goto EXIT;

        while(key < MaxKey  ){
            // read the tuple
            if ((rc = rf.read(rid, key, value)) < 0) {
              fprintf(stderr, "Error: while reading a tuple from table.\n");
              break;
            }
            printf("|  %d\t| %s\n",key, value.c_str());
            if( cursor.pid == -1 ) break;
            rc = idx.readForward(cursor, key, rid);
            if(rc != 0)  break;
        }
        if(MaxKey == key && maxEQ){
            // read the tuple
            if ((rc = rf.read(rid, key, value)) < 0) {
              fprintf(stderr, "Error: while reading a tuple from table.\n");
              goto EXIT;
            }
            printf("|  %d\t| %s\n",key, value.c_str());
        }

        printf("|-----------------|\n\n");
        DEBUG('s',"************End of Range Query***********\n\n");

    }else{//single query
        searchKey = EQKey;
        DEBUG('s',"***************  Search Key:%d ****************\n",searchKey);
        key = searchKey - 1;

        rc = idx.locate(searchKey,cursor);
        if(rc != 0) goto EXIT;

        printf("|- KEY -|- VALUE -|\n");
        if(cursor.pid == -1){
            printf("Not found key:%d in index\n",searchKey);
        }else{
            rc = idx.readForward(cursor, key, rid);
            if(rc != 0) goto EXIT;

            if(searchKey != key ){
                printf("Not found key:%d in index\n",searchKey);
            }else{
                DEBUG('s',"Found key:%d in index, record id:{%d,%d}\n",searchKey, rid.pid, rid.sid);

                // read the tuple
                if ((rc = rf.read(rid, key, value)) < 0) {
                  fprintf(stderr, "Error: while reading a tuple from table.\n");
                  goto EXIT;
                }
                printf("|  %d\t| %s\n",key, value.c_str());
            }
        }

        printf("|-----------------|\n\n");

    }

    rc = 0;
EXIT:
    return rc;

}
RC SqlEngine::selectWithoutIndex(int attr, const string& table, 
        const vector<SelCond>& cond, 
        const RecordFile &rf)
{
    RecordId   rid;  // record cursor for table scanning
    int    count;
    KeyType    key;
    string value;
    int    diff;
    RC rc;
    
    printf("WITHOUT INDEX\n");
    // scan the table file from the beginning
    rid.pid = rid.sid = 0;
    count = 0;
    while (rid < rf.endRid()) {
        // read the tuple
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          return -1;
        }

        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
            // compute the difference between the tuple value and the condition value
            switch (cond[i].attr) {
              case 1:
                diff = key - atoi(cond[i].value);
                break;
              case 2:
                diff = strcmp(value.c_str(), cond[i].value);
                break;
            }

            // skip the tuple if any condition is not met
            switch (cond[i].comp) {
              case SelCond::EQ:
                if (diff != 0) goto next_tuple;
                break;
              case SelCond::NE:
                if (diff == 0) goto next_tuple;
                break;
              case SelCond::GT:
                if (diff <= 0) goto next_tuple;
                break;
              case SelCond::LT:
                if (diff >= 0) goto next_tuple;
                break;
              case SelCond::GE:
                if (diff < 0) goto next_tuple;
                break;
              case SelCond::LE:
                if (diff > 0) goto next_tuple;
                break;
            }
        }

        // the condition is met for the tuple. 
        // increase matching tuple counter
        count++;

        // print the tuple 
        switch (attr) {
            case 1:  // SELECT key
              fprintf(stdout, "%d\n", key);
              break;
            case 2:  // SELECT value
              fprintf(stdout, "%s\n", value.c_str());
              break;
            case 3:  // SELECT *
              fprintf(stdout, "%d '%s'\n", key, value.c_str());
              break;
        }

        // move to the next tuple
        next_tuple:
        ++rid;
    }
    // print matching tuple count if "select count(*)"
    if (attr == 4) {
        fprintf(stdout, "%d\n", count);
    }


}
RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
    RecordFile rf;   // RecordFile containing the table
    BTreeIndex idx;

    RC     rc;
    bool index = true;
    bool hasKeyInCon;
    // open the table file
    if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
        fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
        return rc;
    }
  
    rc = idx.open(table + ".idx", 'r');
    if(rc != 0) {
        cout<<"Do Not Have Index File:"<<table<<".idx"<<endl;
        index = false;
    }
    hasKeyInCon = false;
    for (unsigned i = 0; i < cond.size(); i++) {
        if(cond[i].attr==1) {
            hasKeyInCon = true;
            //break;
        }
        //printf("SelCond attr:%d com:%d value:%s\n",cond[i].attr, cond[i].comp, cond[i].value);
    }
    cout<<endl; 
    if( index && hasKeyInCon ){
        rc = selectWithIndex(attr, table, cond, idx, rf);
    }else{
        rc = selectWithoutIndex(attr, table, cond, rf);
    }
    //rc = 0;

    // close the table file and return
    rf.close();
    idx.close();
    return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
    RC rc = 0;
    RecordFile rf;   // RecordFile containing the table
    RecordId rid;
    IndexCursor cursor;
    string value;
    KeyType key, searchKey, startKey, endKey;
    int     bpagecnt, epagecnt;

    cout<< "LOAD "<<table<<" FROM "<<loadfile;
    if(index)   cout<<" WITH INDEX";
    cout<<endl;
    BTreeIndex idx;
    rc = rf.open( table + ".tbl", 'w');
    if(rc < 0){
        cout<<"error:"<<table<<".tbl"<<endl;
        return rc;
    }
    
    if(index){
        rc = idx.open( table + ".idx", 'w'); 
        if(rc<0) {
            cout<<"error:"<<table<<".idx"<<endl;
            return rc;
        }
    }
 
    ifstream readfile(loadfile.c_str() );
    string line;
    while(getline(readfile,line)) {
        string value;
        parseLoadLine(line,key,value);
        //printf("key:%d value:%s\n",key,value.c_str());
        RecordId rid;
        RC rc;
        rc = rf.append(key,value,rid);
        if(rc < 0)  goto LOAD_EXIT;
        
        if(index){   
            rc = idx.insert(key, rid);  
            if(rc < 0) goto LOAD_EXIT;
        }
    }
    if(DebugIsEnabled('i')) idx.printTree();
    
    /*    
    searchKey = 101;
    printf("***************  Search Key:%d ****************\n",searchKey);
    key = searchKey - 1;
    bpagecnt = PageFile::getPageReadCount();

    rc = idx.locate(searchKey,cursor);
    if(rc != 0) goto LOAD_EXIT;

    if(cursor.pid == -1){
        printf("Not found key:%d in index\n",searchKey);
    }else{
        rc = idx.readForward(cursor, key, rid);
        if(rc != 0) goto LOAD_EXIT;

        if(searchKey != key ){
            printf("Not found key:%d in index\n",searchKey);
        }else{
            printf("Found key:%d in index, record id:{%d,%d}\n",searchKey, rid.pid, rid.sid);

            // read the tuple
            if ((rc = rf.read(rid, key, value)) < 0) {
              fprintf(stderr, "Error: while reading a tuple from table.\n");
              goto LOAD_EXIT;
            }
            printf("RECORD key:%d value:%s\n",key, value.c_str());
        }
    }
    epagecnt = PageFile::getPageReadCount();
    printf("*****************End of Search Key:%d , read %d pages *********\n\n",searchKey, epagecnt-bpagecnt);


    startKey = 80;
    endKey = 90;
    printf("***************  Range Query, Start Key:%d , End Key:%d ****************\n",startKey, endKey);
    bpagecnt = PageFile::getPageReadCount();

    rc = idx.locate(startKey, cursor);
    if(rc != 0) goto end_query;
    if(cursor.pid == -1)    goto end_query;
    rc = idx.readForward(cursor, key, rid);
    if(rc != 0)  goto end_query;

    while(key < endKey && cursor.pid != -1 ){
        // read the tuple
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table.\n");
          break;
        }
        printf("RECORD key:%d value:%s\n",key, value.c_str());

        rc = idx.readForward(cursor, key, rid);
        if(rc != 0)  break;
    }
end_query:
    epagecnt = PageFile::getPageReadCount();
    printf("************End of Range Query, read %d pages ***********\n\n", epagecnt-bpagecnt);
*/
LOAD_EXIT:    
    idx.close();
    rf.close();
    if(rc != 0) cout<<"error"<<endl;
    return rc;
}
struct Range
{
    KeyType min,max;
    Range(){}
    Range(KeyType mi, KeyType ma): min(mi), max(ma){}
};
/*
struct Node
{
    int pid;
    Range range;
    bool isLeaf;
    Node(int p, Range r, bool i): pid(p), range(r), isLeaf{}
};
*/
class IntervalNode
{
private:
public:
    vector<IntervalNode*> heap;
    PageId pid;
    Range range;
    string bossName;
    IntervalNode(int p, Range r, string b): pid(p), range(r),bossName(b)
    {
    }

    bool operator< ( IntervalNode* m ) const
    {
        return (range.max - range.min)  < (m->range.max - m->range.min);
    }
    void print()
    {
        printf("%d(%d, %d)[\"%s\"] ", pid, range.min, range.max, bossName.c_str());
        //printf("%d(%d, %d)[0x%x] ", pid, range.min, range.max, this);
    }
    int printAll()
    {
        print();
        printf("\n\tNeightbours (%d): ",heap.size());
        printNeighbour();
        printf("\n");
        return heap.size();
    }
    
    bool empty()
    {
        return heap.size() == 0;
    }
    void push( IntervalNode *node)
    {
        heap.push_back(node);
        push_heap(heap.begin(), heap.end());
    }
    int del( IntervalNode *node)
    {
        printf("Delete neighbour from ");
        this->print();
        printf("to ");
        node->print();
        printf("\n");
        //printAll();
        printf("heap size:%d\n",heap.size()); 
        vector<IntervalNode*>::iterator it = heap.begin();
        for(; it!= heap.end(); it++)
        {
            if( (*it)->pid == node->pid && (*it)->range.min == node->range.min){
                heap.erase( it );
                break;
            }
        }
        if( !heap.empty() && it == heap.end() )  {
            printf("delete neighbour error\n");
            printAll();
            return -1;
        }else{
        /*    printf("After Delete:");
            printAll();
            */
        }

        make_heap( heap.begin(), heap.end() );
        return 0;
    }

    IntervalNode* pop()
    {
        IntervalNode* node = heap.front();
        pop_heap(heap.begin(), heap.end());
        heap.pop_back();
        return node;
    }
    void printNeighbour()
    {
        vector<IntervalNode*>::iterator it = heap.begin();
        for(; it!=heap.end(); it++){
            (*it)->print();
        }
    }
    IntervalNode* biggestNeighbour()
    {
        IntervalNode* biggest = NULL;
        //find big range in neighbours
        
        if( !this->empty() ){
            biggest = this->heap.front();
        }else{
            printf("Biggest Neighbour error!");
        }

        return biggest;
    }
    int addNeighbour(IntervalNode* to)
    {
        this->push(to);
        return 0;
    }
    int deleteNeighbour(IntervalNode* to)
    {
        this->del( to );
        return 0;
    }

};

static bool overlap(KeyType key, Range r)
{
    if(key >= r.min && key < r.max)
        return true;
    else
        return false;
}


static bool overlap(Range r1, Range r2)
{
    bool result;
    if(r1.min < r2.max && r2.min < r1.max)
        result = true;
    else
        result =  false;
    printf(" overlap( (%d, %d), (%d, %d) )=%d\n", r1.min, r1.max, r2.min, r2.max, result);
    return result;
}


class IntervalList
{
private:
    BTreeIndex * idx;
public:
    list<IntervalNode*> l;
    string name;
    IntervalList(string n, BTreeIndex *i):name(n), idx(i)
    {
    }
    int printList()
    {
        int refNum = 0;
        printf("\n********** Print List : %s   size:%d *************\n",name.c_str(),l.size());
        list<IntervalNode*>::iterator it = l.begin();
        while(it != l.end()){
            refNum += (*it)->printAll();
            it++;
        }
        printf("\n********** Print List End:%d  **********\n\n",refNum);
        return refNum;
    }
    bool empty()
    {
        return l.empty();
    }
    void insert(list<IntervalNode*>::iterator &it, IntervalNode* node  ){
        l.insert(it, node);
    //    it++;
    }
    void push(IntervalNode* node)
    {
        l.push_front(node);
    }
    IntervalNode * front()
    {
        return l.front();
    }
    IntervalNode * pop()
    {
        IntervalNode * node = l.front();
        l.pop_front();
        return node;
    }
    void eraseEmptyNodes(){
        cout<<"Delete empty nodes:"<<endl;
        /*
        list<IntervalNode*>::iterator it1 = l.begin(), it2;
        while(it1 != l.end()) {
            if( (*it1)->empty() ){
                printf("Delete Node:%d(%d, %d) is empty, delete it!\n", 
                        (*it1)->pid, 
                        (*it1)->range.min, 
                        (*it1)->range.max);
                it2 = it1;
                it1++;
                l.erase(it2); 
            }else{
              it1 ++;
            }
        }*/

        while( l.front()->empty() ){
            IntervalNode * node = this->pop();
            printf("Delete Node:%d(%d, %d) is empty, delete it!\n", 
                        node->pid, 
                        node->range.min, 
                        node->range.max);

            delete node;
            node = NULL;
        }
    }

    void erase(IntervalNode * node)
    {
        list<IntervalNode*>::iterator it = l.begin();
        while(it!=l.end()){
            if( (*it)->pid == node->pid ){
                printf("Erase Node:%d(%d, %d) in %s\n", (*it)->pid, (*it)->range.min, (*it)->range.max,name.c_str());
                l.erase( it );
                break;
            }
            it ++;
        }
        if(it == l.end()){
             printf("Erase Node:%d(%d, %d) in %s Failed!\n", node->pid, node->range.min, node->range.max,name.c_str());
        }
    }

    RC openNode( IntervalNode * chosenOne)
    {
        PageId cPid;
        if(chosenOne->bossName.compare( this->name ) != 0){
            printf("ERROR! Boss Error!\n");
            return -1;
        }
        chosenOne->print();
        cout<<endl;
        BTNode bnode;
        RC rc;
        int i;
        //find the position of chosen one
        list<IntervalNode*>::iterator chosenOneIt = l.begin();
        while(chosenOneIt!=l.end()){
            if( (*chosenOneIt)->pid == chosenOne->pid ){
                break;
            }
            chosenOneIt ++;
        }
        if(chosenOneIt == l.end()){
             printf("ERROR: Cannot find the position of chosen one in %s!\n", name.c_str());
             return -1;
        }


        vector<IntervalNode*>::iterator it1 = chosenOne->heap.begin();
        while( it1 != chosenOne->heap.end() ){
            (*it1)->deleteNeighbour( chosenOne );
            it1++;
        }
        rc = idx->getBTNode(chosenOne->pid, bnode);
        if(rc != 0){
            return rc;
        }
        bnode.print();
        printf("New Nodes: \n");
        if(!bnode.isLeaf){
            KeyType min,max;
            min = bnode.minKey;
            for( i=0;i<=bnode.n; i++){
                if( i== bnode.n) max = bnode.maxKey;
                else max = bnode.keys[i];

                if(min > max) {
                    printf("ERROR: Range error(%d, %d)\n",min, max);
                    return -1;
                }
                Range r(min, max);
                IntervalNode * childNode = new IntervalNode(bnode.pids[i], r, this->name);
                childNode->print();
                printf("\n"); 

                vector<IntervalNode*>::iterator it = chosenOne->heap.begin();
                while( it != chosenOne->heap.end() ){
                    if( (*it)->pid == -1 ){
                        if(overlap( (*it)->range.min, childNode->range)){
                            childNode->addNeighbour( (*it) );
                            (*it)->addNeighbour( childNode );
                        }
                    }else{
                        if(overlap( (*it)->range, childNode->range)){
                            childNode->addNeighbour( (*it) );
                            (*it)->addNeighbour( childNode );
                        }
                    }
                    it++;
                }
                min = max;
                if(!childNode->empty()) {
                    this->insert(chosenOneIt, childNode);
                }else{
                    delete childNode;
                }
            }
        }else{
            for( i=0; i<=bnode.n-1;  i++){
                KeyType key =  bnode.keys[i];
                Range r(key, key);
                IntervalNode * childNode = new IntervalNode( -1, r, this->name);
                childNode->print();
                
                vector<IntervalNode*>::iterator it = chosenOne->heap.begin();
                while( it != chosenOne->heap.end() ){
                    if( (*it)->pid == -1){
                        if( key == (*it)->range.min ){
                            childNode->addNeighbour( (*it) );
                            (*it)->addNeighbour( childNode );
                        }
                    }else{
                        if(overlap( key, (*it)->range)){
                            childNode->addNeighbour( (*it) );
                            (*it)->addNeighbour( childNode );
                        }
                    }
                    it++;
                }
                
                if(!childNode->empty()) {
                    this->insert(chosenOneIt, childNode);
                }else{
                    delete childNode;
                }
            }
        }
        printf("\n");
        this->erase( chosenOne );
        delete chosenOne;
        chosenOne = NULL;
        return 0;
    }
};


RC SqlEngine::join(const string& tableR, const string& tableS)
{
    RC rc = 0;
    BTreeIndex idxR;
    BTreeIndex idxS;
    RecordFile rfR;   // RecordFile containing the tableR
    RecordFile rfS;   // RecordFile containing the tableS
    RecordId rid;
    IndexCursor cursor;
    string value;
    KeyType key, searchKey, startKey, endKey;
    int     bpagecnt, epagecnt;
    int i;

    
    cout<< "JOIN "<<tableR<<" AND "<<tableS<<endl;
    rc = rfR.open( tableR + ".tbl", 'w');
    if(rc != 0){
        cout<<"error:"<<tableR<<".tbl"<<endl;
        return rc;
    }
    
    rc = idxR.open( tableR + ".idx", 'w'); 
    if(rc != 0) {
        cout<<"error:"<<tableR<<".idx"<<endl;
        return rc;
    }
    rc = rfS.open( tableS + ".tbl", 'w');
    if(rc != 0){
        cout<<"error:"<<tableS<<".tbl"<<endl;
        return rc;
    }
    
    rc = idxS.open( tableS + ".idx", 'w'); 
    if(rc != 0) {
        cout<<"error:"<<tableS<<".idx"<<endl;
        return rc;
    }
 
    if(DebugIsEnabled('j')) {
        idxR.printTree();
        idxS.printTree();
    }
    IntervalList R(string("R"), &idxR );
    IntervalList S(string("S"), &idxS );

    Range rR(idxR.minKey , idxR.maxKey);
    Range rS(idxS.minKey , idxS.maxKey);
    IntervalNode *n1 = new IntervalNode(idxR.rootPid, rR, R.name );
    IntervalNode *n2 = new IntervalNode(idxS.rootPid, rS, S.name );

    R.push( n1 );
    S.push( n2 );
    n1->addNeighbour( n2);
    n2->addNeighbour( n1);

    int k = 0;
    int refNumR = 0;
    int refNumS = 0;
    while( !R.empty() && !S.empty() ){
        refNumR = R.printList();
        refNumS = S.printList();
        if( refNumR != refNumS ){
            printf("ERROR!!  refNumR:%d refNumS:%d\n",refNumR, refNumS);
            goto EXIT;
        }
        

        printf("-----------------Output:");
        while( R.front()->pid == -1 && S.front()->pid == -1){
            IntervalNode * rn = R.pop();
            IntervalNode * sn = S.pop();
            if( rn->range.min == sn->range.min){
                printf("[%d, %d], ",rn->range.min, rn->range.min);
            }else{
                /*printf("Output error!!!!!!!!!\n");
                R.printList();
                S.printList();

                rc = -1;
                goto EXIT;
                */
            }
            delete rn;
            delete sn;
        }
        printf("\n");
        R.eraseEmptyNodes();
        S.eraseEmptyNodes();

        if( R.empty() || S.empty()) break;

        IntervalNode *left, *chosenOne, *bn;
        if( R.front()->range.min <= S.front()->range.min  && R.front()->pid != -1 ){
            left = R.front();
            bn = left->biggestNeighbour();
            if( (bn->range.max - bn->range.min) > (left->range.max - left->range.min) ){
                printf("\n---------------- Opening Nodes of List S: ");
                rc = S.openNode( bn );
            }else{
                printf("\n---------------- Opening Nodes of List R: ");
                rc = R.openNode( left );
            }
        }else{
            left = S.front();
            bn = left->biggestNeighbour();
            if( (bn->range.max - bn->range.min) > (left->range.max - left->range.min) ){
                printf("\n---------------- Opening Nodes of List R: ");
                rc = R.openNode( bn );
            }else{
                printf("\n---------------- Opening Nodes of List S: ");
                rc = S.openNode( left );
            }
        }
        if(rc != 0)  goto EXIT;


        //if( k++ == 6) break;
    }

EXIT:    
    idxR.close();
    rfR.close();
    idxS.close();
    rfS.close();
    if(rc != 0) cout<<"error"<<endl;
    return rc;
}

RC SqlEngine::normal_join(const string& tableR, const string& tableS)
{
    RC rc = 0;
    BTreeIndex idxR;
    BTreeIndex idxS;
    RecordFile rfR;   // RecordFile containing the tableR
    RecordFile rfS;   // RecordFile containing the tableS
    RecordId rid;
    IndexCursor cursor;
    string value;
    KeyType key, searchKey, startKey, endKey;
    int     bpagecnt, epagecnt;
    int i;

    
    cout<< "JOIN "<<tableR<<" AND "<<tableS<<endl;
    rc = rfR.open( tableR + ".tbl", 'w');
    if(rc != 0){
        cout<<"error:"<<tableR<<".tbl"<<endl;
        return rc;
    }
    
    rc = idxR.open( tableR + ".idx", 'w'); 
    if(rc != 0) {
        cout<<"error:"<<tableR<<".idx"<<endl;
        return rc;
    }
    rc = rfS.open( tableS + ".tbl", 'w');
    if(rc != 0){
        cout<<"error:"<<tableS<<".tbl"<<endl;
        return rc;
    }
    
    rc = idxS.open( tableS + ".idx", 'w'); 
    if(rc != 0) {
        cout<<"error:"<<tableS<<".idx"<<endl;
        return rc;
    }
    if(DebugIsEnabled('j')) {
        idxR.printTree();
        idxS.printTree();
    }
    IndexCursor cursorR(idxR.rootPid, 0);
    IndexCursor cursorS(idxS.rootPid, 0);
    KeyType keyR,keyS;
    KeyType rfkeyR,rfkeyS;
    RecordId recR, recS;
    string valueR,valueS;
    int valueRint, valueSint;
    idxR.getFirstKey( cursorR );
    idxS.getFirstKey( cursorS );
    idxR.readForward( cursorR, keyR, recR );
    idxS.readForward( cursorS, keyS, recS );
    rfR.read(recR, rfkeyR, valueR);
    rfS.read(recS, rfkeyS, valueS);

    while( cursorR.pid != -1  && cursorS.pid != -1){
        if( keyR == keyS ){
            printf("Node R: (%d, \"%s\")\n",keyR, valueR.c_str());
            printf("Node S: (%d, \"%s\")\n\n",keyS, valueS.c_str());
            idxR.readForward( cursorR, keyR, recR );
            idxS.readForward( cursorS, keyS, recS );
            rfR.read(recR, rfkeyR, valueR);
            rfS.read(recS, rfkeyS, valueS);
        }else if( keyR < keyS ){
            idxR.readForward( cursorR, keyR, recR );
            rfR.read(recR, rfkeyR, valueR);
        }else{
            idxS.readForward( cursorS, keyS, recS );
            rfS.read(recS, rfkeyS, valueS);
        }
    }

EXIT:    
    idxR.close();
    rfR.close();
    idxS.close();
    rfS.close();
    if(rc != 0) cout<<"error"<<endl;
    return rc;
}


RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
