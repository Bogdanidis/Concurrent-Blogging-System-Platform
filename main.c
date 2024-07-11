#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <limits.h>

typedef struct ListNode {
    int postID;
    pthread_mutex_t lock;
    int marked;
    struct ListNode *next;
}LIST_NODE;

typedef struct SinglyLinkedList {
    struct ListNode *head;
}LIST;

typedef struct queueNode {
    int postID;
    struct queueNode *next;
}QUEUE_NODE;

typedef struct queue {
    struct queueNode *Head;
    struct queueNode *Tail;
    pthread_mutex_t headLock;
    pthread_mutex_t tailLock;
}QUEUE;

typedef struct treeNode {
    int lock_count;
    int postID;
    struct treeNode *lc;
    struct treeNode *rc;
    int IsRightThreaded;
    int IsLeftThreaded;
    pthread_mutex_t lock;
}TREE_NODE;

TREE_NODE *global_root_head;
LIST *global_list;
QUEUE **Categories;
int M,N;
int global_tree_count;
pthread_barrier_t   barrier_1st_phase_end;
pthread_barrier_t   barrier_2nd_phase_start;
pthread_barrier_t   barrier_2nd_phase_end;
pthread_barrier_t   barrier_3rd_phase_start;
pthread_barrier_t   barrier_3rd_phase_end;
pthread_barrier_t   barrier_4th_phase_start;
pthread_barrier_t   barrier_4th_phase_end;



void enqueue(int postID,struct queue* target_queue) {
    QUEUE_NODE *new_node = malloc(sizeof(struct queueNode));
    new_node->postID = postID;
    new_node->next = NULL;
    pthread_mutex_lock(&(target_queue->tailLock));
    target_queue->Tail->next = new_node;
    target_queue->Tail = new_node;
    pthread_mutex_unlock(&(target_queue->tailLock));
}

int dequeue(struct queue* target_queue) {
    int result;
    pthread_mutex_lock(&(target_queue->headLock));
    if (target_queue->Head->next == NULL)
        result = -1;//empty queue
    else {
        result = target_queue->Head->next->postID;
        target_queue->Head = target_queue->Head->next;
    }
    pthread_mutex_unlock(&(target_queue->headLock));
    return result;
}

int lazySearch(int postID ) {
   // printf("Lazy seearch ID: %d\n",postID);
    LIST_NODE* curr=global_list->head;
    while (curr->postID < postID) curr = curr->next;
    if (curr->marked ==0 && postID==curr->postID) return 1;
    else return 0;
}

int validate(LIST_NODE* pred, LIST_NODE* curr) {
    if (curr==NULL) return 1;
    if ( pred->marked == 0 && curr->marked == 0 && pred->next == curr) return 1;
    else return 0;
}

int lazyInsert(int postID) {

    //printf("Insert postID: %d\n",postID);
    LIST_NODE* new_node=malloc(sizeof(struct ListNode));
    new_node->postID=postID;
    pthread_mutex_init(&(new_node->lock),NULL);
    new_node->marked=0;
    new_node->next=NULL;
    LIST_NODE* pred, *curr;
    int result;
    int return_flag = 0;
    while (1) {      
        pred = global_list->head;
        curr = pred->next;
        while (curr && curr->postID < postID) {
            pred = curr;
            curr = curr->next;
        }
        if (pred) pthread_mutex_lock(&(pred->lock));
        if (curr) pthread_mutex_lock(&(curr->lock));
        if (validate(pred, curr) == 1) {
            if (curr && postID == curr->postID) {
            result = 0; 
            return_flag = 1;
            }
            else {
                new_node->next=curr;
                pred->next = new_node;
                result = 1; 
                return_flag = 1;
            }
        }
        if (pred) pthread_mutex_unlock(&(pred->lock));
        if (curr) pthread_mutex_unlock(&(curr->lock));
        if (return_flag) return result;
    }
}

int lazyDelete(int postID) {  
    //printf("Lazy delete ID: %d\n",postID);
    LIST_NODE* pred, *curr;
    int result;
    int return_flag = 0;
    while (1) {
        pred = global_list->head;
        curr = pred->next;
        while (curr&&curr->postID < postID) {
            pred = curr;
            curr = curr->next;
        }
        if (pred) pthread_mutex_lock(&(pred->lock));
        if (curr) pthread_mutex_lock(&(curr->lock));
        if (validate(pred, curr)==1) {
            if (postID == curr->postID) {
                curr->marked = 1;
                pred->next = curr->next;
                result = 1;
            }
            else result = 0;

            return_flag = 1;
        }
        if (pred) pthread_mutex_unlock(&(pred->lock));
        if (curr) pthread_mutex_unlock(&(curr->lock));
        if (return_flag ) return result;
    }
} 

void Count_total_list_size(int expected){
    //printf("Count_total_list_size\n");
    int count=0;
    LIST_NODE* temp =global_list->head->next;
    while (temp){
        count++;
        temp=temp->next;
    }
    printf("Total list size counted. (expected: %d, found: %d)\n",expected,count);

    if(expected!=count){
        printf("ERROR: False total list size\nEXITING\n");
        exit(-1);
    } 
}

void Count_total_list_keysum(){
   // printf("Count_total_list_keysum\n");
    int sum=0;
    LIST_NODE* temp =global_list->head->next;
    while (temp){
        sum+=temp->postID;
        temp=temp->next;
    }
    printf("Total list keysum counted. (expected: %d, found: %d)\n",2*N*N*N*N-N*N,sum);
    if(2*N*N*N*N-N*N!=sum){
        printf("ERROR: False total list keysum\nEXITING\n");
        exit(-1);
    }
}

void Count_total_queue_sizes(int expected){
    int i;
   for(i=0;i<(N/4);i++){
        int count=0;
        QUEUE_NODE* temp=Categories[i]->Head->next;
        while (temp){
            temp=temp->next;
            count++;
        }
        printf("Categories[%d] queue's total size counted. (expected: %d, found: %d)\n",i,expected*N,count);

        if(expected*N!=count){
            printf("ERROR: False %d queue's size\nEXITING\n",i);
            exit(-1);
        } 
    }

   
}

void print_queues(){
    int i;
    for(i=0;i<(N/4);i++){
        QUEUE_NODE* temp=Categories[i]->Head->next;
        printf("queue %d: \n",i);
        while (temp){
            printf("%d ",temp->postID);
            temp=temp->next;          
        }
        printf("\n");
    }
}

void Count_total_queue_keysum(){
    int sum=0;
    int i;
    for(i=0;i<(N/4);i++){
        QUEUE_NODE* temp=Categories[i]->Head->next;
        while (temp){
            sum+=temp->postID;
            temp=temp->next;          
        }
    }
    printf("Total queues keysum check counted. (expected: %d, found: %d)\n",2*N*N*N*N-N*N,sum);

    if(2*N*N*N*N-N*N!=sum){
        printf("ERROR: False  queue's checksum\nEXITING\n");
        exit(-1);
    } 

}

void lock(struct treeNode* ptr){
    //ptr->lock_count++;
    if (!ptr->lock_count) ;
    else pthread_mutex_lock(&(ptr->lock));
   
}

void unlock(struct treeNode* ptr){ 
    //ptr->lock_count--;
   if (!ptr->lock_count) ;
    else pthread_mutex_unlock(&(ptr->lock));
    
}

void threaded_tree_insert(int postID) { 
    //printf("threaded_tree_insert %d\n",postID);
    TREE_NODE *curr = global_root_head->rc; 
    TREE_NODE *parent = global_root_head; // Parent of key to be inserted 
    if (parent) lock(parent);
    if (curr) lock(curr);

    //search
    while (curr != NULL) 
    {    
 
        if (parent) unlock(parent);
        parent = curr; // Update parent pointer 
  
        // Moving on lc subtree. 
        if (postID < curr->postID) 
        { 
            if (curr -> IsLeftThreaded == 0) 
                curr = curr -> lc; 
            else
                break; 
        } 
  
        // Moving on rc subtree. 
        else
        { 
            if (curr->IsRightThreaded == 0) 
                curr = curr -> rc; 
            else
                break; 
        } 

     if (curr) lock(curr);

    } 
  
    // Create a new node 
    TREE_NODE *new_node = malloc(sizeof(struct treeNode)); 
    new_node -> postID = postID; 
    new_node -> IsLeftThreaded = 1; 
    pthread_mutex_init(&(new_node->lock),NULL);
    new_node->lock_count=1;
    new_node -> IsRightThreaded = 1; 

    if (parent == global_root_head) 
    { 
        global_root_head->rc = new_node; 
        new_node -> lc = NULL; 
        new_node -> rc = NULL; 
    } 
    else if (postID < (parent -> postID)) 
    { 
        new_node -> lc = parent -> lc; 
        new_node -> rc = parent; 
        parent -> IsLeftThreaded = 0; 
        parent -> lc = new_node; 
    } 
    else
    { 
        new_node -> lc = parent; 
        new_node -> rc = parent -> rc; 
        parent -> IsRightThreaded = 0; 
        parent -> rc = new_node; 
    } 
   if (curr) unlock(curr);
   if (parent&&(parent!=curr)) unlock(parent);
    
} 

void caseA(struct treeNode* parent,struct treeNode* curr){
    // If Node to be deleted is root
    //printf("caseA\n");
    if (parent == global_root_head)
        global_root_head->rc = NULL;
    
    // If Node to be deleted is lc
    // of its parent
    else if (curr == parent->lc) {
        parent->IsLeftThreaded = 1;
        parent->lc = curr->lc;
    }
    else {
        parent->IsRightThreaded = 1;
        parent->rc = curr->rc;
    }

}

struct treeNode* inSucc(struct treeNode* curr){
    //printf("inSucc\n");
    TREE_NODE* inorderSucc;
    if (curr->IsRightThreaded == 1){
        inorderSucc=curr->rc;
    }else{
            curr = curr->rc;
            //if (curr) lock(curr);
            while (curr->IsLeftThreaded == 0){
               //if (curr) unlock(curr);
                curr = curr->lc;
               //if (curr) lock(curr);
            }
        //if (curr) unlock(curr);
        inorderSucc= curr;
    }
    return inorderSucc;
}

struct treeNode* inPred(struct treeNode* curr){
    //printf("inPred\n");
    TREE_NODE* inorderPred;
        if (curr->IsLeftThreaded == 1){
            inorderPred=curr->lc;       
        }else{

            curr = curr->lc;
            //if (curr) lock(curr);

            while (curr->IsRightThreaded == 0){
             //if (curr) unlock(curr);
                curr = curr->rc;
             //if (curr) lock(curr);
            }
            //if (curr) unlock(curr);
            inorderPred=curr;
        }
        return inorderPred;
}

void  caseB(struct treeNode* parent,struct treeNode* curr){
    //printf("caseB\n");
    TREE_NODE* child;
 
    if (curr->IsLeftThreaded == 0)
        child = curr->lc;
 
    // Node to be deleted has rc child.
    else
        child = curr->rc;
 
    if (child) lock(child);

    // Node to be deleted is root Node.
    if (parent == global_root_head)
        global_root_head->rc = child;
 
    // Node is lc child of its parent.
    else if (curr == parent->lc)
        parent->lc = child;
    else
        parent->rc = child;
 
   if (child) unlock(child);

    TREE_NODE* inorderPred=inPred(curr);
    TREE_NODE* inorderSucc=inSucc(curr);

    if (inorderSucc) lock(inorderSucc);
    if (inorderPred) lock(inorderPred);
    // If curr has lc subtree.
    if (curr->IsLeftThreaded == 0)
        inorderPred->rc = inorderSucc;
 
    // If curr has rc subtree.
    else {
        if (curr->IsRightThreaded == 0)
            inorderSucc->lc = inorderPred;
    }
    if (inorderSucc) unlock(inorderSucc);
    if (inorderPred) unlock(inorderPred);


}

void  caseC(struct treeNode* parent,struct treeNode* curr){
   // printf("caseC\n");
    // Find inorder successor and its parent.
    TREE_NODE* parsucc = curr;
    TREE_NODE* succ = curr->rc;
    if (succ) lock(succ);

    // Find leftmost child of successor
    while (succ->lc != NULL) {
       if (parsucc&&(parsucc!=curr)) unlock(parsucc);
        parsucc = succ;
        succ = succ->lc;
       if (succ) lock(succ);
    }
    curr->postID = succ->postID;
 
    if (succ->IsLeftThreaded == 1 && succ->IsRightThreaded == 1)
        caseA(parsucc, succ);
    else
        caseB(parsucc, succ);

    if (parsucc&&(parsucc!=curr)) lock(parsucc);
    if (succ) lock(succ);

}

void threaded_tree_delete(int postID){   
    //printf("threaded_tree_delete %d\n",postID);
    int found = 0;
    TREE_NODE *parent = global_root_head, *curr = global_root_head->rc;
    if (parent) lock(parent);
    if (curr) lock(curr);
    
 
    // Search 
    while (curr != NULL) {
        if (postID == curr->postID) {
            found = 1;
            break;
        }
        if (parent) unlock(parent);
        parent = curr;
        if (postID < curr->postID) {
            if (curr->IsLeftThreaded == 0)
                curr = curr->lc;
            else
                break;
        }
        else {
            if (curr->IsRightThreaded == 0)
                curr = curr->rc;
            else
                break;
        }
        if (curr) lock(curr);
    }

     if (found==0)  {
        if (parent) unlock(parent);
        if (curr&&(parent!=curr)) unlock(curr);
        return;
    }
    
    if (curr->IsLeftThreaded == 0 && curr->IsRightThreaded == 0)
        caseC(parent, curr);
 
    // Only Left Child
    else if (curr->IsLeftThreaded == 0)
        caseB(parent, curr);
 
    // Only Right Child
    else if (curr->IsRightThreaded == 0)
        caseB(parent, curr);
 
    // No child
    else
        caseA(parent, curr);
 
    if (parent) unlock(parent);
    if (curr&&(parent!=curr)) unlock(curr);
   
}

void print_tree_inorder(struct treeNode *root){
    if (root == NULL){
        printf("Tree is empty\n");
        return;}
    // Reach leftmost Node
    struct treeNode * ptr = root;
    while (ptr->IsLeftThreaded == 0)
        ptr = ptr->lc;
    printf("[%d:%d] ", global_root_head->postID,global_root_head->lock_count);
    // One by one print successors
    while (ptr != NULL) {
        printf("[%d:%d] ", ptr->postID,ptr->lock_count);
        ptr = inSucc(ptr);
    }
    printf("\n");
}

void Count_total_tree_size(struct treeNode *root,int expected){
    if (root == NULL) return;
    // Reach leftmost Node
    struct treeNode * ptr = root;
    while (ptr->IsLeftThreaded == 0)
        ptr = ptr->lc;
 
    // One by one print successors
    while (ptr != NULL) {
        global_tree_count++;
        ptr = inSucc(ptr);
    }
    printf("Tree’s total size finished. (expected: %d, found: %d)\n",expected,global_tree_count);

    if(expected!=global_tree_count){
        printf("ERROR: False  tree's size\nEXITING\n");
        exit(-1);
    } 
}

int tree_search(struct treeNode *root,int postID){ 
   // printf("Tree_search\n");
    if (root == NULL){
        printf("Tree is empty\n");
        return 0;}
    // Reach leftmost Node
    struct treeNode * ptr = root;
    while (ptr->IsLeftThreaded == 0)
        ptr = ptr->lc;
    // One by one print successors
    while (ptr != NULL) {
        if (postID==ptr->postID) return 1;
        ptr = inSucc(ptr);
    }
    return 0;
}

void* thread_function(void* data){
    //printf("poster: %d\n",*(int*)data);
    int i;
   for(i = 0; i<N; i++) { 
       //printf("poster: %d inserting: %d\n",*(int*)data,*(int*)data+i*M);
       lazyInsert(*(int*)data+i*M);
   }

   pthread_barrier_wait(&barrier_1st_phase_end);
  // printf("Eksw apo barrier_1st_phase_end\n");
   if(*(int*)data==0){
       Count_total_list_size(2*N*N);
       Count_total_list_keysum();
       printf("\n<--------END OF PHASE A--------->\n\n");
   }
   pthread_barrier_wait(&barrier_2nd_phase_start);
   //printf("Eksw apo barrier_2nd_phase_start\n");
   if(*(int*)data<N){
       int i;
        for(i = 0; i<N; i++) { 
            if( lazySearch(*(int*)data+i*M)){              
                lazyDelete(*(int*)data+i*M);
                enqueue(*(int*)data+i*M,Categories[(*(int*)data)%(N/4)]);
            }else printf("LazySearch for id %d failed",*(int*)data+i*M);

            if( lazySearch(*(int*)data+i*M+N)){
                lazyDelete(*(int*)data+i*M+N);
                enqueue(*(int*)data+i*M+N,Categories[(*(int*)data)%(N/4)]);
            }else printf("LazySearch for id %d failed",*(int*)data+i*M+N);
        }
   }
   pthread_barrier_wait(&barrier_2nd_phase_end);
   //printf("Eksw apo barrier_2nd_phase_end\n");

   if(*(int*)data==0){
       Count_total_queue_sizes(8);
       Count_total_queue_keysum();
       Count_total_list_size(0);  
       //print_queues();   
       printf("\n<--------END OF PHASE B--------->\n\n");
   }
   pthread_barrier_wait(&barrier_3rd_phase_start);
   //printf("Eksw apo barrier_3rd_phase_start\n");

   if(*(int*)data>=N){
       int i;
        for(i = 0; i<N; i++) { 
            int postID=dequeue(Categories[(*(int*)data)%(N/4)]);
            //printf("dequeue  id %d from Categories[%d]\n",postID,(*(int*)data)%(N/4));
            if(postID==-1){
                printf("Dequeue from categories[%d] failed.\nEXITING\n",(*(int*)data)%(N/4));
                exit(-1);
            }
            //sleep(0.001);
            threaded_tree_insert(postID);
            
        }
   }
   pthread_barrier_wait(&barrier_3rd_phase_end);
   //printf("Eksw apo barrier_3rd_phase_end\n");

    if(*(int*)data==M-1){
       //printf("Tree's inorder: ");
       Count_total_tree_size(global_root_head->rc,N*N);
       Count_total_queue_sizes(4);
       printf("\n<--------END OF PHASE C--------->\n\n");
       //print_queues();     
   }
   pthread_barrier_wait(&barrier_4th_phase_start);
   //printf("Eksw apo barrier_4th_phase_start\n");
   if(*(int*)data<N){
       int i;
       for(i = 0; i<N; i++) { 
            //print_tree_inorder(global_root_head->rc);
           if (tree_search(global_root_head->rc,*(int*)data+i*M)){
              enqueue(*(int*)data+i*M,Categories[(*(int*)data)%(N/4)]);
              //threaded_tree_delete(*(int*)data+i*M);
           }else{
               printf("postID: %d not present in tree.\n",*(int*)data+i*M);
           }
            //print_tree_inorder(global_root_head->rc);
           if (tree_search(global_root_head->rc,*(int*)data+i*M+N)){
              enqueue(*(int*)data+i*M+N,Categories[(*(int*)data)%(N/4)]);
               //threaded_tree_delete(*(int*)data+i*M+N);
           }else{
               printf("postID: %d not present in tree.\n",*(int*)data+i*M+N);
           }
           
       }
 }
   pthread_barrier_wait(&barrier_4th_phase_end);
    if(*(int*)data==0){
         global_tree_count=0;
         global_root_head->rc=NULL;
         Count_total_tree_size(global_root_head->rc,0);
         Count_total_queue_sizes(8);
         Count_total_queue_keysum();
         printf("\n<--------END OF PHASE D--------->\n\n");
   }    
}

int main(int argc, char **argv){

    N=atoi(argv[1]);
    M=2*N;
    global_tree_count=0;
    //A fash
    global_list=malloc(sizeof(struct SinglyLinkedList));
    global_list->head=malloc(sizeof(struct ListNode));
    global_list->head->marked=0;
    global_list->head->next=NULL;
    global_list->head->postID=-1;
    pthread_mutex_init(&(global_list->head->lock),NULL);

    //B fash
    Categories=malloc(sizeof(struct queue *)*(N/4));
    int i;
    for(i=0;i<N/4;i++){
        Categories[i]=malloc(sizeof(struct queue));
        /*ta ksekiname na deixnoun sto idio*/
        Categories[i]->Head=Categories[i]->Tail=malloc(sizeof(struct queueNode));
        pthread_mutex_init(&(Categories[i]->headLock),NULL);
        pthread_mutex_init(&(Categories[i]->tailLock),NULL);
        Categories[i]->Head->next=NULL;
        Categories[i]->Head->postID=-1;
        //printf("%d\n",i);
    }

    global_root_head=malloc(sizeof(struct treeNode));
    global_root_head->postID=-1;
    global_root_head->lc=NULL;
    global_root_head->rc=NULL;
    global_root_head->IsLeftThreaded=0;
    global_root_head->IsRightThreaded=0;
    global_root_head->lock_count=1;
    pthread_mutex_init(&(global_root_head->lock),NULL);


    pthread_t threads[M];
    
    pthread_barrier_init (&barrier_1st_phase_end, NULL, M);
    pthread_barrier_init (&barrier_2nd_phase_start, NULL, M);
    pthread_barrier_init (&barrier_2nd_phase_end, NULL, M);
    pthread_barrier_init (&barrier_3rd_phase_start, NULL, M);
    pthread_barrier_init (&barrier_3rd_phase_end, NULL, M);
    pthread_barrier_init (&barrier_4th_phase_start, NULL, M);
    pthread_barrier_init (&barrier_4th_phase_end, NULL, M);
    int j;
	for(j = 0; j<M; j++) { 
        int* int_pointer=malloc(sizeof(int));
        *int_pointer=j;
        pthread_create (&(threads[j]), NULL, thread_function, (void*)int_pointer);	
    
    }

	for(i = 0; i<M; i++)   
		pthread_join((threads[i]), NULL);
   
    printf("RUN COMPLETED\nEXITING\n");
    return 0;

}