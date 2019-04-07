#include<stdio.h>
#include<limits.h>
#define HEAP_CAPACITY 20
typedef struct s_message{
    int msg;
    int seq_no;
    int id;
}message;

typedef struct s_heap{
    message msg;
    int rts;
}heap;
//creates waiting waiting_queue
int heap_size=0;
heap waiting_queue[HEAP_CAPACITY];
//for heap operations
int parent(int i) { return (i-1)/2; }
int left(int i) { return (2*i + 1); }
int right(int i) { return (2*i + 2); }
void swap(heap *x, heap *y)
{
    heap temp = *x;
    *x = *y;
    *y = temp;
}
// Inserts a new key 'k'
void printHeap(){
    for(int i=0;i<heap_size;++i){
        printf("seq_no=%d, id=%d, msg=%d\n",waiting_queue[i].msg.seq_no,waiting_queue[i].msg.id,waiting_queue[i].msg.msg);
    }
}
void insertKey(message m)
{
    if (heap_size == HEAP_CAPACITY)
    {
        printf("\nOverflow: Could not insertKey\n");
    }

    // First insert the new key at the end
    heap_size++;
    int i = heap_size - 1;
    waiting_queue[i].msg = m;
    waiting_queue[i].rts = m.seq_no;
    // Fix the min heap property if it is violated
    while (i != 0 && (waiting_queue[parent(i)].rts > waiting_queue[i].rts || (waiting_queue[parent(i)].rts == waiting_queue[i].rts && waiting_queue[parent(i)].msg.id > waiting_queue[i].msg.id)))
    {
       swap(&waiting_queue[i], &waiting_queue[parent(i)]);
       i = parent(i);
    }
}

message extractMin()
{
    if (heap_size <= 0)
        printf("Trying to extractMin from empty waiting_queue\n");
    if (heap_size == 1)
    {
        heap_size--;
        return waiting_queue[0].msg;
    }
    // Store the minimum value, and remove it from heap
    message root = waiting_queue[0].msg;
    waiting_queue[0] = waiting_queue[heap_size-1];
    heap_size--;
    MinHeapify(0);
    return root;
}

void MinHeapify(int i)
{
    int l = left(i);
    int r = right(i);
    int smallest = i;
    if (l < heap_size && waiting_queue[l].rts < waiting_queue[i].rts)
        smallest = l;
    if(l < heap_size && waiting_queue[l].rts == waiting_queue[i].rts && waiting_queue[i].msg.id > waiting_queue[l].msg.id)
        smallest=l;
    if(r < heap_size && waiting_queue[r].rts == waiting_queue[smallest].rts && waiting_queue[smallest].msg.id > waiting_queue[r].msg.id)
        smallest = r;
    if (r < heap_size && waiting_queue[r].rts < waiting_queue[smallest].rts)
        smallest = r;
    if (smallest != i)
    {
        swap(&waiting_queue[i], &waiting_queue[smallest]);
        MinHeapify(smallest);
    }
}
int main(){
    message m,del;
    int choice,a,b,c;
    while(1){
        printf("1.insert 2.extractmin\n enter : ");
        scanf("%d",&choice);
        switch(choice){
            case 1:
                printf("Enter msg,seq_no,id : ");
                scanf("%d%d%d",&m.msg,&m.seq_no,&m.id);
                insertKey(m);
                break;
            case 2:
                del=extractMin();
                printHeap();
                printf("removed seq_no = %d,  msg=%d, id=%d\n",del.seq_no,del.msg,del.id);
                break;
        }
    }
}
