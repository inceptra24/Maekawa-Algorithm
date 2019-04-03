#define DEBUG 1 // set  to 0 to hide debug messages
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stddef.h>
#include <omp.h>
#include <time.h>
#include <unistd.h>
//For printing debug messages inside printd() if DEBUG is set to 1
#define printd(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)

enum Message {REQUEST,      //request to enter CS
              YES,          //voting for the request
              RELEASE,      // message sent by i to every one in district once it comes out of its CS
              INQUIRE,      // message from i to candidate to check whether candidate has entered its CS.
              RELINQUISH    //candidate relinquishes the vote obtained by it in favour of a candidate with lower timestamp to avoid deadlock
              };

typedef struct message_s{
    //enum Message msg;
    int msg;
    int seq_no;
    int id;
}message;

MPI_Datatype mpi_message;
int printRecvMessage(message m,int world_rank){
    char *msg_texts[]={"REQUEST","YES","RELEASE","INQUIRE", "RELINQUISH"};
    printf("Received Message %d(%s) at %d from %d, seq= %d\n",m.msg,msg_texts[m.msg],world_rank,m.id,m.seq_no);
    return 0;
}
int printSentMessage(message m,int rank){
    char *msg_texts[]={"REQUEST","YES","RELEASE","INQUIRE", "RELINQUISH"};
    printf("Sent Message %d(%s) from %d to %d, seq= %d\n",m.msg,msg_texts[m.msg],m.id,rank,m.seq_no);
    return 0;
}
int createVotingDistricts(char *filename,int n,int **vd){
    printd("DEBUG : Inside createVotingDistricts()");
    int c,row=0,col=0,nodes,k,temp=0;
    //vd=(int *) malloc (sizeof(int) * n*(n-1))
    FILE *file;
    file = fopen(filename, "r");
    if (file) {
        printd("DEBUG : voting district configuration file opened.\n");
        while ((c = getc(file)) != '\n'){
            switch(c){
                case 'x':
                case 'X':
                    nodes=temp;
                    temp=0;
                    break;
                default:
                    temp*=10;
                    temp+=(c-'0');
            }
        }
        k=temp;
        printf("Number of nodes = %d\nNumber of nodes in a voting district = %d",nodes,k);
        while ((c = getc(file)) != EOF){
            //putchar(c);
            switch(c){
                case '\n':
                    row++;
                    col=0;
                    break;
                case ' ':
                    col++;
                    break;
                default:
                    vd[row][col]=c-'0';
                    //printf("%d\n",vd[row][col]);
            }
        }
        fclose(file);
    }
    return 0 ;
}
int enterCS(int world_rank,int *seq,int k,MPI_Datatype mpi_message,int *voting_district){
    int yes_votes=0,      // # of processes voted yes to i
        sender_rank
        ;
    (*seq)++;
    //Broadcast (REQUEST, Ts, i) in voting_district
    message send_message,recv_message;
    MPI_Status status;
    send_message.msg=REQUEST;
    send_message.seq_no=*seq;
    send_message.id=world_rank;
    for(int i=0;i<k;++i){
        if(voting_district[i]!=world_rank){
            MPI_Send(&send_message,1,mpi_message,voting_district[i],1,MPI_COMM_WORLD);
            printSentMessage(send_message,voting_district[i]);
        }
        else
            yes_votes++; //Own vote
    }
    while (yes_votes<k) {
        //receive message
        MPI_Recv(&recv_message,1,mpi_message,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&status);
        printf("Entry Section Testpoint 1");
        printRecvMessage(recv_message,world_rank);
        //process message
        sender_rank=status.MPI_SOURCE;
        switch(recv_message.msg){
            case YES:
                yes_votes++;
                break;
            case INQUIRE:
                //send RELINQUISH message to sender
                send_message.msg=RELINQUISH;
                MPI_Send(&send_message,1,mpi_message,sender_rank,1,MPI_COMM_WORLD);
                printSentMessage(send_message,sender_rank);
                yes_votes--;
                break;
            default:
                printf("WARNING - THIS WARNING IS UNEXPECTED! inside enterCS\n");
                printf("Got Message with message=%d, seq_no=%d, world_rank=%d at node %d\n",
                        recv_message.msg,recv_message.seq_no,recv_message.id,world_rank);
        }
    }
    printf("%d reached end of Entry Section",world_rank);
    return 0;
}
int criticalSection(){
    printf("*************************\n");
    printf("Critical Section Executed\n");
    printf("*************************\n");
    return 0;
}
int exitCS(int world_rank,int k,MPI_Datatype mpi_message,int *voting_district){
    message send_message;
    send_message.msg=RELEASE;
    send_message.seq_no=-1;
    send_message.id=world_rank;
    //for (∀r ∈ Si), Send(RELEASE, i) to r
    for(int i=0;i<k;++i){
        if(voting_district[i]!=world_rank){
            MPI_Send(&send_message,1,mpi_message,voting_district[i],1,MPI_COMM_WORLD);
            printSentMessage(send_message,voting_district[i]);
        }
    }
    return 0;
}
int compare(const void *s1, const void *s2){
    message *m1 = (message *)s1;
    message *m2 = (message *)s2;
    int compared=(m1->seq_no - m2->seq_no);
    if(compared)
        return compared;
    return m1->id - m2->id;
}//debug - voting district changed to comm_district
int messageHandlingSection(int world_rank,int *seq,int k,MPI_Datatype mpi_message,int *voting_district){
    //MPI_Comm voting_district=comm_district[world_rank];
    message send_message, //For sending of messages
            recv_message, //For receiving messages
            waiting_queue[20], //Weighting queue for messages
            min_message     //Message with minimum timestamp
            ;
    MPI_Status status;
    int local_rank_lookup[k],   //for world rank to district rank conversion
        top=0 ,              //for waiting queue
        voted_candidate=-1,//the candidate for whom the node is voted
        candidate_seq=-1, //Sequence number(time stamp) of Candidate for which the node has voted
        sender_rank=-1 //For storing sender's rank(of received message)
        ;
    bool have_voted=false ,     //true, if i has already voted for a candidate process
         have_inquired=false  //true, if i has tried to recall a voting (initially it is false)
        ;
    /*/test
    send_message.msg=YES;
    send_message.seq_no=recv_message.seq_no;
    send_message.id=world_rank;
    MPI_Send(&send_message,1,mpi_message,0,0,voting_district);
    printSentMessage(send_message,0);
    //test_end*/
    //receive message
    while(1){
        MPI_Recv(&recv_message,1,mpi_message,MPI_ANY_SOURCE,1,MPI_COMM_WORLD,&status);
        printd("TEST POINT MHS1\n");
        printRecvMessage(recv_message,world_rank);
        //process message
        sender_rank=status.MPI_SOURCE;

        switch(recv_message.msg){
            case REQUEST:
                if(!have_voted){
                    //Send (YES,i) to sender
                    send_message.msg=YES;
                    send_message.seq_no=recv_message.seq_no;
                    send_message.id=world_rank;
                    MPI_Send(&send_message,1,mpi_message,sender_rank,0,MPI_COMM_WORLD);//voting_district);
                    printSentMessage(send_message,sender_rank);
                    voted_candidate=sender_rank;
                    candidate_seq=recv_message.seq_no;
                    have_voted=true;
                }
                else{
                    waiting_queue[top++]=recv_message;
                    printf("Message from %d added to waiting_queue of %d",recv_message.id,world_rank);
                    //Add the rank of the sender in local_rank_lookup table
                    local_rank_lookup[recv_message.id]=sender_rank;
                    if((recv_message.seq_no<candidate_seq)&& !have_inquired){
                        // Send(INQUIRE,i, Candidate_TS) to Candidate
                        send_message.msg=INQUIRE;
                        send_message.seq_no=recv_message.seq_no;
                        send_message.id=world_rank;
                        MPI_Send(&send_message,1,mpi_message,sender_rank,0,MPI_COMM_WORLD);
                        printSentMessage(send_message,sender_rank);
                        have_inquired=true;
                    }
                }
                break;
            case RELINQUISH:
                waiting_queue[top++]=recv_message;
                printf("Message from %d added to waiting_queue of %d",recv_message.id,world_rank);
                //Sort the waiting waiting_queue
                qsort(waiting_queue,top,sizeof(message), compare);
                //Remove(s,rts) from Waiting_Q such that rts is minimum
                min_message=waiting_queue[top--];
                //Send(YES,i) to s
                send_message.msg=YES;
                send_message.seq_no=recv_message.seq_no;
                send_message.id=world_rank;
                MPI_Send(&send_message,1,mpi_message,sender_rank,0,MPI_COMM_WORLD);
                printSentMessage(send_message,sender_rank);
                //candidate:=s.
                voted_candidate=local_rank_lookup[min_message.id];
                //candidate_TS:=RTS
                candidate_seq=min_message.seq_no;
                have_inquired=false;

            case RELEASE:
                // If (Waiting_Q is not empty)
                if(top!=0){
                    //Sort the waiting waiting_queue
                    qsort(waiting_queue,top,sizeof(message), compare);
                    //Remove(s,rts) from Waiting_Q such that rts is minimum
                    min_message=waiting_queue[top--];
                    //Send(YES,i) to s
                    send_message.msg=YES;
                    send_message.seq_no=recv_message.seq_no;
                    send_message.id=world_rank;
                    MPI_Send(&send_message,1,mpi_message,sender_rank,0,MPI_COMM_WORLD);
                    printSentMessage(send_message,sender_rank);
                    //candidate:=s.
                    voted_candidate=local_rank_lookup[min_message.id];
                    //candidate_TS:=RTS
                    candidate_seq=min_message.seq_no;
                    have_inquired=false;
                }
                else{
                    //Have_voted:= false
                    have_voted=false;
                    //Have_inquired:= false
                    have_inquired=false;
                }
                break;
            default:
                printf("WARNING - THIS WARNING IS UNEXPECTED! Inside messageHandlingSection \n");
                printf("Got Message with message=%d, seq_no=%d, from_rank=%d at node %d\n",
                        recv_message.msg,recv_message.seq_no,recv_message.id,world_rank);
        }
    }
    return 0;
}
MPI_Comm *comm_district; //For communication among voting district groups
int main(int argc, char *argv[]) {
    srand(time(NULL));//For random number
    int world_rank, //for storing rank of a node in MPI_COMM_WORLD
        world_size,  //number of nodes in the MPI_COMM_WORLD
        district_size, //size of a district
        //district_rank, //rank in a district
        length,        //for storing length of hostname
        **voting_district, // For storing the voting district arrays.
        seq=0            //Sequence number(Timestamp)
        //color         //For assigning nodes to different voting districts
        ;

    char hostname[MPI_MAX_PROCESSOR_NAME]   //hostname of a node
        ;
    //Initializing MPI
    //MPI_Init(NULL,NULL);
    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
    //Create new datatype for message
    int count= 3; //number of blocks
    int array_of_blocklengths[3]={1,1,1}; //number of elements in each block
    //MPI_Datatype array_of_types[3]={MPI_BYTE,MPI_INT, MPI_INT}; //type of elements in each block
    MPI_Datatype array_of_types[3]={MPI_INT,MPI_INT, MPI_INT}; //type of elements in each block
    MPI_Datatype mpi_message; //new datatype (handle)
    MPI_Aint array_of_displacements[3];//byte displacement of each block
    array_of_displacements[0] = offsetof(message,msg);
    array_of_displacements[1] = offsetof(message,seq_no);
    array_of_displacements[2] = offsetof(message,id);
    MPI_Type_create_struct(count,
                        array_of_blocklengths,
                         array_of_displacements,
                         array_of_types,
                         &mpi_message);
    MPI_Type_commit(&mpi_message);
    //get communication world size
    MPI_Comm_size(MPI_COMM_WORLD,&world_size);
    //get rank of a node
    MPI_Comm_rank(MPI_COMM_WORLD,&world_rank);
    //get hostname
    MPI_Get_processor_name(hostname, &length);
    //district size

    //Creating groups
    /*MPI_Group *g_district, //For storing N voting districts
              g_world //processes in MPI_COMM_WORLD
              ;
    g_district=(MPI_Group *) malloc (sizeof(MPI_Group)*world_size);


    comm_district=(MPI_Comm *) malloc (sizeof(MPI_Comm)*world_size);
    //Creating a group with processes in MPI_COMM_WORLD
    MPI_Comm_group(MPI_COMM_WORLD,&g_world);
    */
    printf("Number of nodes : %d\n",world_size);
    //for now
    district_size=world_size-1;
    voting_district=(int **)malloc(sizeof (int *)*world_size +world_size*(sizeof (int **)*(world_size-1)));
    int *data=voting_district+world_size;
    for(size_t i=0;i<world_size;++i)
        voting_district[i]=data+i*(world_size-1);
    length=createVotingDistricts("voting_district.config",world_size,voting_district);

    //Barrier to load the configuration file
    printd("DEBUG : World rank %d : %s\n",world_rank,hostname);
    MPI_Barrier(MPI_COMM_WORLD);

    if(world_rank==0){
    //check vd
        for(int i=0;i<world_size;++i){
            for(int j=0;j<world_size-1;++j)
                printd("%d ",voting_district[i][j]);
            printd("\n");
        }
    }
    /*for(int i=0;i<world_size;++i){
        //Creating voting district groups
        MPI_Group_incl(g_world,world_size-1,voting_district[i],&g_district[i]);
        //Creating Communicator for groups
        MPI_Comm_create_group(MPI_COMM_WORLD,g_district[i],0,&comm_district[i]);
    }
    //print districts and ranks
    for(int i=0;i<world_size;++i){
        for(int j=0;j<world_size-1;++j)
            if(world_rank==voting_district[i][j]){
                //get communication world size
                MPI_Comm_size(comm_district[i],&district_size);
                //get rank of a node
                MPI_Comm_rank(comm_district[i],&district_rank);
                printf("World rank %d, District %d, district size %d, rank %d\n",world_rank,i+1,district_size,district_rank);
            }
    }
    //Own district size
    MPI_Comm_size(comm_district[world_rank],&district_size);
    //Own district rank
    MPI_Comm_rank(comm_district[world_rank],&district_rank);
    */
    //Test
    int r; //for random number
    #pragma omp parallel shared(mpi_message)
    #pragma omp single
    {
        #pragma omp task
        messageHandlingSection(world_rank,&seq,district_size,mpi_message,voting_district[world_rank]);

        //testmsg
            while(1){
                r = rand()%world_size;
                if(r==world_rank){
                    if(!enterCS(world_rank,&seq,district_size,mpi_message,voting_district[world_rank])){
                        criticalSection();
                        exitCS(world_rank,district_size,mpi_message,voting_district[world_rank]);
                    }
                }
                sleep(2);
            }
    }


    /*/Test
    if(world_rank==1){
        message testmsg;
        testmsg.msg=YES;
        testmsg.seq_no=1;
        testmsg.id=0;
        MPI_Send(&testmsg,1,mpi_message,0,0,comm_district[1]);
    }
    if(world_rank==0){
        message recvmsg[2];
        MPI_Status status[2];
        #pragma omp parallel shared(mpi_message)
        #pragma omp single
        {
            #pragma omp task
            MPI_Recv(&recvmsg[0],1,mpi_message,MPI_ANY_SOURCE,0,comm_district[0],&status[0]);
            printf("Message Recieved with message= %d, seq_no= %d, id = %d\n",recvmsg[0].msg,recvmsg[0].seq_no,recvmsg[0].id);
            #pragma omp task
            MPI_Recv(&recvmsg[1],1,mpi_message,MPI_ANY_SOURCE,0,comm_district[2],&status[1]);
        }
    }
    //Test - END*/

    MPI_Type_free(&mpi_message);
    MPI_Finalize();
    return 0;
}
