#define DEBUG 1 // set  to 0 to hide debug messages
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <stdarg.h>
//For printing debug messages inside printd() if DEBUG is set to 1
#define printd(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)

int createVotingDistricts(char *filename,int n,int **vd){
    printd("DEBUG : Inside createVotingDistricts()");
    int c,row=0,col=0;
    //vd=(int *) malloc (sizeof(int) * n*(n-1))
    FILE *file;
    file = fopen(filename, "r");
    if (file) {
        printd("DEBUG : voting district configuration file opened.\n");
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
int main(int argc, char *argv[]) {
    int world_rank, //for storing rank of a node in MPI_COMM_WORLD
        world_size,  //number of nodes in the MPI_COMM_WORLD
        district_size, //size of a district
        district_rank, //rank in a district
        length,        //for storing length of hostname
        **voting_district // For storing the voting district arrays.
        //color         //For assigning nodes to different voting districts
        ;
    char hostname[MPI_MAX_PROCESSOR_NAME]   //hostname of a node
        ;
    //Initializing MPI
    MPI_Init(NULL,NULL);
    //get communication world size
    MPI_Comm_size(MPI_COMM_WORLD,&world_size);
    //get rank of a node
    MPI_Comm_rank(MPI_COMM_WORLD,&world_rank);
    //get hostname
    MPI_Get_processor_name(hostname, &length);
    //Creating groups
    MPI_Group *g_district, //For storing N voting districts
              g_world //processes in MPI_COMM_WORLD
              ;
    g_district=(MPI_Group *) malloc (sizeof(MPI_Group)*world_size);

    MPI_Comm *comm_district; //For communication among voting district groups
    comm_district=(MPI_Comm *) malloc (sizeof(MPI_Comm)*world_size);
    //Creating a group with processes in MPI_COMM_WORLD
    MPI_Comm_group(MPI_COMM_WORLD,&g_world);

    printf("Number of nodes : %d\n",world_size);
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
    for(int i=0;i<world_size;++i){
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

    MPI_Finalize();
    return 0;
}
