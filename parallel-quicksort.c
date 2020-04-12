// Constantine Xipolitopoulos
// 11 May 2018

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h>

#define N 100000      // number of array elements to sort
#define CUTOFF 10     // cutoff to begin sorting with insertion sort
#define NUMTHREADS 4  // run as many threads as your CPU thread count (i5=4 | i7=8 | ryzen7=16)

typedef struct{
    double *a;
    int n;
} qPart;

void  insertionSort(double *, int);
int   partition    (double *, int); // segment given array into smaller -> pivot -> bigger
void* work        		   (void*); // Thread Function
void  addJob       (double *, int); // Add Job to Queue
qPart* readJob              (void); // Execute Job from Queue
bool  can_read              (void);
bool  buf_is_full           (void);

qPart global_buffer[N]; // quicksort job global buffer, stores qPart structs
int written = 0; 		// number of jobs created
int read = 0;   	    // number of jobs completed
int insertion_slots_done = 0 // how many did we sort
int exitcond = 0;

pthread_cond_t job_in = PTHREAD_COND_INITIALIZER;  // signal means new job got put in buffer - wait means we are waiting for more  jobs
pthread_cond_t job_out = PTHREAD_COND_INITIALIZER; // signal means a job task got finished by a thread - wait means main thread waits for signals and every time checks if sorting finished
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;


int main()
{
    // CREATE NUMBERS ARRAY
    double *a = (double *)malloc(N * sizeof(double));
	printf("FILLING ARRAY:\n");
	srand(time(NULL));
	for (int i = 0; i < N; i++){
		a[i] = rand() % 1000; // up to 1000 rand numbers
		printf("%f   ", a[i]);
	}


    // CREATE THREADS
	pthread_t * pthread_array = malloc(NUMTHREADS * sizeof(pthread_t));
    for(int i = 0; i < NUMTHREADS; i++)
        pthread_create(&pthread_array[i], NULL, work, (void*)((long)i));


	// CREATE FIRST JOB
	addJob(a, N); // the entire array
	pthread_cond_signal(&job_in); // signal threads that job is in


	// CHECK SHUTDOWN CONDITIONS AND SEND SHUTDOWN MESSAGE TO ALL THREADS
	printf("\n ***** Checking Shutdown_in ***** \n");
	pthread_mutex_lock(&mutex);
	while(insertion_slots_done != N){
		pthread_cond_wait(&job_out, &mutex);
		printf("checking? for shutdown\n");
	}
	printf("YES!!! exitcond=1 now.\n");
	exitcond = 1;
	pthread_cond_broadcast(&job_in); // tell every thread to wake, and then cond=1 so they break the while loop and exit
	pthread_mutex_unlock(&mutex);
	printf("\n\n\n >> BROKE OUT OF MAIN THREAD CHECKING << \n\n\n");


	// CHECK IF CORRECTLY SORTED
    for (int i = 0; i < (N - 1); i++){
		if (a[i] > a[i + 1]){
			printf("Sorting failed!! :( \n");
			break;
		}else{
			printf("ok\n");
		}
	}
	printf("\nDone Checking Sorting..\n");


    // JOIN THE THREADS
    for (int i = 0; i < NUMTHREADS; i++){
        pthread_join(pthread_array[i], NULL);
		printf("\nJOINED THREAD #: %d\n", i+1);
	}
	printf("\n\nDONE ********************** YEAH!! \n\n");


	// CLEANING UP
	free(pthread_array);
	free(a);
	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&job_out);
	pthread_cond_destroy(&job_in);


	// EXIT
	return 0;
}


void *work(void* which)
{
	printf("............ I am THREAD number %ld out of %d.\n\n", (long)which +1, NUMTHREADS);
	qPart *temp; // local pointer to qPart struct objects

	while(1){
		/**********read job*/
		pthread_mutex_lock(&mutex);
		while (!can_read()){
			printf("trying to read job_in");
			if (exitcond) break;
			pthread_cond_wait(&job_in, &mutex); // waiting till we can read a job
		}
		if (exitcond) break; // protected by mutex lock
		temp = readJob();
		pthread_mutex_unlock(&mutex);


		/**********do job - write 2 more partition jobs*/
		if ( (*temp).n <= CUTOFF) {
			pthread_mutex_lock(&mutex);
			printf("Oh damn, performing INSERTION SORT!!\n");
			insertion_slots_done += (*temp).n;
			printf("INSERTION SLOTS DONE: %d", insertion_slots_done);
			pthread_mutex_unlock(&mutex);
			// ******** PERFORMANCE CRITICAL SECTION #1 ********

			insertionSort((*temp).a, (*temp).n);  // insertion sort NOT mutex locked

			// ******** PERFORMANCE CRITICAL SECTION #1 ********
		}
		else{
			// ******** PERFORMANCE CRITICAL SECTION #2 ********

			int i = partition((*temp).a, (*temp).n); // partition NOT mutex locked

			// ******** PERFORMANCE CRITICAL SECTION #2 ********
			pthread_mutex_lock(&mutex);
			addJob((*temp).a, i);  // a -> i
			addJob((*temp).a + i, (*temp).n - i); // a+i -> N -i
			printf("\n\nJobs in Queue: %d", (written-read));
			pthread_mutex_unlock(&mutex);
			pthread_cond_signal(&job_in); // signal that something was put in job buffer,wake up at least one more thread
		}
		pthread_cond_signal(&job_out);
	}
	pthread_mutex_unlock(&mutex);
	pthread_exit(NULL);
}


void addJob(double* my_a, int my_n){
	printf("Adding Job\n");
    // adds job to global buffer
    global_buffer[written].a = my_a;
    global_buffer[written].n = my_n;
    written++;
}


qPart* readJob(void){
	printf("Reading Job\n");
    // reads next job from buffer, returns qPart struct pointer
    read++;
    return &global_buffer[read-1];
}


bool can_read(void){
	// returns true if there are any pending jobs in the queue
    if( read < written )
        return true;
	return false;
}


bool buf_is_full(void){
    if( written <= N )
        return true;
	return false;
}


void insertionSort(double *a, int n)
{
	int i, j;
	double t;

	for (i = 1; i < n; i++) {
		j = i;
		while ((j > 0) && (a[j - 1] > a[j])) {
			t = a[j - 1];
			a[j - 1] = a[j];
			a[j] = t;
			j--;
		}
	}
}


int partition(double *a, int n)
{
	printf("partition function called\n");
	int first = 0;
	int middle = n - 1;
	int last = n / 2;
	double p, t;
	int i, j;

	if (a[middle] < a[first]) {
		t = a[middle];
		a[middle] = a[first];
		a[first] = t;
	}
	if (a[last] < a[middle]) {
		t = a[last];
		a[last] = a[middle];
		a[middle] = t;
	}
	if (a[middle] < a[first]) {
		t = a[middle];
		a[middle] = a[first];
		a[first] = t;
	}

	p = a[middle];
	for (i = 1, j = n - 2;; i++, j--) {
		while (a[i] < p)
			i++;
		while (p < a[j])
			j--;
		if (i >= j)
			break;

		t = a[i];
		a[i] = a[j];
		a[j] = t;
	}
	return i;
}
