#include <inttypes.h>

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "time.h"
#include <pthread.h>
#include <dirent.h>


int intersezione(uint32_t *array1, uint32_t *array2, int sizeArray1, int sizeArray2);


uint32_t *eliminaDuplicati(int numberOfHashes, uint32_t *hashedShingle, int *sizeFinale);

uint32_t jenkins_one_at_a_time_hash(const char *key, //l'inizio dello shingles di cui fa l'hashing
                                    int length); //lunghezza del singolo shingle
void createSignature(
        char *shingledDocument, //array ottenuto dalla funzione createShingle --> contiene tutti gli shingle del documento
        int shingleSize,     //single shingle size
        int lenDocument,   //lunghezza del documento originale. Serve per calcolare il numero di shingles con la formula: (len - k + 1 ) * k)
        uint32_t *hashArray,   //array con i valori randomoci (delle funzioni di hash) che devono essere uguali per tutti i documenti
        int numberOfHashes,//numero di hash function
        int id, //la colonna(che corriposnde al documento) su cui deve scrivere
        uint32_t **matriceSignature,
        uint32_t **hashedShingles,
        int i);


int hashBucket(uint32_t key, int nBuckets);

void reallocMatriceBuckets(int **matriceBuckets, int numeroDocumenti, int nBande, int nBuckets,

                           int idDocumento, int destBucket, int *nDocPerBucket
);


void createRandomicHashValues(int numberOfHashes, uint32_t *hashArray);

char *createShingles(char *document,
                     int k, //lunghezza single shingle
                     int len); //lunghezza documento

void *threadWork(void *input);

void LSH(uint32_t **matriceSignatures,
         int numeroDocumenti,
         int numberOfHashes,
         int nBande,
         int nRows,
         int nBuckets,
         uint32_t **hashedShingles,
         long myRank
);

void loadfile(char *file,   //il file
              int id,   //id del  testo
              char **buffer);   //out è dove viene salvato il testo  in char

//Variabili Globali accessibili da ogni thread
int thread_count;
int numeroDocumenti;
int shingleSize;
int numberOfHashes;
int nRows;
int nBande;
int nBuckets;

char **nomeDocumenti = NULL;
int argcGlobal;
char **argvGlobal;
char **documenti = NULL;
int *sizeDocumenti = NULL;
char **matriceShingles;
uint32_t *hashArray;
uint32_t **hashedShingles;
uint32_t **matriceSignatures;
int **matriceBuckets;
int *nDocPerBucket;
int **MatriceCaratteristicaBucket;
int **confrontoDocumenti;
float **jaccardMatrix;
pthread_barrier_t barrieraPreLSH;
pthread_barrier_t barrieraBucket;
pthread_mutex_t mutexCharacteristicMatrixBucket;
pthread_mutex_t mutexSupremo;


int main(int argc, char *argv[]) {

    struct timespec start, finish;
    double elapsed;

    clock_gettime(CLOCK_MONOTONIC, &start);


    //mi ricavo gli argomenti passati ad Argv
    argvGlobal = argv;
    argcGlobal = argc;
    shingleSize = atoi(argv[2]);
    numberOfHashes = atoi(argv[3]);
    nRows = atoi(argv[4]);
    nBande = numberOfHashes / nRows;
    nBuckets = atoi(argv[5]);
    thread_count = atoi(argv[6]);


    //inizializzo i mutex

    if (pthread_mutex_init(&mutexCharacteristicMatrixBucket, NULL) != 0) {
        printf("\n mutex init has failed\n");
        return 1;
    }
    if (pthread_mutex_init(&mutexSupremo, NULL) != 0) {
        printf("\n mutex init has failed\n");
        return 1;
    }

//inizializzo le barriere
    pthread_barrier_init(&barrieraPreLSH, NULL, thread_count + 1);
    pthread_barrier_init(&barrieraBucket, NULL, thread_count);

    struct dirent *pDirent;
    DIR *pDir;

    pDir = opendir (argv[1]);

    //provo a leggere la cartella dei documenti da confrontare
    if (pDir == NULL) {
        printf ("Cannot open directory '%s'\n", argv[1]);
        return 1;
    }

    char* dir = argv[1];
    numeroDocumenti = 0;
    while ((pDirent = readdir(pDir)) != NULL) {
        char *nomeDoc = pDirent->d_name;
        if ( !strcmp(pDirent->d_name, ".") == 0 && !strcmp(pDirent->d_name, "..")==0 )
        {
            numeroDocumenti++;
            char src[] = "./";
            char src2[] = "/";
            nomeDoc = strcat(src, dir);
            nomeDoc = strcat(nomeDoc, src2);
            nomeDoc = strcat(nomeDoc, pDirent->d_name);
            nomeDocumenti = (char**) realloc(nomeDocumenti, numeroDocumenti* sizeof(char *));
            nomeDocumenti[numeroDocumenti-1] = (char*) malloc(sizeof(char)* strlen(nomeDoc));
            nomeDoc = strcpy(nomeDocumenti[numeroDocumenti-1], nomeDoc);

        }
    }


//inizializzo le matrici e vettori dichiarate globali
    documenti = (char **) malloc(numeroDocumenti * sizeof(char *));
    sizeDocumenti = (int *) malloc(numeroDocumenti * sizeof(int));
    matriceShingles = (char **) malloc(numeroDocumenti * sizeof(char *));


    hashArray = (uint32_t *) malloc(numberOfHashes * sizeof(uint32_t));
    createRandomicHashValues(numberOfHashes, hashArray);
    hashedShingles = (uint32_t **) malloc(sizeof(uint32_t *) * numeroDocumenti);


    confrontoDocumenti = (int **) malloc(numeroDocumenti * sizeof(int *));
    for (int i = 0; i < numeroDocumenti; i++) {
        confrontoDocumenti[i] = (int *) malloc(numeroDocumenti * sizeof(int));
    }

    jaccardMatrix = (float **) malloc(numeroDocumenti * sizeof(float *));
    for (int i = 0; i < numeroDocumenti; i++) {
        jaccardMatrix[i] = (float *) calloc(numeroDocumenti, sizeof(float));
    }


    matriceSignatures = (uint32_t **) malloc(sizeof(uint32_t *) * numberOfHashes);
    for (int i = 0; i < numberOfHashes; i++) {
        matriceSignatures[i] = (uint32_t *) malloc(numeroDocumenti * sizeof(uint32_t));
    }

    long thread;
    pthread_t *thread_handles;

    thread_handles = malloc(thread_count * sizeof(pthread_t));

    //mi creo i thread
    for (thread = 0; thread < thread_count; thread++) {
        pthread_create(&thread_handles[thread], NULL, threadWork, (void *) thread);

    }


    matriceBuckets = (int **) calloc(nBuckets, sizeof(int *));
    for (int i = 0; i < nBuckets; i++) {
        matriceBuckets[i] = (int *) calloc(1, sizeof(int));
    }
    nDocPerBucket = (int *) calloc(nBuckets, sizeof(int));

    MatriceCaratteristicaBucket = (int **) malloc(nBuckets * sizeof(int *));
    for (int i = 0; i < nBuckets; i++) {
        MatriceCaratteristicaBucket[i] = (int *) malloc(numeroDocumenti * sizeof(int));
    }

    //metto la barriera pure qui dato che voglio essere sicuro di aver allocato le strutture dati necessarie al LSH
    pthread_barrier_wait(&barrieraPreLSH);

    //aspetto che i thread finiscano
    for (thread = 0; thread < thread_count; thread++) {
        pthread_join(thread_handles[thread], NULL);

    }


    clock_gettime(CLOCK_MONOTONIC, &finish);


    //stampo la matrice di con i coefficenti di jaccard
    int **confrontati = (int**)malloc(numeroDocumenti* sizeof(int*));
    for(int i=0; i<numeroDocumenti; i++ ){
        confrontati[i] =  (int*) malloc(numeroDocumenti*sizeof(int));
    }

    for (int i = 0; i < numeroDocumenti; i++) {
        printf("\n");
        for (int j = 0; j < numeroDocumenti; j++) {
            if(jaccardMatrix[i][j] >=0.1f ){
                confrontati[i][j] = 1;
            }
            printf(" %.6f", jaccardMatrix[i][j]);

        }
    }

    printf("\n\nI file più simili (maggiori di %f) in base alla coefficiente di Jaccard sono:\n", 0.1f);

    for(int i=0; i<numeroDocumenti; i++){
        for(int j=0; j<numeroDocumenti; j++ ){
            if (confrontati[i][j] == 1){
                printf("\n Il coefficiente di Jaccard tra il file %s e il file %s è: %f", nomeDocumenti[i], nomeDocumenti[j], jaccardMatrix[i][j]);
            }
        }
    }


    elapsed = (finish.tv_sec - start.tv_sec);
    elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    printf("\n\nIl wall clock time è: %f secondi ", elapsed);


    exit(0);
    return 0;

}

void loadfile(char *file, int id, char **buffer) {
    /*
     * funzione che serve per leggere un file
    */

    FILE *fp;
    long lSize;

    fp = fopen(file, "rb");
    if (fp == NULL) {
        printf("Errore nell'apertura del file\n");
        exit(1);
    }
    fseek(fp, 0L, SEEK_END);
    lSize = ftell(fp);
    int size1 = lSize / sizeof(char);
    sizeDocumenti[id] = size1;
    rewind(fp);
/* allocate memory for entire content */
    buffer[0] = calloc(1, lSize + 1);

/* copy the file into the buffer */
    if (1 != fread(buffer[0], lSize, 1, fp)) {
        fclose(fp);
        free(buffer[0]);
        fputs("entire read fails", stderr);
        exit(1);
    }

    fclose(fp);

}


char *createShingles(char *document,
                     int k, //lunghezza single shingle
                     int len) //lunghezza documento
{
    //char *shingles = (char *) malloc(sizeof(char) * (len - k + 1) * k); //(len - k + 1 ) è il numero di shingles in cui il documento verrà diviso
    //memorizziamo gli shingles in maniera contigua in modo tale da avere meno cache miss
    char *shingles = NULL;
    int pos = 0;
    int count = 1;
    for (int row = 0; row < len - k + 1; row++) {
        for (int col = 0; col < k; col++) {
            if (len <= pos + col) return shingles;
            shingles = (char *) realloc(shingles, count);
            shingles[row * k + col] = document[pos + col];
            count++;
        }
        pos++;
    }

    shingles = (char *) realloc(shingles, sizeof(char) * count);
    shingles[count - 1] = '\0';
    return shingles;
}

void createRandomicHashValues(int numberOfHashes, uint32_t *hashArray) {
    /*
     * Serve per creare un array di valori casuali per fare molteplici volte l'hashing nella siignature
     */

    srand(time(NULL));
    for (int i = 0; i < numberOfHashes; i++) {
        hashArray[i] = (uint32_t) rand();
    }
}


uint32_t jenkins_one_at_a_time_hash(const char *key, //l'inizio dello shingles di cui fa l'hashing
                                    int length) //lunghezza del singolo shingle
{
    /*
     * funzione di hasing trovata online. Serve per trasformare una stringa in un unsigned int a 32 bit
     */


    int i = 0;
    uint32_t hash = 0;
    while (i != length) {

        if(key[i] == '\0') {
            hash += hash << 3;
            hash ^= hash >> 11;
            hash += hash << 15;
            return hash;
        }
        hash += key[i];
        hash += hash << 10;
        hash ^= hash >> 6;
        i++;

    }
    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;

}

void createSignature(
        char *shingledDocument, //array ottenuto dalla funzione createShingle --> contiene tutti gli shingle del documento
        int shingleSize,     //single shingle size
        int lenDocument,   //lunghezza del documento originale. Serve per calcolare il numero di shingles con la formula: (len - k + 1 ) * k)
        uint32_t *hashArray,   //array con i valori randomoci (delle funzioni di hash) che devono essere uguali per tutti i documenti
        int numberOfHashes,//numero di hash function
        int id, //la colonna(che corriposnde al documento) su cui deve scrivere
        uint32_t **matriceSignature,
        uint32_t **hashedShingles,
        int i)
{

    int numberOfShingles = lenDocument - shingleSize + 1;     //numero di shingles
    uint32_t *jenkinsHashings = (uint32_t *) malloc(numberOfShingles * sizeof(uint32_t));

    for (int i = 0; i < numberOfShingles; i++) {
        jenkinsHashings[i] = jenkins_one_at_a_time_hash(&(shingledDocument[i * shingleSize]),
                                                        shingleSize);   //quando facciamo [i * shingleSize] stiamo deferenziando perciò ci serve il &

    }

    hashedShingles[id] = jenkinsHashings;
    uint32_t min;
    for (int i = 0; i < numberOfHashes; i++) {     //prendiamo la i-esima hash value
        min = jenkinsHashings[0] ^ hashArray[i];
        for (int j = 1; j < numberOfShingles; j++) {  //prendiamo il j-esimo jenkins
            uint32_t currentHash = jenkinsHashings[j] ^ hashArray[i];
            if (currentHash < min) min = currentHash;
        }
        matriceSignature[i][id] = min;
    }

}

//fa il setup dei documenti, crea la matrice degli shingles e crea la hashedShingles (ogni thread lo fa per la sua porzione di documenti)
void *threadWork(void *input) {
    long my_rank = (long) input;
    long my_nDocumenti = numeroDocumenti / thread_count;
    long my_first_i = my_nDocumenti * my_rank;
    long my_last_i = my_first_i + my_nDocumenti;

    for (int i = my_first_i; i < my_last_i; i++) {
        char *nomeDoc = nomeDocumenti[i];
        char **bufferD = (char **) malloc(1 * sizeof(char *));
        loadfile(nomeDoc, i, bufferD);
        documenti[i] = bufferD[0];


        matriceShingles[i] = (char *) malloc((sizeDocumenti[i] - shingleSize + 1));
        matriceShingles[i] = createShingles(documenti[i], shingleSize, sizeDocumenti[i]);

        hashedShingles[i] = (uint32_t *) malloc((sizeDocumenti[i] - shingleSize + 1) * sizeof(uint32_t));
        createSignature(matriceShingles[i], shingleSize, sizeDocumenti[i], hashArray, numberOfHashes, i,
                        matriceSignatures, hashedShingles, i);

    }
    pthread_barrier_wait(&barrieraPreLSH);

    LSH(matriceSignatures, numeroDocumenti, numberOfHashes, nBande, nRows, nBuckets, hashedShingles, my_rank);


    return NULL;

}

void reallocMatriceBuckets(int **matriceBuckets, int numeroDocumenti, int nBande, int nBuckets,

                           int idDocumento, int destBucket, int *nDocPerBucket
) {

    matriceBuckets[destBucket][nDocPerBucket[destBucket]] = idDocumento;
    nDocPerBucket[destBucket] += 1;
    int test = (nDocPerBucket[destBucket] + 1) * sizeof(int);
    matriceBuckets[destBucket] = (int *) realloc(matriceBuckets[destBucket], test);

}

int hashBucket(uint32_t key, int nBuckets) {   //  h(k)=(a*k+b){mod Bucket} (a e b costanti) --> B = numero di buckets
    int a = 5;
    int b = 7;
    int bucket = (a * key + b) % nBuckets;
    return bucket;
}

uint32_t *eliminaDuplicati(int numberOfHashes, uint32_t *hashedShingle, int *sizeFinale) {
    uint32_t *nuovoArray = NULL;


    int lenNuovo = 0;
    for (int i = 0; i < numberOfHashes; i++) {
        int check = 0;
        uint32_t elemento = hashedShingle[i];
        for (int j = 0; j < lenNuovo; j++) {
            uint32_t jelemento = nuovoArray[j];
            if (jelemento == elemento) {
                check = 1;
            }
        }
        if (check == 0) {
            lenNuovo++;
            nuovoArray = (uint32_t *) realloc(nuovoArray, lenNuovo * sizeof(uint32_t));
            nuovoArray[lenNuovo - 1] = elemento;


        }
    }

    *sizeFinale = lenNuovo;
    return nuovoArray;
}

int intersezione(uint32_t *array1, uint32_t *array2, int sizeArray1, int sizeArray2) {
    int max = 0;
    int count = 0;
    if (sizeArray1 > sizeArray2) {
        max = sizeArray1;
    } else {
        max = sizeArray2;
    }
    uint32_t *intersection = (int *) malloc(sizeof(int) * max);
    for (int i = 0; i < max; i++) intersection[i] = -1;

    int h = 0;
    for (int i = 0; i < sizeArray1; i++) {
        for (int j = 0; j < sizeArray2; j++)
            if (array1[i] == array2[j]) {
                int presente = 0;
                for (int k = 0; k < max; k++) {
                    if (intersection[k] == array1[i]) {
                        presente = 1;
                    }
                }
                if (presente == 0) {
                    count++;
                    intersection[h] = array1[i];
                    h++;
                }
            }
    }

    return count;
}


void    LSH(uint32_t **matriceSignatures,
         int numeroDocumenti,
         int numberOfHashes,
         int nBande,
         int nRows,
         int nBuckets,
         uint32_t **hashedShingles,
         long my_rank
        //      int *sizeDocumenti
) {


    //supponiamo che nBande sia perfettamente divisibile per thread_count
    long my_nBande = nBande / thread_count;
    long my_first_i = my_nBande * my_rank;
    long my_last_i = my_first_i + my_nBande;

    for (int iBanda = my_first_i; iBanda < my_last_i; iBanda++) {
        int start = (nRows * iBanda);
        for (int i = 0; i < numeroDocumenti; i++) {
            uint32_t somma = 0;
            for (int j = start; j < start + nRows; j++) {
                somma += matriceSignatures[j][i];
            }
            int bucket = hashBucket(somma, nBuckets);
            //ogni thread ha le sue bande, che deve andare ad hashare in un bucket: è possibile che 2 thread contemporaneamente facciano l'hash della banda nello stesso bucket --> sezione critica
            //SEZIONE CRITICA SIUUUUM
            pthread_mutex_lock(&mutexCharacteristicMatrixBucket);
            if (MatriceCaratteristicaBucket[bucket][i] != 1) {
                MatriceCaratteristicaBucket[bucket][i] = 1;
                reallocMatriceBuckets(matriceBuckets, numeroDocumenti, nBande, nBuckets, i, bucket, nDocPerBucket);
            }
            //FINE SEZIONE CRITICA
            pthread_mutex_unlock(&mutexCharacteristicMatrixBucket);
        }
    }



    pthread_barrier_wait(&barrieraBucket);


    long my_nBucket = nBuckets / thread_count;
    long my_first_i_Bucket = my_nBucket * my_rank;
    long my_last_i_bucket = my_first_i_Bucket + my_nBucket;


    uint32_t *ptr1;
    uint32_t *ptr2;
    for (int bucket = my_first_i_Bucket; bucket < my_last_i_bucket; bucket++) {      //seleziono il bucket
        for (int i = 0; i < nDocPerBucket[bucket]; i++) {    // seleziono il primo documento
            int idPrimoDoc = matriceBuckets[bucket][i];
            for (int j = i + 1; j < nDocPerBucket[bucket]; j++) {//seleziono il secondo
                int idSecondoDoc = matriceBuckets[bucket][j];

                //SEZIONE CRITICA
                pthread_mutex_lock(&mutexSupremo);
                if (confrontoDocumenti[idPrimoDoc][idSecondoDoc] == 0 &&
                    confrontoDocumenti[idSecondoDoc][idPrimoDoc] == 0) {

                    confrontoDocumenti[idPrimoDoc][idSecondoDoc] = 1;
                    confrontoDocumenti[idSecondoDoc][idPrimoDoc] = 1;

                    pthread_mutex_unlock(&mutexSupremo);

                    int sizePrimoDoc;
                    int sizeSecondoDoc;
                    int numberOfShingles1 = sizeDocumenti[idPrimoDoc] - shingleSize + 1;
                    int numberOfShingles2 = sizeDocumenti[idSecondoDoc] - shingleSize + 1;

                    ptr1 = eliminaDuplicati(numberOfShingles1, hashedShingles[idPrimoDoc], &sizePrimoDoc);
                    ptr2 = eliminaDuplicati(numberOfShingles2, hashedShingles[idSecondoDoc], &sizeSecondoDoc);
                    float inters = intersezione(ptr1, ptr2,
                                                sizePrimoDoc, sizeSecondoDoc);

                    float unione = sizePrimoDoc + sizeSecondoDoc - inters;
                    jaccardMatrix[idPrimoDoc][idSecondoDoc] = inters / unione;
                    //t   }
                } else {
                    pthread_mutex_unlock(&mutexSupremo);
                }
                //FINE SEZIONE CRITICA
            }
        }
    }

}