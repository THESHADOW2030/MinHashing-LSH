#include <inttypes.h>
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "timer.h"
#include <dirent.h>
#include <time.h>



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
    for(int i = 0; i <  max; i++) intersection[i] = -1;

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
    free(intersection);
    return count;
}


uint32_t jenkins_one_at_a_time_hash(const char *key, //l'inizio dello shingles di cui fa l'hashing
                                    int length) //lunghezza del singolo shingle
{


    int i = 0;
    uint32_t hash = 0;
    while (i != length) {
        if(key[i] == '\0') {
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


void printPointer(char *stringa) {

    for (int i = 0; i < strlen(stringa) / sizeof(char); i++) printf("%c", stringa[i]);

}

int hashBucket(uint32_t key, int nBuckets) {   //  h(k)=(a*k+b){mod Bucket} (a e b costanti) --> B = numero di buckets
    int a = 5;
    int b = 7;
    int bucket = (a * key + b) % nBuckets;
    return bucket;
}


void loadfile(char *file, int *size, int id, char **buffer) {

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
    size[id] = size1;
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


    char *shingles = NULL;
    int pos = 0;
    int count = 1;
    for (int row = 0; row < len - k + 1; row++) {
        for (int col = 0; col < k; col++) {
            if (len <= pos + col) return shingles;
            shingles = (char*) realloc(shingles, count);
            int x = sizeof(shingles);
            shingles[row * k + col] = document[pos + col];
            count++;
        }
        pos++;

    }

    shingles = (char*) realloc(shingles, sizeof(char)*count);
    shingles[count-1] = '\0';

    return shingles;


}


int charCount(char *x) {
    return strlen(x) / sizeof(char); //ottengo la lunghezza in numero di caratteri dividendo per la size del char
}


void printShingle(char *shingles, int len, int k) {
    for (int row = 0; row < len - k + 1; row++) {

        for (int col = 0; col < k; col++) printf("%c", shingles[row * k + col]);
    }
}


void createRandomicHashValues(int numberOfHashes, uint32_t *hashArray) {
    srand(time(NULL));
    for (int i = 0; i < numberOfHashes; i++) {
        hashArray[i] = (uint32_t) rand();
    }
}

// la funzione create signature crea la signature matrix: per ogni documento, per ogni funzione hash,
// hasha tutti gli shingle con una data funzione di hash e poi prende il minimo valore tra gli hash degli shingle.

void createSignature(
        char *shingledDocument, //array ottenuto dalla funzione createShingle --> contiene tutti gli shingle del documento
        int shingleSize,     //single shingle size
        int lenDocument,   //lunghezza del documento originale. Serve per calcolare il numero di shingles con la formula: (len - k + 1 ) * k)
        uint32_t *hashArray,   //array con i valori randomoci (delle funzioni di hash) che devono essere uguali per tutti i documenti
        int numberOfHashes,//numero di hash function
        int id, //la colonna(che corrisponde al documento) su cui deve scrivere
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
        //printf("%lu\n", min)
        for (int j = 1; j < numberOfShingles; j++) {  //prendiamo il j-esimo jenkins
            uint32_t currentHash = jenkinsHashings[j] ^ hashArray[i];
            if (currentHash < min) min = currentHash;
        }
        matriceSignature[i][id] = min;
    }
}

void reallocMatriceBuckets(int **matriceBuckets, int numeroDocumenti, int nBande, int nBuckets,

                           int idDocumento, int destBucket, int *nDocPerBucket
) {

    matriceBuckets[destBucket][nDocPerBucket[destBucket]] = idDocumento;
    nDocPerBucket[destBucket] += 1;
    int t = nDocPerBucket[destBucket];
    int test = (nDocPerBucket[destBucket] + 1) * sizeof(int);
    matriceBuckets[destBucket] = (int *) realloc(matriceBuckets[destBucket], test);

}


float **LSH(uint32_t **matriceSignatures,
            int numeroDocumenti,
            int numberOfHashes,
            int nBande,
            int nRows,
            int nBuckets,
            uint32_t **hashedShingles,
            int *sizeDocumenti,
            int shingleSize
) {


    int **matriceBuckets = (int **) calloc(nBuckets, sizeof(int *));
    for (int i = 0; i < nBuckets; i++) {
        matriceBuckets[i] = (int *) calloc(1, sizeof(int));
    }
    int *nDocPerBucket = (int *) calloc(nBuckets, sizeof(int));


    //La MatriceCaratteristicaBucket ha per righe i buckets e oer colonne i documenti: MatriceCaratteristicaBucket[i][j] vale 1 se il documento j sta nel bucket i, altrimenti vale 0
    int **MatriceCaratteristicaBucket = (int **) malloc(nBuckets * sizeof(int *));
    for (int i = 0; i < nBuckets; i++) {
        MatriceCaratteristicaBucket[i] = (int *) malloc(numeroDocumenti * sizeof(int));
    }


    for (int iBanda = 0; iBanda < nBande; iBanda++) {
        int start = (nRows * iBanda);
        for (int i = 0; i < numeroDocumenti; i++) {
            uint32_t somma = 0;
            for (int j = start; j < start + nRows; j++) {
                somma += matriceSignatures[j][i];
            }
            int bucket = hashBucket(somma, nBuckets);

            if (MatriceCaratteristicaBucket[bucket][i] != 1) {
                MatriceCaratteristicaBucket[bucket][i] = 1;
                reallocMatriceBuckets(matriceBuckets, numeroDocumenti, nBande, nBuckets, i, bucket, nDocPerBucket);
            }
        }
    }


    int **confrontoDocumenti = (int **) malloc(numeroDocumenti * sizeof(int *));
    for (int i = 0; i < numeroDocumenti; i++) {
        confrontoDocumenti[i] = (int *) malloc(numeroDocumenti * sizeof(int));
    }

    float **jaccardMatrix = (float **) malloc(numeroDocumenti * sizeof(float *));
    for (int i = 0; i < numeroDocumenti; i++) {
        jaccardMatrix[i] = (float *) calloc(numeroDocumenti, sizeof(float));
    }

    uint32_t *ptr1;
    uint32_t *ptr2;
    for (int bucket = 0; bucket < nBuckets; bucket++) {      //seleziono il bucket
        for (int i = 0; i < nDocPerBucket[bucket]; i++) {    // seleziono il primo documento
            int idPrimoDoc = matriceBuckets[bucket][i];
            for (int j = i + 1; j < nDocPerBucket[bucket]; j++) {
                int idSecondoDoc = matriceBuckets[bucket][j];
                if (confrontoDocumenti[idPrimoDoc][idSecondoDoc] == 0 &&
                    confrontoDocumenti[idSecondoDoc][idPrimoDoc] == 0) {
                    confrontoDocumenti[idPrimoDoc][idSecondoDoc] = 1;
                    confrontoDocumenti[idSecondoDoc][idPrimoDoc] = 1;
                    int sizePrimoDoc;
                    int sizeSecondoDoc;
                    int numberOfShingles1 = sizeDocumenti[idPrimoDoc] - shingleSize + 1;
                    int numberOfShingles2 = sizeDocumenti[idSecondoDoc] - shingleSize + 1;
                    if ((idPrimoDoc == 1 && idSecondoDoc == 2) || idPrimoDoc == 2 && idSecondoDoc == 1) {
                        int chePalle = 10;
                    }
                    ptr1 = eliminaDuplicati(numberOfShingles1, hashedShingles[idPrimoDoc], &sizePrimoDoc);
                    ptr2 = eliminaDuplicati(numberOfShingles2, hashedShingles[idSecondoDoc], &sizeSecondoDoc);
                    float inters = intersezione(ptr1, ptr2,
                                                sizePrimoDoc, sizeSecondoDoc);

                    float unione = sizePrimoDoc + sizeSecondoDoc - inters;
                    jaccardMatrix[idPrimoDoc][idSecondoDoc] = inters / unione;
                }
            }
        }
    }
    for (int i = 0; i < nBuckets; i++) {
        free(matriceBuckets[i]);
    }
    free(matriceBuckets);


    for (int i = 0; i < nBuckets; i++) {
        free(MatriceCaratteristicaBucket[i]);
    }
    free(MatriceCaratteristicaBucket);
    free(nDocPerBucket);

    return jaccardMatrix;
}

int main(int argc, char *argv[]) {


    double start, finish;

    GET_TIME(start);

    struct dirent *pDirent;
    DIR *pDir;
    char **documenti = NULL;
    int *sizeDocumenti = NULL;
    char* dir = argv[1];

    pDir = opendir (argv[1]);
    if (pDir == NULL) {
        printf ("Cannot open directory '%s'\n", argv[1]);
        return 1;
    }

    int numeroDocumenti = 0;

    char** nomeDocumenti = NULL;
    int i = 1;
    while ((pDirent = readdir(pDir)) != NULL) {

        char *nomeDoc = pDirent->d_name;
        if ( !strcmp(pDirent->d_name, ".") == 0 && !strcmp(pDirent->d_name, "..")==0 )
        {
            char *nomeDoc;
            numeroDocumenti++;
            char src[] = "./";
            char src2[] = "/";
            nomeDoc = strcat(src, dir);
            nomeDoc = strcat(nomeDoc, src2);
            nomeDoc = strcat(nomeDoc, pDirent->d_name);

            nomeDocumenti = (char**) realloc(nomeDocumenti, numeroDocumenti* sizeof(char *));
            nomeDocumenti[numeroDocumenti-1] = (char*) malloc(sizeof(char)* strlen(nomeDoc));nomeDoc = strcpy(nomeDocumenti[numeroDocumenti-1], nomeDoc);
            char **bufferD = (char **) malloc(1 * sizeof(char *));
            documenti = (char**) realloc(documenti, numeroDocumenti* sizeof(char *));
            sizeDocumenti = (int *) realloc(sizeDocumenti, numeroDocumenti * sizeof(int));
            loadfile(nomeDoc, sizeDocumenti, i - 1, bufferD);
            documenti[i - 1] = bufferD[0];
            i++;
        }

    }

// Close directory and exit.

    closedir (pDir);


    int shingleSize = atoi(argv[2]);
    int numberOfHashes = atoi(argv[3]);
    int nRows = atoi(argv[4]);
    int nBande = numberOfHashes / nRows;
    int nBuckets = atoi(argv[5]);


    //creazione dei Shingles
    char **matriceShingles = (char **) malloc(numeroDocumenti * sizeof(char *));
    for (int i = 0; i < numeroDocumenti; i++) {
        matriceShingles[i] = createShingles(documenti[i], shingleSize, sizeDocumenti[i]);

    }

    /////////////////////////////////////////////////////////////////////////////////////////////

    uint32_t *hashArray = (uint32_t *) malloc(numberOfHashes * sizeof(uint32_t));
    createRandomicHashValues(numberOfHashes, hashArray);


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //la matrice delle signature ha come righe le funzioni di hash, come colonne i documenti


    uint32_t **matriceSignatures = (uint32_t **) malloc(sizeof(uint32_t *) * numberOfHashes);
    for (int i = 0; i < numberOfHashes; i++) {
        matriceSignatures[i] = (uint32_t *) malloc(numeroDocumenti * sizeof(uint32_t));
    }


    uint32_t **hashedShingles = (uint32_t **) malloc(sizeof(uint32_t *) * numeroDocumenti);
    for (int i = 0; i < numeroDocumenti; i++) {
        hashedShingles[i] = (uint32_t *) malloc((sizeDocumenti[i] - shingleSize + 1) * sizeof(uint32_t));

    }


    for (int i = 0; i < numeroDocumenti; i++) {
        createSignature(matriceShingles[i], shingleSize, sizeDocumenti[i], hashArray, numberOfHashes, i,
                        matriceSignatures, hashedShingles, i);

    }



    float **JaccardMatrix = LSH(matriceSignatures, numeroDocumenti, numberOfHashes, nBande, nRows, nBuckets,
                                hashedShingles, sizeDocumenti, shingleSize);


    GET_TIME(finish);


    int **confrontati = (int**)malloc(numeroDocumenti* sizeof(int*));
    for(int i=0; i<numeroDocumenti; i++ ){
        confrontati[i] =  (int*) malloc(numeroDocumenti*sizeof(int));
    }

    for (int i = 0; i < numeroDocumenti; i++) {
        printf("\n");
        for (int j = 0; j < numeroDocumenti; j++) {
            if(JaccardMatrix[i][j] >=0.1f ){
                confrontati[i][j] = 1;
            }
            printf(" %.6f", JaccardMatrix[i][j]);
        }
    }

    printf("\n\nI file più simili in base alla coefficiente di Jaccard sono:\n");

    for(int i=0; i<numeroDocumenti; i++){
        for(int j=0; j<numeroDocumenti; j++ ){
            if (confrontati[i][j] == 1){
                printf("\n Il coefficiente di Jaccard tra il file %s e il file %s è: %f", nomeDocumenti[i], nomeDocumenti[j], JaccardMatrix[i][j]);
            }
        }
    }




    printf("\n\n\nIl wall clock time è %f secondi", finish-start);



    free(sizeDocumenti);
    for(int i = 0; i< numeroDocumenti;i++){
        free(documenti[i]);
    }
    free(documenti);

    for(int i = 0; i< numeroDocumenti;i++){
        free(nomeDocumenti[i]);
    }
    free(nomeDocumenti);

    for(int i = 0; i< numeroDocumenti;i++){
        free(matriceShingles[i]);
    }
    free(matriceShingles);


    free(hashArray);

    for(int i = 0; i< numberOfHashes;i++){
        free(matriceSignatures[i]);
    }
    free(matriceSignatures);

    for(int i = 0; i< numeroDocumenti;i++){
        free(hashedShingles[i]);
    }
    free(hashedShingles);

    for(int i = 0; i< numeroDocumenti;i++){
        free(confrontati[i]);
    }
    free(confrontati);






    exit(0);



}
