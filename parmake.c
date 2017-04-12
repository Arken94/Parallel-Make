/**
 * Parallel Make
 * CS 241 - Fall 2016
 */


#include "parmake.h"
#include "parser.h"
#include "includes/vector.h"
#include "includes/queue.h"
#include "common_vector.h"
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>




//wrapper mostly for queue_t struct
typedef struct ThreadInfo{
    queue_t* queue;
}ThreadInfo;

//globals
Vector* rule_list;
int NO_MORE_TASKS = 0;
pthread_mutex_t GL = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t RM = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t CV = PTHREAD_COND_INITIALIZER;
int RULES_ADDED = 0;

// Callback function for parser_parse_makefile in parser.h
void parsed_new_target(rule_t* target){
    
    // not sure what else to do here ??
    Vector_append(rule_list, target);
    
}


// function to see if at all the dependencies of a rule has been touched
// return 1 if true, and 0 if false

int dependencies_touched(rule_t* rule){
    Vector* deps = rule->dependencies;
    for (size_t i = 0; i < Vector_size(deps); i++) {
        rule_t* dependency = Vector_get(deps, i);
        if (dependency->state == 0) {
            return 0;
        }
    }
    return 1;
}

// function to see if at least one the dependencies of a rule has failed
// return 1 if true, and 0 if false

int dependencies_failed(rule_t* rule){
    Vector* deps = rule->dependencies;
    for (size_t i = 0; i < Vector_size(deps); i++) {
        rule_t* dependency = Vector_get(deps, i);
        if (dependency->state == -1) {
            return 1;
        }
    }
    return 0;
}


// if a list of dependencies includes one that is not a file on disk
int not_file_on_disk(Vector* deps){
    
    for (size_t i = 0; i < Vector_size(deps); i++) {
        rule_t* rule = Vector_get(deps, i);
        if ( access(rule->target, F_OK | R_OK) != 0 ) {
            return 1;
        }
    }
    
    return 0;
}

//returns 0 if none are newer else returns 1
int dependencies_newer(rule_t* rule){
    
    Vector* deps = rule->dependencies;
    struct stat orig;
    stat(rule->target, &orig);
    
    for (size_t i = 0; i < Vector_size(deps); i++) {
        
        rule_t* cur_rule = Vector_get(deps, i);
        struct stat compare;
        stat(cur_rule->target, &compare);
        
        double res =  difftime(orig.st_mtim.tv_sec, compare.st_mtim.tv_sec);
        if (res < 0) {
            return 1;
        }
        
        
    }
    
    return 0;
    
}




void* process(void* ptr){
    
    
    ThreadInfo* ti = (ThreadInfo*)ptr;
    queue_t* queue = ti->queue;
    //we no longer care about the package - it was only a delivery container :)
    free(ti);
    
    while(1){
        rule_t* rule;
        
        /* This must be wrapped in the same lock that changes the NO_MORE_TASKS global variable
         because we want to make SURE that threads read the proper value of that variable after it has
         been changed and not while it is BEING changed */
        pthread_mutex_lock(&GL);
        
        if (NO_MORE_TASKS){
            pthread_mutex_unlock(&GL);
            break;
        }
        else rule = (rule_t*)queue_pull(queue);
        if (rule->state == -99 ){
            NO_MORE_TASKS = 1;
            pthread_mutex_unlock(&GL);
            break;
        }
        
        pthread_mutex_unlock(&GL);
        
        // work goes here
        
        
        
        
        Vector* commands = rule->commands;
        
        int failed = 0;
        
        for (size_t i = 0; i < Vector_size(commands); i++) {
            
            int res = system(Vector_get(commands, i));
            if (res != 0 || !WIFEXITED(res)) {
                failed = 1;
                break;
            }
            
        }
        
        if (failed) {
            rule->state = -1; // 1 = success, -1 = failed, 0 = pristine
        }
        else{
            rule->state = 1;
        }
        
        //work goes here
        
        pthread_mutex_lock(&RM);
        RULES_ADDED--;
        pthread_mutex_unlock(&RM);
        
        pthread_cond_broadcast(&CV);
        
    }
    
    
    return NULL;
}


int parmake(int argc, char **argv) {
    
    char* filename = NULL;
    int num_threads = 0;
    char** targets;
    
    
     /*********************************** GET OPTIONS ************************/
    int opt, res;
    while ((opt = getopt (argc, argv, "f:j:")) != -1)
        switch (opt){
        case 'f':
                res = access(optarg, F_OK | R_OK );
                if (!res)
                    filename = optarg;
                else{ // if file passes does not exist/have proper permissions
                    fprintf(stderr, "Bad file argument!\n");
                    return 1;
                }
                break;
        case 'j':
                num_threads = atoi(optarg);
                break;
        case '?':
                return 1;
        default:
            return 1;
    }
    
    //if No file was specified in options then use the default
    if (!filename) {
        
        int res = access("makefile", F_OK);
        if (!res) {
            filename = "makefile";
        }
        else{
            if (  (res = access("Makefile", F_OK))  == 0 ) {
                
                filename = "Makefile";
            }
            //no default makefiles exist -- hence throw an error and end program
            else{
                fprintf(stderr, "No files were able to be opened!\n");
                return 1;
            }
        }
        
        
    }
    // default number of threads
    if (!num_threads) {
        num_threads = 1;
    }
    
    
    if (optind >= argc)
        targets = NULL;
    else
        targets = argv + optind;
    
    //debugging prints
    //printf("Filename: %s\n", filename);
    //printf("Number of threads: %d\n", num_threads);
    
    
    /*********************************** END OPTIONS ************************/
    
    /*
     * solution model -> we will implement this in a fetch-then-pass fashion.
     * first we will have to fetch all the rules that are available to be run. This will initially
     * be all the rules with no dependencies to begin with. Think of it as satisfying rules in stages.
     * 1st stage will allow for 2nd to be successfuly run, 2nd will allow for 3rd, etc... Faiures in a
     * lower stage need to be propogated up to higher stages in order to automaticaly fail those rules
     * as well. lets do this.
     *
     */
    
    //initialize vector to simply pass pointers around - no data copying inside vector
    rule_list = Vector_create(copy_pointer, destroy_pointer);
    parser_parse_makefile(filename, targets,parsed_new_target);
    queue_t* queue = queue_create(-1, copy_pointer, destroy_pointer);

    
    pthread_t threads[num_threads];
    for (int i = 0; i < num_threads; i++) {
        ThreadInfo* ti = (ThreadInfo*)malloc(sizeof(ThreadInfo));
        ti->queue = queue;
        pthread_create(&threads[i], 0, process, (void*)ti);
    }
    
    
    
    
    size_t processed = 0;
    size_t rules_to_process = Vector_size(rule_list);
    
    //while there are still rules that need to be satisfied
    while (processed < rules_to_process) {
        
        // here we need to add all the rules to the queue that have not yet been satisfied,
        // and all their dependencies have been satisfied. This will work in stages. Will
        // probably need a barrier here.
        for (size_t i = 0; i < Vector_size(rule_list); i++) {
            
            //current rule
            rule_t* rule = Vector_get(rule_list, i);
            
            // No need to look at the rules that have already been processed
            if (rule->state == 0) {
                // return true if all dependencies of the rule have state other than zero
                if ( dependencies_touched(rule) ) {
                    if (dependencies_failed(rule)) {
                        rule->state = -1;
                        processed++;
                        continue;
                    }
                    // if the rule is a is a file on disk
                    if( access(rule->target, F_OK | R_OK) == 0 ){
                        Vector* deps = rule->dependencies;
                        //if all the dependencies are files
                        if ( !not_file_on_disk(deps) ) {
                            //if none of the files are newer
                            if (!dependencies_newer(rule)) {
                                rule->state = 1;
                                processed++;
                                continue;
                                
                            }
                        }
                    }
                    
                    // TODO:
                    //  1.) account for files
                    // so we can know when to wake up the main thread!
                    // this variable only keeps track of the current stage
                    pthread_mutex_lock(&RM);
                    RULES_ADDED++;
                    pthread_mutex_unlock(&RM);
                    
                    //give the rule to the threads
                    queue_push(queue, rule);
                    

                    // number of rules processed by workers increases -> whether
                    // they fail or succeed
                    processed++;
                }
            }
        }
        
        // end of a single stage -> we need to wait for threads to finish procesing tasks at this point
        // and continue on to the next loop
        pthread_mutex_lock(&RM);
        
        if (RULES_ADDED > 0) {
            
            while (RULES_ADDED > 0)
                pthread_cond_wait(&CV, &RM);
            
        }
        
        pthread_mutex_unlock(&RM);
        
        
        
    }
    
    //MAKE SURE TO FREE THIS
    rule_t* last_rule = (rule_t*)malloc(sizeof(rule_t));
    last_rule->state = -99;
    
    //pushed the poisin pill to the queue to make all threads exit
    queue_push(queue, last_rule);
    
    
    for (int i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);
    
    free(queue);
    free(last_rule);
   
    for (size_t i = 0; i < Vector_size(rule_list); i++){
        rule_destroy(Vector_get(rule_list, i));
        free(Vector_get(rule_list, i));
    }
    
    Vector_destroy(rule_list);
    
    /** Make sure to free all rules and make sure to free all thread tasks as well!! **/
    
    return 0;
}
