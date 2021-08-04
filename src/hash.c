#include <stdlib.h>
#include <stdint.h>
#include <string.h>

/* temporarily for errors */
#include <stdio.h>

/* -- interface -- */

typedef struct hash hash_t;
typedef struct hash_elt hash_elt_t;
typedef unsigned int hash_value_t;

/* hash_elt struct must start with
   const char *key;                                                                                                                                                           **/

hash_t *new_hash(hash_value_t len, double max_load);
hash_elt_t *hash(hash_t *h, const char *key, hash_elt_t *val, int rm);

/* -- implementation -- */

struct hash_elt {
    const char *key;
};

struct hash {
    hash_value_t m, els;   /* hash size, added elements */
    hash_value_t max_load; /* max. load - resize when reached */
    int k;                 /* used bits, payload type */
    hash_elt_t *ix[1];
};

hash_t *new_hash(hash_value_t len, double max_load) {
    hash_t *h;
    int k = 8; /* force a minimal size of 256 */
    hash_value_t m = 1 << k;
    hash_value_t max_load_;
    while (m < len) { m *= 2; k++; }
    max_load_ = (hash_value_t) (((double) m) * max_load);
    h = (hash_t*) calloc(1, sizeof(hash_t) + (sizeof(hash_elt_t*) * m));
    if (!h)
	return 0;
	/* Rf_error("unable to allocate %.2fMb for a hash table",
	   (double) sizeof(hash_value_t) * (double) m / (1024.0 * 1024.0)); */
	
    h->max_load = max_load_;
    h->m = m;
    h->k = k;
    return h;
}

/* pi-hash fn */
#define HASH(X) (3141592653U * ((unsigned int)(X)) >> (32 - h->k))

/* h   - mandatory
   key - if set and val = NULL then this is a get
   val - if set, this is add/replace
   rm  - if non-zero then this is remove */
hash_elt_t *hash(hash_t *h, const char *key, hash_elt_t *val, int rm) {
    if (!h)
	return 0;

    if (val)
	key = val->key;

    hash_value_t hv = 0;

    int n = strlen(key), i = 0;
    while (i + 3 < n) {
	hv ^= HASH(*((hash_value_t*)(key + i)));
	i += 4;
    }
    if (i < n) {
	char tm[4] = { 0, 0, 0, 0};
	memcpy(tm, key + i, n - i);
	hv ^= HASH(*((hash_value_t*)tm));
    }
    
    while (h->ix[hv] && strcmp(h->ix[hv]->key, key)) {
        hv++;
        if (hv == h->m) hv = 0;
    }

    if (h->ix[hv]) {
	hash_elt_t *old = h->ix[hv];
	if (rm) {
	    h->ix[hv] = 0;
	    return old;
	}
	if (val) { /* replace */
	    h->ix[hv] = val;
	    return old;
	}
	/* get */
	return h->ix[hv];
    }
    if (!h->ix[hv]) {
	if (!val) /* no val? -> get/rm -> not found */
	    return 0;
	/* otherise this is add */
	if (h->els == h->max_load) {
	    fprintf(stderr, "Maximal hash load reached, resizing is currently unimplemented");
	    return 0;
	}
	h->els++;    
        h->ix[hv] = val;
    }
    return val;
}
