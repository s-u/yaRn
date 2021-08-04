/* Simple threaded TCP server (therver), based on a
   FIFO queue and worker threads. It is intended to
   run entirely in a parallel to the main thread.
   Also intentionally it uses static variables,
   the only dynamically allocated pieces are
   a) the thread pool and b) the queue entries

   Author and (c) Simon Urbanek <urbanek@R-project.org>
   License: MIT

   (work in progress, ports expected eventually)
*/

/* --- interface --- */

typedef struct conn_s {
    int s; /* socket to the client */
    void *data; /* opaque per-thread pointer */
} conn_t;

/* The process(conn_t*) API:
   You don't own the parameter, but it is guaranteed
   to live until you return. If you close the socket
   you must also set s = -1 to indicate you did so,
   otherwise the socket is automatically closed. */
typedef void (*process_fn_t)(conn_t*);

/* Binds host/port, then starts threads, host can be NULL for ANY.
   Returns non-zero for errors. */
int therver(const char *host, int port, int max_threads, process_fn_t process_fn);


/* ------ cut here ------ */

/* --- implementation --- */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>
#include <pthread.h>

#define FETCH_SIZE (512*1024)

#define SOCKET int
#define closesocket(X) close(X)

volatile int active = 1;

static int ss;

static process_fn_t process;

static pthread_t *worker_threads;
static pthread_t accept_thread;

typedef struct qentry_s {
    /* used for queuing */
    struct qentry_s *prev, *next;
    /* connection info */
    conn_t c;
} qentry_t;

static qentry_t root;
static pthread_mutex_t pool_mutex;
static pthread_cond_t pool_work_cond;

static void *worker_thread(void *pool_arg) {
    qentry_t *me;
    void *data = 0;
    /* printf("worker_thread %p is a go\n", (void*)&me); */
    while (active) {
	/* lock queue mutex */
	pthread_mutex_lock(&pool_mutex);
	/* printf("worker %p waiting\n", (void*)&me); */
	
	/* wait on condition until we get work */
	while (!(me = root.next) || me == &root) 
	    pthread_cond_wait(&pool_work_cond, &pool_mutex);

	/* remove us from the queue */
	root.next = me->next;
	if (me->next) me->next->prev = &root;
	/* we don't care to update our prev/next since we never use it */

	/* release queue lock */
	pthread_mutex_unlock(&pool_mutex);

	/* printf("worker %p calling process() with s=%d\n", (void*)&me, me->c.s); */
	me->c.data = data;
	/* serve the connection */
	process(&me->c);
	data = me->c.data;

	/* clean up */
	if (me->c.s != -1)
	    close(me->c.s);
	/* printf("worker %p is done\n", (void*)&me); */
	free(me);
    }
    return 0;
}

/* me must be free()-able and we take ownership */
static int add_task(qentry_t *me) {
    /* printf("add_task(%d) about to lock\n", me->c.s); */
    pthread_mutex_lock(&pool_mutex);
    /* printf(" add_task() locked, adding\n"); */
    me->next = &root;
    me->prev = root.prev;
    if (me->prev) me->prev->next = me;
    root.prev = me;
    /* printf(" add_task() broadcasting\n"); */
    pthread_cond_broadcast(&pool_work_cond);
    pthread_mutex_unlock(&pool_mutex);
    /* printf(" add_task() unlocked\n"); */
    return 0;
}

/* this is not absolutely safe, but fork() is asking for trouble anyway,
   so we only make sure that the child does not process anything and
   closes all its sockets. If any communications are in-flight,
   it's anyone's guess what will hapen since we don't try to join the threads
   before forking (which would be the only way to do this safely).
*/
static void forked_child() {
    qentry_t *me;
    /* make accept thread quit */
    active = 0;
    /* close server socket */
    closesocket(ss);
    ss = -1;
    /* close and reset all sockets in the queue */
    me = root.next;
    while (me && me != &root) {
	if (me->c.s != -1)
	    closesocket(me->c.s);
	me->c.s = -1;
	me = me->next;
    }
}

/* thread for the incoming connections */
static void *accept_thread_run(void *nothing) {
    int s;
    socklen_t cli_al;
    struct sockaddr_in sin_cli;
    /* printf("accept_thread %p is a go\n", (void*)&s); */
    while (active) {
	cli_al = sizeof(sin_cli);
	s = accept(ss, (struct sockaddr*) &sin_cli, &cli_al);
	/* printf("accept_thread: accept=%d\n", s); */
	if (s != -1) {
	    qentry_t *me = (qentry_t*) calloc(1, sizeof(qentry_t));
	    if (me) {
		/* once enqueued the task takes ownership of me.
		   On any kind of error we have to free it. */
		/* printf(" - accept_thread got me, enqueuing\n"); */
		me->c.s = s;
		if (add_task(me)) {
		    /* printf(" - add_task() failed, oops\n"); */
		    free(me);
		    me = 0;
		    close(s);
		}
	    } else /* sorry, out of memory, over and out */
		close(s);
	}
    }
    close(ss);
    ss = -1;
    return 0;
}

static int start_threads(int max_threads) {
    sigset_t mask, omask;
    pthread_attr_t t_attr;
    pthread_attr_init(&t_attr); /* all out threads are detached since we don't care */
    pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);

    root.next = root.prev = &root;
    root.c.s = -1;

    if (!(worker_threads = malloc(sizeof(pthread_t) * max_threads)))
	return -1;

    /* init cond/mutex */
    pthread_mutex_init(&pool_mutex, 0);
    pthread_cond_init(&pool_work_cond, 0);

    /* mask all signals - the threads will inherit the mask
       and thus not fire and leave it to R */
    sigfillset(&mask);
    sigprocmask(SIG_SETMASK, &mask, &omask);

    /* start worker threads */
    for (int i = 0; i < max_threads; i++)
	pthread_create(&worker_threads[i], &t_attr, worker_thread, 0);

    /* start accept thread */
    pthread_create(&accept_thread, &t_attr, accept_thread_run, 0);

    /* re-set the mask back for the main thread */
    sigprocmask(SIG_SETMASK, &omask, 0);

    /* in case the user uses multicore or something else, we want to shut down
       all proessing in the children (not perfect, see comments above) */
    pthread_atfork(0, 0, forked_child);

    return 0;
}

int therver(const char *host, int port, int max_threads, process_fn_t process_fn) {
    int i;
    struct sockaddr_in sin;
    struct hostent *haddr;

    ss = socket(AF_INET, SOCK_STREAM, 0);

    i = 1;
    setsockopt(ss, SOL_SOCKET, SO_REUSEADDR, (const char*)&i, sizeof(i));

    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    if (host) {
        if (inet_pton(sin.sin_family, host, &sin.sin_addr) != 1) { /* invalid, try DNS */
            if (!(haddr = gethostbyname(host))) { /* DNS failed, */
                closesocket(ss);
                ss = -1;
            }
            sin.sin_addr.s_addr = *((uint32_t*) haddr->h_addr); /* pick first address */
        }
    } else
        sin.sin_addr.s_addr = htonl(INADDR_ANY);

    if (ss != -1 && (bind(ss, (struct sockaddr*)&sin, sizeof(sin)) || listen(ss, 8))) {
        closesocket(ss);
	ss = -1;
    }
    if (ss == -1) {
        perror("ERROR: failed to bind or listen");
        return 1;
    }

    process = process_fn;
    
    return start_threads(max_threads);
}

/* client */

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#include <Rinternals.h>

#define MAX_BUF  65536
#define MAX_OBUF 2048
#define MAX_SEND (1024*1024) /* 1Mb */

typedef struct {
    int  bol, n;
    char buf[MAX_BUF];
    char obuf[MAX_OBUF];
} work_t;

typedef unsigned long int obj_len_t;

typedef struct hash_elt {
    const char *key_ptr; /* for hash */
    struct hash_elt *next;
    obj_len_t len;
    void *obj;
    char key[1];
} entry_t;

static pthread_mutex_t obj_mutex;

/* hash API */
typedef struct hash hash_t;
typedef struct hash_elt hash_elt_t;
typedef unsigned int hash_value_t;

hash_t *new_hash(hash_value_t len, double max_load);
hash_elt_t *hash(hash_t *h, const char *key, hash_elt_t *val, int rm);

/* FIXME: use hash */
static hash_t  *obj_hash;
static entry_t *obj_gc_pool;

static void obj_add_buf(const char *key, void *data, obj_len_t len) {
    entry_t *e = (entry_t*) calloc(1, sizeof(entry_t) + strlen(key));
    strcpy(e->key, key);
    e->key_ptr = e->key;
    e->len = len;
    e->obj = data;
    pthread_mutex_lock(&obj_mutex);
    e->next = (entry_t*) hash(obj_hash, key, (hash_elt_t*)e, 0);
    if (e->next == e) e->next = 0; /* no loops */
    pthread_mutex_unlock(&obj_mutex);
}

/* NOTE: the ownership is transferred ! */
static void obj_add(entry_t *e) {
    pthread_mutex_lock(&obj_mutex);
    e->key_ptr = e->key; /* make sure the object is complete */
    e->next = (entry_t*) hash(obj_hash, e->key, (hash_elt_t*)e, 0);
    if (e->next == e) e->next = 0; /* no loops */
    pthread_mutex_unlock(&obj_mutex);
}

static void obj_gc() {
    pthread_mutex_lock(&obj_mutex);
    while (obj_gc_pool) {
	entry_t *c = obj_gc_pool; 
        obj_gc_pool = obj_gc_pool->next;
        free(c);
    }
    pthread_mutex_unlock(&obj_mutex);
}

static entry_t *obj_get(const char *key, int rm) {
    pthread_mutex_lock(&obj_mutex);
    entry_t *e = (entry_t*) hash(obj_hash, key, 0, 0);
    if (e) {
	if (rm) {
	    /* it's a bit annoying, to do rm we have to
	       do a second pass since we don't know whether
	       to replace or delete until we retrieve
	       the entry; for performance it would be
	       nice to keep the address so we don't need
	       to look it up twice - FIXME */
	    if (e->next) /* replace with the next entry */
		hash(obj_hash, key, e->next, 0);
	    else /* nothing else, just remove */
		hash(obj_hash, key, 0, 1);
	    e->next = obj_gc_pool;
	    obj_gc_pool = e;
	}
	pthread_mutex_unlock(&obj_mutex);
	return e;
    }
    pthread_mutex_unlock(&obj_mutex);
    return 0;
}

static int send_buf(int s, const char* buf, obj_len_t len) {
    while (len) {
	int ts = (len > MAX_SEND) ? MAX_SEND : ((int) len);
	int n = send(s, buf, ts, 0);
	if (n < 1)
	    return (n < 0) ? -1 : 1;
	len -= n;
	buf += n;
    }
    return 0;
}

static void do_process(conn_t *c) {
    int s = c->s, n;
    char *d, *e, *a;
    unsigned char hdr[16];
    
    if (s < 0) return;

    {
        int opt = 1;
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const void*) &opt, sizeof(opt));
    }

    while (1) {
	char cmd;
	obj_len_t lKey = 0, lObj = 0;
	n = recv(s, &cmd, 1, 0);
	if (n != 1)
	    break;
	switch (cmd) {
	case 'A': /* get, 8-bit */
	case 'C': /* rm,  8-bit */
	    n = recv(s, hdr, 1, 0);
	    if (n < 1)
		break;
	    lKey = (obj_len_t) hdr[0];
	    cmd = (cmd == 'A') ? 'g' : 'r';
	    break;
	case 'B': /* get, 16-bit */
	case 'D': /* rm,  16-bit */
	    n = recv(s, hdr, 2, 0);
	    if (n < 2)
		break;
	    lKey = ((obj_len_t) hdr[0]) | (((obj_len_t) hdr[1]) << 8);
	    cmd = (cmd == 'B') ? 'g' : 'r';
	    break;
	case 'E': /* set, 8 + 8 */
	    n = recv(s, hdr, 2, 0);
	    if (n < 2)
		break;
	    lKey = (obj_len_t) hdr[0];
	    lObj = (obj_len_t) hdr[1];
	    cmd = 's';
	    break;
	case 'F': /* st, 16 + 16 */
	    n = recv(s, hdr, 4, 0);
	    if (n < 4)
		break;
	    lKey = ((obj_len_t) hdr[0]) | (((obj_len_t) hdr[1]) << 8);
	    lObj = ((obj_len_t) hdr[2]) | (((obj_len_t) hdr[3]) << 8);
	    cmd = 's';
	    break;
	case 'G': /* st, 16 + 32 */
	    n = recv(s, hdr, 6, 0);
	    if (n < 6)
		break;
	    lKey = ((obj_len_t) hdr[0]) | (((obj_len_t) hdr[1]) << 8);
	    lObj = ((obj_len_t) hdr[2]) | (((obj_len_t) hdr[3]) << 8) | (((obj_len_t) hdr[4]) << 16) | (((obj_len_t) hdr[5]) << 24);
	    cmd = 's';
	    break;
	default:
	    cmd = '?';
	}

	if (cmd != '?') { /* valid command, read key + obj */
	    obj_len_t lTotal = lKey + lObj + 1, l = 0;
	    entry_t *e = calloc(1, sizeof(e) + lKey + lObj + 1);
	    if (!e) {
		fprintf(stderr, "ERROR: out of memory when allocating for %ld + %ld\n", (long) lKey, (long) lObj);
		break;
	    }
	    while (l < lKey) {
		n = recv(s, e->key + l, lKey - l, 0);
		Rprintf("  read key: @%d:%d -> %d\n", (int) l, (int) lKey, n);
		if (n < 1) {
		    fprintf(stderr, "ERROR: incomplete packet (%ld of %ld yields %d)\n", (long) l, (long) lTotal, n);
		    break;
		}
		l += n;
	    }

	    if (l < lKey)
		break;

	    /* terminate the key */
	    e->key[l++] = 0;
	    e->obj = (void*)(e-> key + l);
	    e->len = lObj;

	    while (l < lTotal) {
		n = recv(s, e->key + l, lTotal - l, 0);
		Rprintf("  read obj: @%d:%d -> %d\n", (int) l, (int) lTotal, n);
		if (n < 1) {
		    fprintf(stderr, "ERROR: incomplete packet (%ld of %ld yields %d)\n", (long) l, (long) lTotal, n);
		    break;
		}
		l += n;
	    }

	    if (cmd == 'g' || cmd == 'r') {
		entry_t *o = obj_get(e->key, (cmd == 'r') ? 1 : 0);
		free(e);
		if (o) {
		    hdr[0] = '2'; /* 16-bit resp */
		    hdr[1] = (unsigned char) (o->len & 0xff);
		    hdr[2] = (unsigned char) ((o->len >> 8) & 0xff);
		    if (send_buf(s, hdr, 3))
			break;
		    if (send_buf(s, o->obj, o->len))
			break;
		} else {
		    hdr[0] = '0';
		    if (send_buf(s, hdr, 1))
			break;
		}
	    } else if (cmd == 's') {
		obj_add(e);
	    }
	}
    }
    closesocket(s);
    c->s = -1;
}

#include <Rinternals.h>

static int init_pt;

static void do_init() {
    if (!init_pt) {
	pthread_mutex_init(&obj_mutex, 0);
	obj_hash = new_hash(1024*128, 0.85);
	init_pt = 1;
    }
}

SEXP C_start(SEXP sHost, SEXP sPort, SEXP sThreads) {
    const char *host = (TYPEOF(sHost) == STRSXP && LENGTH(sHost) > 0) ?
	CHAR(STRING_ELT(sHost, 0)) : 0;
    int port = Rf_asInteger(sPort);
    int threads = Rf_asInteger(sThreads);

    if (port < 1 || port > 65535)
	Rf_error("Invalid port %d", port);
    if (threads < 1 || threads > 1000)
	Rf_error("Invalid number of threads %d", threads);

    do_init();
    if (therver(host, port, threads, do_process))
	return ScalarLogical(0);

    /* printf("Started on %s:%d, try me.\n", host ? host : "*", port); */

    return ScalarLogical(1);
}

SEXP C_put(SEXP sKey, SEXP sWhat) {
    if (TYPEOF(sKey) != STRSXP || LENGTH(sKey) != 1)
	Rf_error("Invalid key, must be a string");
    if (TYPEOF(sWhat) != RAWSXP)
	Rf_error("Value must be a raw vector");
    do_init();
    obj_add_buf(CHAR(STRING_ELT(sKey, 0)), RAW(sWhat), XLENGTH(sWhat));
    return ScalarLogical(1);
}

SEXP C_get(SEXP sKey, SEXP sRm) {
    int rm = asInteger(sRm);
    if (TYPEOF(sKey) != STRSXP || LENGTH(sKey) != 1)
	Rf_error("Invalid key, must be a string");
    do_init();
    entry_t *o = obj_get(CHAR(STRING_ELT(sKey, 0)), rm);
    if (o) {
	SEXP r = Rf_allocVector(RAWSXP, o->len);
	memcpy(RAW(r), o->obj, o->len);
	obj_gc();
	return r;
    }
    return R_NilValue;
}

SEXP C_rm(SEXP sKey) {
    if (TYPEOF(sKey) != STRSXP || LENGTH(sKey) != 1)
	Rf_error("Invalid key, must be a string");
    do_init();
    entry_t *o = obj_get(CHAR(STRING_ELT(sKey, 0)), 1);
    return ScalarLogical(o ? 1 : 0);
}

SEXP C_clean() {
    do_init();
    obj_gc();
    return ScalarLogical(1);
}

static void Rsend(int ss, const char *buf, obj_len_t len) {
    obj_len_t i = 0;
    while (i < len) {
	int n = send(ss, buf + i, len - i, 0);
	
	if (n < 1) {
	    closesocket(ss);
	    if (n < 0)
		Rf_error("Error sending command %s", errno ? strerror(errno) : "");
	    Rf_error("Error sending command, could only send %ld of %ld bytes", (long) i, (long) len);
	}
	
	i += n;
    }
}

static void Rread(int ss, char *buf, obj_len_t len) {
    obj_len_t i = 0;
    Rprintf("Rread(%d)\n", (int) len);
    while (i < len) {
	int n = recv(ss, buf + i, len - i, 0);
	Rprintf("  recv(@%d,%d)=%d\n", i, len - i, n);
	if (n <= 0) {
	    /* we have set timeout as to allow interrupts, HOWEVER,
	       the socket will NOT be closed after an interrupt! */
	    if (errno == EAGAIN || errno == EWOULDBLOCK) {
		R_CheckUserInterrupt();
		continue;
	    }
	    closesocket(ss);
	    if (n == 0) {
		Rf_error("Connection closed unexpectedly");
	    }
	    Rf_error("Error while receiving response %s", errno ? strerror(errno) : "");
	}
	i += n;
    }
}

SEXP C_ask(SEXP sHost, SEXP sPort, SEXP sCmd, SEXP sKey, SEXP sObj) {
    SOCKET ss;
    int n, l, i = 1, port = asInteger(sPort), has_obj = 0;
    obj_len_t lKey = 0, lObj = 0;
    char cmd = 0;
    const char *host = 0;
    struct sockaddr_in sin;
    struct hostent *haddr;
    struct timeval tv;

    if (TYPEOF(sHost) != STRSXP || LENGTH(sHost) != 1)
	Rf_error("host must be a string");
    if (TYPEOF(sCmd) != STRSXP || LENGTH(sCmd) != 1)
	Rf_error("command must be a string");
    if (TYPEOF(sKey) != RAWSXP &&
	(TYPEOF(sKey) != STRSXP || LENGTH(sKey) != 1))
	Rf_error("key must be string or a raw vector");

    cmd = CHAR(STRING_ELT(sCmd, 0))[0];

    if (cmd == 'g' || cmd == 'r' || cmd == 's') {
	if (TYPEOF(sKey) == RAWSXP)
	    lKey = LENGTH(sKey);
	else
	    lKey = strlen(CHAR(STRING_ELT(sKey, 0)));
	if (lKey > 0xffff)
	    Rf_error("Key is too long, only 16-bit key length is supported");

	if (cmd == 's') {
	    if (TYPEOF(sObj) == RAWSXP)
		lObj = XLENGTH(sObj);
	    else if (TYPEOF(sObj) == STRSXP && LENGTH(sObj) == 1)
		lObj = strlen(CHAR(STRING_ELT(sObj, 0)));
	    else
		Rf_error("object must be a string or raw vector");
	    has_obj = 1;
	}

	if (cmd == 'g')
	    cmd = 'B'; /* 16-bit get */
	else if (cmd == 'r')
	    cmd = 'D'; /* 16-bit rm */
	else
	    cmd = 'G'; /* 16 + 32-bit set */
    } else {
	Rf_error("Invalid command, it must be one of get, set, remove");
    }
    
    host = CHAR(STRING_ELT(sHost, 0));
    if (port < 0 || port > 65535)
	Rf_error("invalid port");

    ss = socket(AF_INET, SOCK_STREAM, 0);
    if (ss == -1)
	Rf_error("Cannot obtain a socket %s", errno ? strerror(errno) : "");

    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    if (host) {
        if (inet_pton(sin.sin_family, host, &sin.sin_addr) != 1) { /* invalid, try DNS */
            if (!(haddr = gethostbyname(host))) { /* DNS failed, */
                closesocket(ss);
                ss = -1;
		Rf_error("Cannot resolve host '%s'", host);
            }
            sin.sin_addr.s_addr = *((uint32_t*) haddr->h_addr); /* pick first address */
        }
    } else
        sin.sin_addr.s_addr = htonl(INADDR_ANY);

    if (connect(ss, (struct sockaddr*)&sin, sizeof(sin)))
	Rf_error("Unable to connect to %s:%d %s", host, port, errno ? strerror(errno) : "");

    /* enable TCP_NODELAY */
    setsockopt(ss, IPPROTO_TCP, TCP_NODELAY, (const void*) &i, sizeof(i));

    /* enable timeout so we can support R-level interrupts */
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(ss, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    unsigned char hdr[8];
    hdr[0] = cmd;
    hdr[1] = (unsigned char) (lKey & 0xff);
    hdr[2] = (unsigned char) ((lKey >> 8) & 0xff);
    if (has_obj) {
	hdr[3] = (unsigned char) (lKey & 0xff);
	hdr[4] = (unsigned char) ((lKey >> 8) & 0xff);
	hdr[5] = (unsigned char) ((lKey >> 16) & 0xff);
	hdr[6] = (unsigned char) ((lKey >> 24) & 0xff);
	Rsend(ss, hdr, 7);
    } else
	Rsend(ss, hdr, 3);
    
    Rsend(ss, (TYPEOF(sKey) == RAWSXP) ? (char*) RAW(sKey) : CHAR(STRING_ELT(sKey, 0)), lKey);
    if (has_obj)
      Rsend(ss, (TYPEOF(sObj) == RAWSXP) ? (char*) RAW(sObj) : CHAR(STRING_ELT(sObj, 0)), lObj);

    if (!has_obj) { /* response expected */
	Rread(ss, hdr, 1);
	if (*hdr == '0') {
	    closesocket(ss);
	    return R_NilValue;
	}
	if (*hdr == '2') {
	    SEXP res;
	    Rread(ss, hdr, 2);
	    lObj = ((obj_len_t)hdr[0]) | (((obj_len_t)hdr[1]) << 8);
	    res = allocVector(RAWSXP, lObj);
	    Rread(ss, RAW(res), lObj);
	    closesocket(ss);
	    return res;
	}
	if (*hdr == '3') {
	    SEXP res;
	    Rread(ss, hdr, 4);
	    lObj = ((obj_len_t)hdr[0]) | (((obj_len_t)hdr[1]) << 8) | (((obj_len_t)hdr[2]) << 16) | (((obj_len_t)hdr[3]) << 24);
	    res = allocVector(RAWSXP, lObj);
	    Rread(ss, RAW(res), lObj);
	    closesocket(ss);
	    return res;
	}

	closesocket(ss);
	hdr[1] = 0;
	Rf_error("Invalid response: %s", hdr);
    }
    closesocket(ss);
    return ScalarLogical(1);
}
