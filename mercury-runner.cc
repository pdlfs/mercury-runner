/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * mercury-runner.cc  run one or more mercury instances in the same process
 * 01-Feb-2017  chuck@ece.cmu.edu
 */

/*
 * this program contains both a mercury RPC client and RPC server.
 * the client sends "count" number of RPC requests and exits when
 * all replies are in.  The server receives "count" number of RPC
 * requests and exits when all requests have been processed.
 *
 * to use the program you need to run two copies of it.  by
 * default both client and server are active so the RPCs flow
 * in both directions.   if you only want a one way flow of RPC
 * calls run one copy as a client and one as a server.
 *
 * the program can run multiple instances of mercury in the same
 * process.   server port numbers are assigned sequentially starting
 * at g.baseport (default defined below as 19900).  (the command line
 * address specs uses a printf "%d" to fill the port number...)
 * for client-only mode, we init the client side with ports after
 * the server ports...
 *
 * note: the number of instances between the client and server
 * should match.
 *
 * by default the client side of the program sends as many RPCs
 * as possible in parallel.  you can limit the number of active RPCs
 * using the "-l" flag.  specifying "-l 1" will cause the client side
 * of the program to fully serialize all RPC calls.
 *
 * usage: mercury-runner [options] ninst localspec [remotespec]
 *
 * options:
 *  -c count     number of RPCs to perform
 *  -D count     client prints status every "count" RPCs
 *  -d dir       shared directory to pass server address through
 *  -g           use MPI gather to get remote addr (MPI only)
 *  -l limit     limit # of concurrent client RPC requests ("-l 1" = serial)
 *  -M           run mercury-runner under mpi (MPI mode)
 *  -m mode      c, s, cs (client, server, or both)
 *  -P           poll mode (NA_NO_BLOCK)
 *  -p baseport  base port number
 *  -q           quiet mode - don't print during RPCs
 *  -r n         enable tag suffix with this run number
 *  -s file      save copy of our output in this file
 *  -t secs      timeout (alarm)
 *
 * size related options:
 * -i size     input req size (>= 8 if specified)
 * -o size     output req size (>= 8 if specified)
 * -L size     server's local rma buffer size
 * -S size     client bulk send sz (srvr RMA reads)
 * -R size     client bulk recv sz (srvr RMA writes)
 * -O          one buffer flag (valid if -S and -R set)
 * -X count    client call handle cache max size (0=unlimited,-1=no cache)
 * -Y count    server reply handle cache max size (0=unlimited,-1=no cache)
 *
 * default payload size is 4.
 * using -O causes the server to RMA read and write to the
 * same buffer (client exports in in RDWR mode).
 *
 * the client/server handle caches are used to cache preallocated
 * mercury handles between calls.
 *
 * note that "remotespec" is optional if mode is "s" (server-only)
 *
 * examples:
 *   one client and one server mode, serialized sending, one instance:
 *
 *    client:
 *    ./mercury-runner -l 1 -c 50 -q -m c 1 cci+tcp://10.93.1.210:%d \
 *                           cci+tcp://10.93.1.233:%d
 *    server:
 *    ./mercury-runner -c 50 -q -m s 1 cci+tcp://10.93.1.233:%d
 *
 * note that the -c's must match on both sides
 *
 *   both processes send and recv RPCs (client and server), one
 *   instance, both sides sending in parallel:
 *
 *    ./mercury-runner -c 50 -q -m cs 1 cci+tcp://10.93.1.210:%d \
 *                           cci+tcp://10.93.1.233:%d
 *
 *    ./mercury-runner -c 50 -q -m cs 1 cci+tcp://10.93.1.233:%d \
 *                           cci+tcp://10.93.1.210:%d
 *
 * when using "-d":
 *    localspec should be tag=<mercury-url>
 *    and remotespec should just be the remote tag (it will read the
 *    actual data from the directory).
 *
 * examples using "-d" to exchange address info:
 *
 *    mpirun ./mercury-runner -l 16 -d `pwd` -q -c 1000 -m cs \
 *                             1 h0=mpi+dynamic h1
 *    mpirun ./mercury-runner -l 16 -d `pwd` -q -c 1000 -m cs   \
 *                             1 h1=mpi+dynamic h0
 *
 *    mpirun ./mercury-runner -l 16 -d `pwd` -q -c 1000 -m s 1 h0=mpi+dynamic
 *    mpirun ./mercury-runner -l 16 -d `pwd` -q -c 1000 -m c    \
 *                             1 h1=mpi+dynamic h0
 *
 *
 * ./mercury-runner -l 16 -d `pwd` -q -c 1000 -m cs 1 h0=bmi+tcp h1
 * ./mercury-runner -l 16 -d `pwd` -q -c 1000 -m cs 1 h1=bmi+tcp h0
 *
 * for MPI mode: we must be run in an MPI world with exactly 2 procs.
 * (in this case MPI is being used to launch mercury-runner but may
 * not be used for transport..)    rank 0 becomes the "local" proc
 * and rank 1 becomes the "remote" proc.  we assume that mpirun
 * has been told to use the hosts that match the local and remote
 * specs on the command line.
 *
 * bidirectional:
 *   mpirun -n 2 -ppn 1 --host h0,h1 \
 *      ./mercury-runner -c 3 -l 1 -M -m cs -q -s /tmp/llogg \
 *      1 bmi+tcp://h0:5555 bmi+tcp://h1:5556
 *
 * single direction:
 *   mpirun -n 2 -ppn 1 --host h0,h1 \
 *      ./mercury-runner -c 3 -l 1 -M -m c -q -s /tmp/llogg \
 *      1 bmi+tcp://h0:5555 bmi+tcp://h1:5556
 *
 * for single direction, rank 1 has its mode toggled from the given -m
 * value.  also, you must specify both remote and local specs when
 * using MPI mode (even for "-m s").
 *
 * for transports that do not allow you to specify a listening port
 * at HG_Init() time (e.g. MPI, newer version of ofi+gni), we need
 * to get the actual address used using HG_Addr_self() and exchange
 * that through some out of band mechanism in order for clients to
 * have the full address to connect to.  there are two options:
 *   1. use a network file system directory to exchange this information
 *      with the "-d dir" flag
 *   2. if using MPI mode (-M) use the "-g" flag to use MPI gather ops
 *      to exchange address strings between endpoints.
 *
 * note that for MP mode, if "-s" is used to save output a ".N" is
 * appended to the filename (where N is either 0 or 1 based on the rank).
 */

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_log.h>
#include <mercury_macros.h>

#ifdef MERCURY_PROGRESSOR
#include <mercury-progressor/mercury-progressor.h>
#endif

#ifdef MPI_RUNNER
#include <mpi.h>
#endif

/*
 * default values for port and count
 */
#define DEF_BASEPORT 19900 /* starting TCP port we listen on (instance 0) */
#define DEF_COUNT 5        /* default # of msgs to send and recv in a run */
#define DEF_TIMEOUT 120    /* alarm timeout */

/* operation modes - can be used as a bitmask or a value */
#define MR_CLIENT 1        /* client */
#define MR_SERVER 2        /* server */
#define MR_CLISRV 3        /* client and server */

/* id buffer size in bytes */
#define IDBUFSZ   256

struct callstate;          /* forward decl. for free list in struct is */
struct respstate;

#ifdef MPI_RUNNER
struct gatherstate;        /* state for gather_remote_url() */
#endif

/* useprobe: for tracking resource usage */
struct useprobe {
    int who;                /* flag to getrusage */
    struct timeval t0, t1;
    struct rusage r0, r1;
};

/*
 * g_s: shared global data (e.g. from the command line)
 */
struct g_s {
    int ninst;               /* number of instances */
    char *localspec;         /* local address spec */
    char *localtag;          /* if using "-d" - the local tag */
    char *remotespec;        /* remote address spec */
    int baseport;            /* base port number */
#if (HG_VERSION_MAJOR >= 1)  /* some overshoot, some ver 0's had it too */
    struct hg_init_info initinfo;   /* for HG_Init_opt() to set progress mode */
#endif
    int clistat;             /* client prints status every "clistat" RPCs */
    int count;               /* number of msgs to send/recv in a run */
    char *dir;               /* shared directory (mainly for mpi) */
    int mpimode;             /* running under mpirun */
    int mpixgather;          /* use MPI gather to xchg addresses */
#ifdef MPI_RUNNER
    struct gatherstate *gs;  /* state for gather_remote_url() */
#endif
    int mode;                /* operation mode (MR_CLIENT, etc.) */
    char modestr[4];         /* mode string */
    int limit;               /* limit # of concurrent RPCs at client */
    int quiet;               /* don't print so much */
    int rflag;               /* -r tag suffix spec'd */
    int rflagval;            /* value for -r */
    FILE *savefp;            /* save copy of outputhere */
    int timeout;             /* alarm timeout */
    char tagsuffix[64];      /* tag suffix: ninst-count-mode-limit-run# */

    /*
     * in/out req size includes byte used for seq and code.  if they
     * are zero then we just have the seq number (4 bytes).  otherwise
     * they must be >= 8 to account for the extended format of the
     * buffer.  this does not include bulk handle information.
     */
    int inreqsz;             /* input request size */
    int outreqsz;            /* output request size */

    /* bulk sizes, if zero, then we don't do any bulk operations */
    int64_t blrmasz;         /* server's local rma buffer size */
    int64_t bsendsz;         /* cli bulk send size (server RMA reads) */
    int64_t brecvsz;         /* cli bulk recv size (server RMA writes) */
    int oneflag;             /* one flag (server read/write same buffer) */

    int extend_rpcin;        /* set to 1 if using extended format */
    int extend_rpcout;       /* set to 1 if using extended format */

    /* cache max sizes: -1=disable cache, 0=unlimited, otherwise limit */
    int xcallcachemax;       /* call cache max size (in entries) */
    int yrespcachemax;       /* resp cache max size (in entries) */
} g;

/*
 * is_s: per-instance state structure.   we malloc an array of these at
 * startup.
 */
struct is_s {
    int n;                   /* our instance number (0 .. n-1) */
    hg_class_t *hgclass;     /* class for this instance */
    hg_context_t *hgctx;     /* context for this instance */
    hg_id_t myrpcid;         /* the ID of the instance's RPC */
    char myid[IDBUFSZ];      /* my local merc address */
    char remoteid[IDBUFSZ];  /* remote merc address */
    hg_addr_t remoteaddr;    /* encoded remote address */
    char myfun[64];          /* my function name */
#ifdef MERCURY_PROGRESSOR
    progressor_handle_t *ph; /* progress/trigger handler handle */
#else
    pthread_t nthread;       /* network thread */
    int stop_progthread;     /* set to stop the progress thread */
    int nprogress;           /* number of times through progress loop */
    int ntrigger;            /* number of times trigger called */
#ifdef RUSAGE_THREAD
    struct useprobe netuse;  /* network thread usage */
#endif
#endif /* MERCURY_PROGRESSOR */
    int recvd;               /* server: request callback received */
    int responded;           /* server: completed responses */
    struct respstate *rfree; /* server: free resp state structures */
    int nrfree;              /* length of rfree list */

    /* client side locking and sending flow control */
    pthread_mutex_t slock;   /* lock for this block of vars */
    pthread_cond_t scond;    /* client blocks here if waiting for network */
    int scond_mode;          /* mode for scond */
    int nstarted;            /* number of RPCs started */
    int nsent;               /* number of RPCs successfully sent */
#define SM_OFF      0        /* don't signal client */
#define SM_SENTONE  1        /* finished sending an RPC */
#define SM_SENTALL  2        /* finished sending all RPCs */
    struct callstate *cfree; /* free call state structures */
    int ncfree;              /* length of cfree list */
    int recvs_done;          /* server sets to let client know it is done */

    /* no mutex since only the main thread can write it */
    int sends_done;          /* set to non-zero when nsent is done */
};
struct is_s *is;    /* an array of state */

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */

char *argv0;                     /* argv[0], program name */
static void clean_dir_addrs();   /* for complain exit */

/*
 * fprint2: fprintf() wrapper
 */
int fprint2(FILE *stream, const char *format, ...) {
    int rv;
    va_list ap;
    va_start(ap, format);
    rv = vfprintf(stream, format, ap);
    va_end(ap);
    va_start(ap, format);
    if (g.savefp) vfprintf(g.savefp, format, ap);
    va_end(ap);
    return(rv);
}

/*
 * print2: printf() wrapper
 */
int print2(const char *format, ...) {
    int rv;
    va_list ap;
    va_start(ap, format);
    rv = vprintf(format, ap);
    va_end(ap);
    va_start(ap, format);
    if (g.savefp) vfprintf(g.savefp, format, ap);
    va_end(ap);
    return(rv);
}

/*
 * vcomplain/complain about something.  if ret is non-zero we exit(ret) after
 * complaining....
 */
void vcomplain(int ret, const char *format, va_list ap) {
    fprint2(stderr, "%s: ", argv0);
    vfprintf(stderr, format, ap);
    if (g.savefp) vfprintf(g.savefp, format, ap);
    fprint2(stderr, "\n");
    if (ret) {
        clean_dir_addrs();
#ifdef MPI_RUNNER
        if (g.mpimode > 0) MPI_Finalize();
#endif
        if (g.savefp) {
            fclose(g.savefp);
            g.savefp = NULL;
        }
        exit(ret);
    }
}

void complain(int ret, const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    vcomplain(ret, format, ap);
    va_end(ap);
}

/*
 * start-end usage state
 */
/* load starting values into useprobe */
static void useprobe_start(struct useprobe *up, int who) {
    up->who = who;
    if (gettimeofday(&up->t0, NULL) < 0 || getrusage(up->who, &up->r0) < 0)
        complain(1, "useprobe_start syscall failed?!");
}


/* load final values into useprobe */
static void useprobe_end(struct useprobe *up) {
    if (gettimeofday(&up->t1, NULL) < 0 || getrusage(up->who, &up->r1) < 0)
        complain(1, "useprobe_end syscall failed?!");
}

/* print useprobe info */
void useprobe_print(FILE *out, struct useprobe *up, const char *tag, int n) {
    char nstr[32];
    double start, end;
    double ustart, uend, sstart, send;
    long nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw, maxrss;

    if (n >= 0) {
        snprintf(nstr, sizeof(nstr), "%d: ", n);
    } else {
        nstr[0] = '\0';
    }

    start = up->t0.tv_sec + (up->t0.tv_usec / 1000000.0);
    end = up->t1.tv_sec + (up->t1.tv_usec / 1000000.0);

    ustart = up->r0.ru_utime.tv_sec + (up->r0.ru_utime.tv_usec / 1000000.0);
    uend = up->r1.ru_utime.tv_sec + (up->r1.ru_utime.tv_usec / 1000000.0);

    sstart = up->r0.ru_stime.tv_sec + (up->r0.ru_stime.tv_usec / 1000000.0);
    send = up->r1.ru_stime.tv_sec + (up->r1.ru_stime.tv_usec / 1000000.0);

    nminflt = up->r1.ru_minflt - up->r0.ru_minflt;
    nmajflt = up->r1.ru_majflt - up->r0.ru_majflt;
    ninblock = up->r1.ru_inblock - up->r0.ru_inblock;
    noublock = up->r1.ru_oublock - up->r0.ru_oublock;
    nnvcsw = up->r1.ru_nvcsw - up->r0.ru_nvcsw;
    nnivcsw = up->r1.ru_nivcsw - up->r0.ru_nivcsw;
    maxrss = up->r1.ru_maxrss - up->r0.ru_maxrss;

    fprintf(out, "%s%s: times: wall=%f, usr=%f, sys=%f (secs)\n", nstr, tag,
        end - start, uend - ustart, send - sstart);
    fprintf(out,
      "%s%s: minflt=%ld, majflt=%ld, inb=%ld, oub=%ld, vcw=%ld, ivcw=%ld\n",
      nstr, tag, nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw);
    fprintf(out, "%s%s: maxrss=%ld (KiB)\n", nstr, tag, maxrss);
}

#ifdef MERCURY_PROGRESSOR
/* print progressor stats */
void progstats_print(FILE *out, struct progressor_stats *ps, const char *tag,
                     int n) {
    char nstr[32];

    if (n >= 0) {
        snprintf(nstr, sizeof(nstr), "%d: ", n);
    } else {
        nstr[0] = '\0';
    }

    fprintf(out, "%s%s: times: wall=%f, usr=%f, sys=%f (secs)\n", nstr, tag,
        ps->runtime.tv_sec + (ps->runtime.tv_usec / 1000000.0),
        ps->runusage.ru_utime.tv_sec +
                              (ps->runusage.ru_utime.tv_usec / 1000000.0),
        ps->runusage.ru_stime.tv_sec +
                              (ps->runusage.ru_stime.tv_usec / 1000000.0));
    fprintf(out,
      "%s%s: minflt=%ld, majflt=%ld, inb=%ld, oub=%ld, vcw=%ld, ivcw=%ld\n",
      nstr, tag, ps->runusage.ru_minflt, ps->runusage.ru_majflt,
      ps->runusage.ru_inblock, ps->runusage.ru_oublock,
      ps->runusage.ru_nvcsw, ps->runusage.ru_nivcsw);
    fprintf(out, "%s%s: maxrss=%ld (KiB)\n", nstr, tag,
      ps->runusage.ru_maxrss);
}
#endif

/*
 * getsize: a souped up version of atoi() that handles suffixes like
 * 'k' (so getsize("1k") == 1024).
 */
int64_t getsize(char *from) {
    int len, end;
    int64_t rv;

    len = strlen(from);
    if (len == 0)
        return(0);
    rv = atoi(from);
    end = tolower(from[len-1]);
    switch (end) {    /* ordered to fallthrough */
        case 'g':
            rv = rv * 1024;
        case 'm':
            rv = rv * 1024;
        case 'k':
            rv = rv * 1024;
    }

    return(rv);
}

#ifdef MPI_RUNNER  /* tbar: only used by gather_remote_url() */
/*
 * tbar: thread barrier.  we've got N threads (where N > 0) in a process
 * that need to sync up.  we number the threads 0 to N-1 and choose thread
 * 0 ("t0") to do setup and manage communications with the other threads
 * (if N >= 2).
 *
 * example usage of API:
 *
 *   struct tbar tb;
 *
 *   rv = tbar_init(&tb, N);     // init tb before any thread accesses it
 *
 *   if (tbar_bar(&tb, tn) < 0)  // all threads block here, except t0
 *     exit(1);                  // fatal error
 *
 *   if (tn == 0) {              // t0 can do setup, then release the others
 *     do_some_setup();
 *     tbar_unbar(&tb, tn);      // release/unblock threads 1 to N-1
 *   }
 */

/*
 * tbar: thread barrier state structure
 */
struct tbar {
    pthread_mutex_t tlck;       /* lock for this data structure */
    int N;                      /* number of threads (incl t0) */
    pthread_cond_t t0cv;        /* control cv (thread t0 block here) */
    int t0_waiting;             /* is t0 waiting on t0cv? */
    int t0_draining;            /* is t0 in draining state? */
    pthread_cond_t tbarcv;      /* barrier cv (threads 1 to N-1 block here) */
    int nwaiters;               /* number of threads waiting on tbarcv */
    int toget;                  /* remaining # of threads to sync with */
};

/*
 * tbar_init: init a tbar into a known state.  must be called before
 * any thread attempts to tbar_bar() or tbar_unbar() on it.  returns
 * -1 on error, 0 otherwise.
 */
int tbar_init(struct tbar *tb, int nthreads) {

    if (pthread_mutex_init(&tb->tlck, NULL) != 0)
        return(-1);
    if (pthread_cond_init(&tb->t0cv, NULL) != 0) {
        pthread_mutex_destroy(&tb->tlck);
        return(-1);
    }
    if (pthread_cond_init(&tb->tbarcv, NULL) != 0) {
        pthread_cond_destroy(&tb->t0cv);
        pthread_mutex_destroy(&tb->tlck);
        return(-1);
    }

    tb->N = nthreads;
    tb->t0_waiting = 0;
    tb->t0_draining = 0;
    tb->nwaiters = 0;
    tb->toget = nthreads;
    return(0);
}

/*
 * tbar_bar: enter a thread barrier.  for t0, we always return without
 * blocking.   all other threads block here in tbar_bar() until t0
 * releases us by calling tbar_unbar().   if a thread that has been
 * released starts the next operation (by calling us), it blocks until
 * the previous operation clears.
 */
int tbar_bar(struct tbar *tb, int tn) {
    pthread_mutex_lock(&tb->tlck);

    if (tn >= tb->N) {
        fprintf(stderr, "tbar_bar: size error %d >= %d\n", tn, tb->N);
        abort();
    }

    /* not t0 and need to wait for prev. op to finish draining? */
    if (tn) {
        while (tb->t0_draining) {
            tb->nwaiters++;
            pthread_cond_wait(&tb->tbarcv, &tb->tlck);
            tb->nwaiters--;
        }
    }

    /* t0_draining must be zero now */
    if (tb->t0_draining) {
        fprintf(stderr, "tbar_bar: draining error\n");
        abort();
    }

    if (tb->toget > 0)     /* drop toget */
        tb->toget--;

    if (tn) {              /* t0 skips this, since it never blocks */

        /* last collected thread?  wake t0 if it is blocked */
        if (tb->toget == 0 && tb->t0_waiting) {
            pthread_cond_broadcast(&tb->t0cv);
        }

        /* wait for t0 to release us */
        while (tb->t0_draining == 0) {
            tb->nwaiters++;
            pthread_cond_wait(&tb->tbarcv, &tb->tlck);
            tb->nwaiters--;
        }

        /* now we are in draining state */
        if (tb->toget > 0)
            tb->toget--;
        if (tb->toget == 0 && tb->t0_waiting) {
            pthread_cond_broadcast(&tb->t0cv);
        }
    }

    pthread_mutex_unlock(&tb->tlck);
    return(0);
}

/*
 * tbar_unbar: called by t0 when it is ready to release the other threads
 * and drain the tbar.  returns 0 when tbar is completely drainted.
 * return -1 on error.
 */
int tbar_unbar(struct tbar *tb, int tn) {
    if (tn)
        return(-1);   /* ERROR, only t0 can call this function */

    pthread_mutex_lock(&tb->tlck);
    /* t0_draining must be 0 since we are t0 and we only set it here */

    /* wait for all procs to hit the bar */
    while (tb->toget > 0) {
        tb->t0_waiting = 1;
        pthread_cond_wait(&tb->t0cv, &tb->tlck);
        tb->t0_waiting = 0;
    }

    /* enter draining state */
    tb->t0_draining = 1;
    tb->toget = tb->N - 1;      /* no need to drain t0, subtract it out now */

    /* wake up procs waiting to be drained, if any */
    if (tb->nwaiters > 0) {
        pthread_cond_broadcast(&tb->tbarcv);
    }

    /* wait for drain to complete */
    while (tb->toget > 0) {
        tb->t0_waiting = 1;
        pthread_cond_wait(&tb->t0cv, &tb->tlck);
        tb->t0_waiting = 0;
    }

    /* exit draining state back to collection */
    tb->t0_draining = 0;
    tb->toget = tb->N;
    if (tb->nwaiters > 0) {   /* look for threads waiting to start next op */
        pthread_cond_broadcast(&tb->tbarcv);
    }

    pthread_mutex_unlock(&tb->tlck);
    return(0);
}

/* end of tbar */
#endif

/*
 * end of helper/utility functions.
 */

/*
 * lookup_state: for looking up an address
 */
struct lookup_state {
    pthread_mutex_t lock;    /* protect state */
    int n;                   /* instance number that owns lookup */
    int done;                /* set non-zero if done */
    pthread_cond_t lkupcond; /* caller waits on this */
};

#if 0
/*
 * input and output structures (this also generates XDR fns using
 * boost pp).  this is just an example of the format of a boost
 * pre-processor version without the extended format info (e.g. bulk
 * handles).  its been superseded by the non-boost manual version...
 */
MERCURY_GEN_PROC(rpcin_t, ((int32_t)(seq)))
MERCURY_GEN_PROC(rpcout_t, ((int32_t)(ret)))
#endif

/*
 * non-boost manual version
 */

/* helper macro to reduce the verbage ... */
#define procheck(R,MSG) if ((R) != HG_SUCCESS) { \
    hg_log_write(HG_LOG_TYPE_ERROR, "HG", __FILE__, __LINE__, __func__, MSG); \
    return(R); \
}

#define RPC_EXTENDED 0x80000000      /* set in seq/ret if extended format */
#define RPC_SEQMASK  ~(RPC_EXTENDED) /* to extract the seq */
#define RPC_EXTHDRSZ 8               /* sizeof(seq and ext_fmt or olen) */

/*
 * rpcin_t: arg for making the RPC call.   variable length, depending
 * on what options are set.  if seq is >= 0, then it is just seq.
 * otherwise use ext_fmt to see if it has bulk handles/lengths, and
 * maybe an extra non-bulk buffer.
 */
typedef struct {
    int32_t seq;            /* extended bit and sequence number */
    int32_t ext_fmt;        /* extended request format code */
#define EXT_BREAD     0x80000000   /* we have a bread bulk handle */
#define EXT_BWRITE    0x40000000   /* we have a bwrite bulk handle */
#define EXT_BLENMASK ~0xc0000000   /* mask for length of xbuf */
    hg_bulk_t bread;        /* bulk read handle */
    int64_t nread;          /* number of bytes for remote to read */
    hg_bulk_t bwrite;       /* bulk write handle */
    int64_t nwrite;         /* number of bytes for remote to write */
    char *xbuf;             /* extra buffer */
    hg_size_t sersize;      /* serialized size of rpcin */
} rpcin_t;

/*
 * encode/decode the rpcin_t structure
 */
static hg_return_t hg_proc_rpcin_t(hg_proc_t proc, void *data) {
    hg_return_t ret = HG_SUCCESS;
    hg_proc_op_t op = hg_proc_get_op(proc);
    rpcin_t *struct_data = (rpcin_t *) data;
    int32_t xlen;

    if (op == HG_DECODE) {
        memset(struct_data, 0, sizeof(*struct_data));
    }

    ret = hg_proc_hg_int32_t(proc, &struct_data->seq);
    procheck(ret, "Proc err seq");

    if ((struct_data->seq & RPC_EXTENDED) == 0) /* done if !extended format */
        goto done;

    ret = hg_proc_hg_int32_t(proc, &struct_data->ext_fmt);
    procheck(ret, "Proc err ext_fmt");

    /* get bulk handle info if present */
    if (struct_data->ext_fmt & EXT_BREAD) {
        ret = hg_proc_hg_bulk_t(proc, &struct_data->bread);
        procheck(ret, "Proc err bread");
        ret = hg_proc_hg_int64_t(proc, &struct_data->nread);
        procheck(ret, "Proc err nread");
    }
    if (struct_data->ext_fmt & EXT_BWRITE) {
        ret = hg_proc_hg_bulk_t(proc, &struct_data->bwrite);
        procheck(ret, "Proc err bwrite");
        ret = hg_proc_hg_int64_t(proc, &struct_data->nwrite);
        procheck(ret, "Proc err nwrite");
    }

    /* finally the xbuf */
    xlen = (struct_data->ext_fmt & EXT_BLENMASK);
    if (xlen) {
        switch (op) {
        case HG_DECODE:
            struct_data->xbuf = (char *)malloc(xlen);
            if (struct_data->xbuf == NULL) {
                hg_log_write(HG_LOG_TYPE_ERROR, "HG", __FILE__, __LINE__,
                             __func__, "Proc xbuf malloc");
                return(HG_NOMEM_ERROR);
            }
            /*FALLTHROUGH*/
        case HG_ENCODE:
            ret = hg_proc_memcpy(proc, struct_data->xbuf, xlen);
            procheck(ret, "Proc err xbuf");
            break;

        case HG_FREE:
            if (struct_data->xbuf) {
                free(struct_data->xbuf);
                struct_data->xbuf = NULL;
            }
            break;
        }
    }

done:
    if (op == HG_ENCODE || op == HG_DECODE) {
        struct_data->sersize = hg_proc_get_size_used(proc);
    }
    return(ret);
}


/*
 * rpcout_t: return value from the server.   the return value is
 * ~(input sequence number), except for the top bit which is used
 * to indicate an extended rpcout (e.g. it has an olen/obuf).
 */
typedef struct {
    int32_t ret;                   /* return value */
    int32_t olen;                  /* obuf's length */
    char *obuf;                    /* extra output buffer */
    hg_size_t sersize;             /* serialized size of rpcout */
} rpcout_t;

/*
 * encode/decode the rpcout_t structure
 */
static hg_return_t hg_proc_rpcout_t(hg_proc_t proc, void *data) {
    hg_return_t ret = HG_SUCCESS;
    hg_proc_op_t op = hg_proc_get_op(proc);
    rpcout_t *struct_data = (rpcout_t *) data;

    if (op == HG_DECODE)
        memset(struct_data, 0, sizeof(*struct_data));

    ret = hg_proc_hg_int32_t(proc, &struct_data->ret);
    procheck(ret, "Proc err ret");

    if ((struct_data->ret & RPC_EXTENDED) == 0)   /* done if !extended fmt */
        goto done;

    ret = hg_proc_hg_int32_t(proc, &struct_data->olen);
    switch (op) {
    case HG_DECODE:
        struct_data->obuf = (char *)malloc(struct_data->olen);
        if (struct_data->obuf == NULL) {
            hg_log_write(HG_LOG_TYPE_ERROR, "HG", __FILE__, __LINE__,
                         __func__, "Proc obuf malloc");
            return(HG_NOMEM_ERROR);
        }
        /*FALLTHROUGH*/
    case HG_ENCODE:
        ret = hg_proc_memcpy(proc, struct_data->obuf, struct_data->olen);
        procheck(ret, "proc err obuf");
        break;

    case HG_FREE:
        if (struct_data->obuf) {
            free(struct_data->obuf);
            struct_data->obuf = NULL;
        }
        break;
    }

 done:
    if (op == HG_ENCODE || op == HG_DECODE) {
        struct_data->sersize = hg_proc_get_size_used(proc);
    }
    return(ret);
}

/*
 * callstate: state of an RPC call.  pulls together all the call info
 * in one structure.
 */
struct callstate {
    struct is_s *isp;       /* instance that owns this call */
    hg_handle_t callhand;   /* main handle for the call */
    rpcin_t in;             /* call args */
    /* rd_rmabuf == wr_rmabuf if -O flag (one buffer flag) */
    void *rd_rmabuf;        /* buffer used for rma read */
    void *wr_rmabuf;        /* buffer used for rma write */
    struct callstate *next; /* linkage for free list */
};

/*
 * respstate: state of an RPC response.
 */
struct respstate {
    struct is_s *isp;       /* instance that owns this call */
    hg_handle_t callhand;   /* main handle for the call */
    rpcin_t in;             /* call in args */
    rpcout_t out;           /* resp args */
    void *lrmabuf;          /* local srvr rmabuf (malloc'd), sz=g.blrmasz */
    hg_bulk_t lrmabufhand;  /* bulk handle to local rmabuf */
    int phase;              /* current phase */
#define RS_READCLIENT  0    /* server is RMA reading from client */
#define RS_WRITECLIENT 1    /* server is RMA writing to client */
#define RS_RESPOND     2    /* server is finishing the RPC */
    struct respstate *next; /* linkage for free list */
};

/*
 * get_callstate: get a callstate for an is.  try the free list first,
 * then allocate a new one if the free list is empty... we grab slock
 * to access the list.
 */
struct callstate *get_callstate(struct is_s *isp) {
    struct callstate *rv;
    int64_t want;
    hg_size_t bs;

    rv = NULL;

    /* try the free list first */
    pthread_mutex_lock(&isp->slock);
    if (isp->cfree) {
        rv = isp->cfree;
        isp->cfree = rv->next;
        isp->ncfree--;
    }
    pthread_mutex_unlock(&isp->slock);

    if (rv)
        return(rv);    /* success via free list */

    /*
     * must malloc a new one.  this can be expensive, thus the free list...
     */
    rv = (struct callstate *) malloc(sizeof(*rv));
    if (!rv)
        complain(1, "get_callstate malloc failed");
    rv->isp = isp;

    /* the main handle ... */
    if (HG_Create(isp->hgctx, isp->remoteaddr,
                  isp->myrpcid, &rv->callhand) != HG_SUCCESS)
        complain(1, "get_callstate handle alloc failed");

    /*
     * start ext_fmt setup and allocate xbuf if needed...
     *
     * note: g.inreqsz includes the EXTHDRSZ (8) byte header, so we
     * don't need to allocate space here for that because that's
     * handled by the proc routine from in.seq and in.ext_fmt.
     * the length in in.ext_fmt is the length of xbuf, so it is
     * always EXTHDRSZ less than g.inreqsz.
     */
    if (g.inreqsz < RPC_EXTHDRSZ) {
        rv->in.ext_fmt = 0;
        rv->in.xbuf = NULL;
    } else {
        rv->in.ext_fmt = g.inreqsz - RPC_EXTHDRSZ;
        rv->in.xbuf = (char *)malloc(rv->in.ext_fmt);
        if (!rv->in.xbuf) complain(1, "getcallstate xbuf malloc failed");
    }

    /*
     * set bulk buffers and handles to zero if we are not using bulk.
     * otherwise allocate the bulk buffer(s) and a handle for it.  if
     * we are sending and recving and the oneflag is set, then we use
     * the same buffer (in read/write mode) for the rmas.  in that case
     * we size the buffer to be the larger of the two requested sizes.
     */
    if (g.bsendsz == 0) {
        rv->rd_rmabuf = NULL;
        rv->in.bread = HG_BULK_NULL;
        rv->in.nread = 0;
    } else {
        want = g.bsendsz;
        if (g.oneflag && g.brecvsz > want)   /* oneflag: rd/wr same buffer? */
            want = g.brecvsz;
        rv->rd_rmabuf = malloc(want);
        bs = want;
        if (HG_Bulk_create(isp->hgclass, 1, &rv->rd_rmabuf, &bs,
                           (g.oneflag) ? HG_BULK_READWRITE : HG_BULK_READ_ONLY,
                           &rv->in.bread) != HG_SUCCESS)
            complain(1, "get_callstate: bulk create 1 failed?");
        rv->in.nread = g.bsendsz;
        rv->in.ext_fmt |= EXT_BREAD;
    }

    if (g.brecvsz == 0) {
        rv->wr_rmabuf = NULL;
        rv->in.bwrite = HG_BULK_NULL;
        rv->in.nwrite = 0;
    } else if (g.oneflag) {
        rv->wr_rmabuf = rv->rd_rmabuf;   /* shared read/write buffer */
        rv->in.bwrite = rv->in.bread;    /* shared reference */
        rv->in.nwrite = g.brecvsz;
        rv->in.ext_fmt |= EXT_BWRITE;
    } else {
        rv->wr_rmabuf = malloc(g.brecvsz);
        bs = g.brecvsz;
        if (HG_Bulk_create(isp->hgclass, 1, &rv->wr_rmabuf, &bs,
                           HG_BULK_WRITE_ONLY, &rv->in.bwrite) != HG_SUCCESS)
            complain(1, "get_callstate: bulk create 2 failed?");
        rv->in.nwrite = g.brecvsz;
        rv->in.ext_fmt |= EXT_BWRITE;
    }

    rv->next = NULL;    /* just to be safe */
    return(rv);

}

/*
 * free_callstate: this frees all resources associated with the callstate.
 * the callstate should be allocated and not on the free list.
 */
void free_callstate(struct callstate *cs) {
    HG_Destroy(cs->callhand);

    if (cs->in.bread != HG_BULK_NULL)
        HG_Bulk_free(cs->in.bread);
    if (cs->in.bwrite != HG_BULK_NULL && !g.oneflag)
        HG_Bulk_free(cs->in.bwrite);
    if (cs->in.xbuf)
        free(cs->in.xbuf);

    if (cs->rd_rmabuf)
        free(cs->rd_rmabuf);
    if (cs->wr_rmabuf && !g.oneflag)
        free(cs->wr_rmabuf);

    free(cs);
}

/*
 * get_respstate: get a respstate for an is.  try the free list first,
 * then allocate a new one if the free list is empty...  no locking
 * required because all work is done in the network thread and there
 * is currently only one of those.
 */
struct respstate *get_respstate(struct is_s *isp) {
    struct respstate *rv;
    hg_size_t lrmasz;

    rv = NULL;

    /* try the free list first */
    if (isp->rfree) {
        rv = isp->rfree;
        isp->rfree = rv->next;
        isp->nrfree--;
    }

    if (rv)
        return(rv);    /* success via free list */

    /*
     * must malloc a new one.
     */
    rv = (struct respstate *) malloc(sizeof(*rv));
    if (!rv)
        complain(1, "get_respstate malloc failed");
    rv->isp = isp;

    /*
     * look for extended format for output and handle it.  g.outreqsz
     * includes the EXTHDRSZ (8) byte header, so we don't allocate
     * that here.
     */
    if (g.outreqsz < RPC_EXTHDRSZ) {
        rv->out.olen = 0;
        rv->out.obuf = NULL;
    } else {
        rv->out.olen = g.outreqsz - RPC_EXTHDRSZ;
        rv->out.obuf = (char *)malloc(rv->out.olen);
        if (!rv->out.obuf) complain(1, "getrespstate obuf malloc failed");
    }

    /*
     * allocate local rma buffer if needed
     */
    lrmasz = g.blrmasz;
    if (lrmasz == 0) {
        rv->lrmabuf = NULL;
        rv->lrmabufhand = HG_BULK_NULL;
    } else {
        rv->lrmabuf = malloc(lrmasz);
        if (rv->lrmabuf == NULL)
            complain(1, "malloc of lrmabuf failed");
        if (HG_Bulk_create(isp->hgclass, 1, &rv->lrmabuf, &lrmasz,
                           HG_BULK_READWRITE, &rv->lrmabufhand) != HG_SUCCESS)
            complain(1, "get_respstate bulk create failed?");
    }

    rv->next = NULL;    /* just to be safe */
    return(rv);

}

/*
 * free_respstate: this frees all resources associated with the respstate.
 * the respstate should be allocated and not on the free list.
 */
void free_respstate(struct respstate *rs) {
    if (rs->out.obuf)
        free(rs->out.obuf);

    if (rs->lrmabuf)
        free(rs->lrmabuf);
    if (rs->lrmabufhand)
        HG_Bulk_free(rs->lrmabufhand);

    free(rs);
}

/*
 * alarm signal handler
 */
void sigalarm(int foo) {
    int lcv;
    uint64_t nprog, ntrig;
    fprint2(stderr, "SIGALRM detected\n");
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
#ifdef MERCURY_PROGRESSOR
    if (is[lcv].ph) {
        nprog = mercury_progressor_nprogress(is[lcv].ph);
        ntrig = mercury_progressor_ntrigger(is[lcv].ph);
    } else {
        nprog = ntrig = 0;
    }
#else
    nprog = is[lcv].nprogress;
    ntrig = is[lcv].ntrigger;
#endif
        fprint2(stderr, "%d: @alarm: ", lcv);
        if (is[lcv].hgctx == NULL) {
            fprint2(stderr, "no context\n");
            continue;
        }
        fprint2(stderr,
                "srvr=%d(%d), clnt=%d(%d), sdone=%d, prog=%d, trig=%d\n",
                is[lcv].recvd, is[lcv].recvd - is[lcv].responded,
                is[lcv].nstarted, is[lcv].nstarted - is[lcv].nsent,
                is[lcv].sends_done, nprog, ntrig);
    }
    clean_dir_addrs();
    fprint2(stderr, "Alarm clock\n");
#ifdef MPI_RUNNER
    if (g.mpimode > 0) MPI_Finalize();
#endif
    if (g.savefp) {
        fclose(g.savefp);
        g.savefp = NULL;
    }
    exit(1);
}

/*
 * forward prototype decls.
 */
static void *run_instance(void *arg);   /* run one instance */
#if !defined(MERCURY_PROGRESSOR)
static void *run_network(void *arg);    /* per-instance network thread */
#endif
static hg_return_t lookup_cb(const struct hg_cb_info *cbi);  /* client cb */
static hg_return_t forw_cb(const struct hg_cb_info *cbi);  /* client cb */
static hg_return_t rpchandler(hg_handle_t handle); /* server cb */
static hg_return_t advance_resp_phase(struct respstate *rs);
static hg_return_t reply_bulk_cb(const struct hg_cb_info *cbi);  /* server cb */
static hg_return_t reply_sent_cb(const struct hg_cb_info *cbi);  /* server cb */

/*
 * usage
 */
static void usage(const char *msg) {
    if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
    fprintf(stderr, "usage: %s [options] ninstances localspec [remotespec]\n",
            argv0);
    fprintf(stderr, "\n\nlocal and remote spec are mercury urls.\n");
    fprintf(stderr, "use printf '%%d' for the port number.\n");
    fprintf(stderr, "remotespec is optional if mode is set to 's' (server)\n");
    fprintf(stderr, "\noptions:\n");
    fprintf(stderr, "\t-c count    number of RPCs to perform\n");
    fprintf(stderr, "\t-D count    client prints status every 'count' RPCs\n");
    fprintf(stderr, "\t-d dir      shared dir for passing server addresses\n");
#ifdef MPI_RUNNER
    fprintf(stderr, "\t-g          MPI gather for remote addr\n");
#else
    fprintf(stderr, "\t-g          (MPI mode not compiled in)\n");
#endif
    fprintf(stderr, "\t-l limit    limit # of client concurrent RPCs\n");
#ifdef MPI_RUNNER
    fprintf(stderr, "\t-M          run mercury-runner under MPI (MPI mode)\n");
#else
    fprintf(stderr, "\t-M          (MPI mode not compiled in)\n");
#endif
    fprintf(stderr, "\t-m mode     mode c, s, or cs (client/server)\n");
#if (HG_VERSION_MAJOR >= 1)  /* some overshoot, some ver 0's had it too */
    fprintf(stderr, "\t-P          poll mode (NA_NO_BLOCK) -- forces spin\n");
#else
    fprintf(stderr, "\t-P          poll mode (not supported)\n");
#endif
    fprintf(stderr, "\t-p port     base port number\n");
    fprintf(stderr, "\t-q          quiet mode\n");
    fprintf(stderr, "\t-r n        enable tag suffix with this run number\n");
    fprintf(stderr, "\t-s file     save copy of our output in this file\n");
    fprintf(stderr, "\t-t sec      timeout (alarm), in seconds\n");
    fprintf(stderr, "\nuse '-l 1' to serialize RPCs\n\n");
    fprintf(stderr, "size related options:\n");
    fprintf(stderr, "\t-i size     input req size (>= 8 if specified)\n");
    fprintf(stderr, "\t-o size     output req size (>= 8 if specified)\n");
    fprintf(stderr, "\t-L size     server's local rma buffer size\n");
    fprintf(stderr, "\t-S size     client bulk send sz (srvr RMA reads)\n");
    fprintf(stderr, "\t-R size     client bulk recv sz (srvr RMA writes)\n");
    fprintf(stderr, "\t-O          one buffer flag (valid if -S and -R set)\n");
    fprintf(stderr, "\t-X count    client call handle cache max size\n");
    fprintf(stderr, "\t-Y count    server reply handle cache max size\n");
    fprintf(stderr, "\ndefault payload size is 4.\n");
    fprintf(stderr, "to enable RMA:\n");
    fprintf(stderr, "  must specify -L (on srvr) and -S and/or -R (on cli)\n");
    fprintf(stderr, "using -O causes the server to RMA read & write to the\n");
    fprintf(stderr, "same buffer (client exports it in RDWR mode)\n");
    fprintf(stderr, "default value for -L is 0 (disables RMA on server)\n");
    fprintf(stderr, "for -X/-Y: count=-1 disable cache, count=0 unlimited\n");
    fprintf(stderr, "when using -d, localspec should be tag=<mercury-url>\n");
    fprintf(stderr, "(remotespec should just be the remote tag to read\n");
    fprintf(stderr, "from the address passing directory)\n");
#ifdef MPI_RUNNER
    if (g.mpimode > 0) MPI_Finalize();
#endif
    if (g.savefp) {
        fclose(g.savefp);
        g.savefp = NULL;
    }
    exit(1);
}

#ifdef MPI_RUNNER
/*
 * gather state: state for gather_remote_url() operation
 */
struct gatherstate {
    struct tbar tb;         /* thread barrier */
    /* let's just reuse tb.tlck to lock these too */
    int maxsize;            /* max URL size in bytes */
    char *urlbuf;           /* malloc'd buffer for MPI_Allgather() */
};
#endif

/*
 * main program.  usage:
 *
 * ./merc-test n-instances local-addr-spec remote-addr-spec
 *
 * the address specs use a %d for port (e.g. 'bmp+tcp://%d')
 */
int main(int argc, char **argv) {
    struct timeval tv;
    int ch, n, lcv, rv;
    char *c;
    pthread_t *tarr;
    struct useprobe mainuse;
    char mytag[128];
    char *savefile;
#ifdef MPI_RUNNER
    struct gatherstate gstatestore;
#endif
    argv0 = argv[0];

    /* we want lines, even if we are writing to a pipe */
    setlinebuf(stdout);

    /* init random for random data */
    (void)gettimeofday(&tv, NULL);
    srandom(getpid() + tv.tv_sec);

    /* setup default to zero/null, except as noted below */
    memset(&g, 0, sizeof(g));
    savefile = NULL;
    g.count = DEF_COUNT;
    g.mode = MR_CLISRV;
    g.baseport = DEF_BASEPORT;
    g.timeout = DEF_TIMEOUT;

    while ((ch = getopt(argc, argv,
                        "c:D:d:gi:l:Mm:o:Pp:qr:t:L:OR:S:s:X:Y:")) != -1) {
        switch (ch) {
            case 'c':
                g.count = atoi(optarg);
                if (g.count < 1) usage("bad count");
                break;
            case 'D':
                g.clistat = atoi(optarg);
                if (g.clistat < 0) usage("bad client status count");
                break;
            case 'd':
                g.dir = optarg;
                break;
            case 'g':
#ifdef MPI_RUNNER
                g.mpixgather = 1;
#else
                complain(1, "not compiled with MPI, can't -g");
#endif
                break;
            case 'i':
                g.inreqsz = getsize(optarg);
                if (g.inreqsz < 8) usage("bad inreqsz");
                break;
            case 'l':
                g.limit = atoi(optarg);
                if (g.limit < 1) usage("bad limit");
                break;
            case 'M':
#ifdef MPI_RUNNER
                g.mpimode = -1;
#else
                complain(1, "not compiled with MPI, can't -M");
#endif
                break;
            case 'm':
                if (strcmp(optarg, "c") == 0)
                    g.mode = MR_CLIENT;
                else if (strcmp(optarg, "s") == 0)
                    g.mode = MR_SERVER;
                else if (strcmp(optarg, "cs") == 0)
                    g.mode = MR_CLISRV;
                else
                    usage("bad mode");
                break;
            case 'o':
                g.outreqsz = getsize(optarg);
                if (g.outreqsz < 8) usage("bad outreqsz");
                break;
            case 'P':
#if (HG_VERSION_MAJOR >= 1)
                g.initinfo.na_init_info.progress_mode = NA_NO_BLOCK;
#else
                usage("poll mode not supported in this version of mercury");
#endif
                break;
            case 'p':
                g.baseport = atoi(optarg);
                if (g.baseport < 1) usage("bad port");
                break;
            case 'q':
                g.quiet = 1;
                break;
            case 'r':
                g.rflag++;  /* will gen tag suffix after args parsed */
                g.rflagval = atoi(optarg);
                break;
            case 't':
                g.timeout = atoi(optarg);
                if (g.timeout < 0) usage("bad timeout");
                break;
            case 'L':
                g.blrmasz = getsize(optarg);
                if (g.blrmasz < 1) usage("bad srvr local rma size");
                break;
            case 'O':
                g.oneflag = 1;
                break;
            case 'R':
                g.brecvsz = getsize(optarg);
                if (g.brecvsz < 1) usage("bad bulk recv size");
                break;
            case 'S':
                g.bsendsz = getsize(optarg);
                if (g.bsendsz < 1) usage("bad bulk send size");
                break;
            case 's':
                savefile = optarg;
                break;
            case 'X':
                g.xcallcachemax = atoi(optarg);
                if (g.xcallcachemax < -1) usage("bad xcallcache max");
                break;
            case 'Y':
                g.yrespcachemax = atoi(optarg);
                if (g.yrespcachemax < -1) usage("bad yrespcache max");
                break;
            default:
                usage(NULL);
        }
    }
    argc -= optind;
    argv += optind;

    /* remotespec is optional if in server mode (unless mpimode) */
    if ((g.mode == MR_SERVER && (argc < 2 || argc > 3)) ||
        (g.mode != MR_SERVER && argc != 3))
        usage("bad args");

    g.ninst = n = atoi(argv[0]);
    g.localspec = argv[1];
    g.remotespec = (argc == 3) ? argv[2] : NULL;

#ifdef MPI_RUNNER
    if (g.mpixgather) {
        if (g.mpimode == 0)
            complain(1, "MPI gather mode (-g) requires MPI mode (-M)");
        if (g.dir) {
            complain(0, "note: '-d dir' when -g is used with MPI");
            g.dir = NULL;    /* switch it off and keep going */
        }

        /* setup gatherstate structure before creating any threads */
        if (tbar_init(&gstatestore.tb, g.ninst) < 0)
            complain(1, "unable to init gstatestore.tb?");
        gstatestore.maxsize = 0;
        gstatestore.urlbuf = NULL;
        g.gs = &gstatestore;
    }

    if (g.mpimode) {
        int myrank, mysize, newmode;
        if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
            g.mpimode = 0;
            complain(1, "MPI_Init failed!");
        }
        g.mpimode = 1;
        if (g.mode == MR_SERVER && argc == 2)
            complain(1, "MPI mode requires remotespec for servers");
        if (MPI_Comm_rank(MPI_COMM_WORLD, &myrank) != MPI_SUCCESS)
            complain(1, "MPI_Comm_rank failed!");
        if (MPI_Comm_size(MPI_COMM_WORLD, &mysize) != MPI_SUCCESS)
            complain(1, "MPI_Comm_size failed!");
        if (mysize != 2)
            complain(1, "Bad MPI world size, must have exactly 2 procs");

        /* ok, now edit the command line for rank 1 */
        if (myrank == 1) {
            char *tmp;

            g.mpimode = 2;    /* indicates we are run 1 */
            /* swap local and remote specs */
            tmp = g.localspec;
            g.localspec = g.remotespec;
            g.remotespec = tmp;

            if (g.mode == MR_SERVER) {
                newmode = MR_CLIENT;
            } else if (g.mode == MR_CLIENT) {
                newmode = MR_SERVER;
            } else {
                newmode = g.mode;  /* no change for client server */
            }
            g.mode = newmode;
        }
    }
#endif

    if (g.oneflag && (g.brecvsz == 0 || g.bsendsz == 0))
        usage("-O only applies if -R and -S are set");

    if (!g.limit)
        g.limit = g.count;    /* max value */
    if (g.dir) {
        if (chdir(g.dir) < 0)
            complain(1, "can't cd to %s: %s", g.dir, strerror(errno));

        if ((c = strchr(g.localspec, '=')) != NULL) {
            g.localtag = g.localspec;
            *c = '\0';
            g.localspec = c + 1;
            /* if both local+remote have "=spec" just discard the remote one */
            if (g.remotespec && (c = strchr(g.remotespec, '=')) != NULL) {
                *c = '\0';
            }
        } else if ((c = strchr(g.remotespec, '=')) != NULL) {
            /*
             * undocumented feature for MPI mode: allow the '=' on the
             * remotespec instead of the localspec...
             */
            g.localtag = g.localspec;
            *c = '\0';
            g.localspec = c + 1;
        } else {
            complain(1, "missing '=' in address specs: l=%s r=%s",
            g.localspec, g.remotespec);
        }
    }
    snprintf(g.modestr, sizeof(g.modestr), "%s%s",
            (g.mode & MR_CLIENT) ? "c" : "", (g.mode & MR_SERVER) ? "s" : "");
    if (g.rflag) {
        snprintf(g.tagsuffix, sizeof(g.tagsuffix), "-%d-%d-%s-%d-%d",
                 n, g.count, g.modestr, g.limit, g.rflagval);
    }
    if (g.inreqsz || g.bsendsz || g.brecvsz)
        g.extend_rpcin = 1;
    if (g.outreqsz)
        g.extend_rpcout = 1;

    if (savefile != NULL) {
        char *tmpsave = savefile;
        if (g.mpimode) {
            int newlen = strlen(savefile)+3;  /* ".?" plus null @ end */
            tmpsave = (char *)malloc(newlen);
            if (!tmpsave)
                complain(1, "malloc savefile failed?!");
            snprintf(tmpsave, newlen, "%s.%d", savefile,
                     (g.mpimode == 1) ? 0 : 1);
        }
        g.savefp = fopen(tmpsave, "w");
        if (!g.savefp) {
            fprint2(stderr, "savefile fopen(%s) failed?!!\n", tmpsave);
        } else {
            setlinebuf(g.savefp);
        }
        if (tmpsave != savefile)
            free(tmpsave);
    }

    print2("\n%s options:\n", argv0);
    print2("\tninstances = %d\n", n);
    print2("\tmpimode    = %s\n", (g.mpimode) ? "ON" : "OFF");
    if (g.mpimode)
        print2("\tmpigather  = %s\n", (g.mpixgather) ? "ON" : "OFF");
    print2("\tlocalspec  = %s\n", g.localspec);
    if (g.localtag)
        print2("\tlocaltag   = %s\n", g.localtag);
    print2("\tremotespec = %s\n", (g.remotespec) ? g.remotespec : "<none>");
    if (g.dir)
        print2("\tdirectory  = %s\n", g.dir);
#if (HG_VERSION_MAJOR >= 1)
    print2("\tpoll       = %s\n",
        (g.initinfo.na_init_info.progress_mode) ? "ON" : "OFF");
#endif
    print2("\tbaseport   = %d\n", g.baseport);
    print2("\tcount      = %d\n", g.count);
    print2("\tmode       = %s\n", g.modestr);
    if (g.extend_rpcin || g.extend_rpcout)
        print2("\textend     =%s%s\n",
               (g.extend_rpcin) ? " in" : "", (g.extend_rpcout) ? " out" : "");

    if (g.limit == g.count)
        print2("\tlimit      = <none>\n");
    else
        print2("\tlimit      = %d\n", g.limit);
    print2("\tquiet      = %d\n", g.quiet);
    if (g.rflag)
        print2("\tsuffix     = %s\n", g.tagsuffix);
    if (savefile)
        print2("\tsavefile   = %s\n", savefile);
    print2("\ttimeout    = %d\n", g.timeout);
    print2("sizes:\n");
    print2("\tinput      = %d\n", (g.inreqsz == 0) ? 4 : g.inreqsz);
    print2("\toutput     = %d\n", (g.outreqsz == 0) ? 4 : g.outreqsz);
    if (g.blrmasz)
        print2("\tlrmasize   = %" PRId64 "\n", g.blrmasz);
    if (g.bsendsz)
        print2("\tbulksend   = %" PRId64 "\n", g.bsendsz);
    if (g.brecvsz)
        print2("\tbulkrecv   = %" PRId64 "\n", g.brecvsz);
    if (g.bsendsz && g.brecvsz)
        print2("\tonebuffer  = %d\n", g.oneflag);
    if (g.xcallcachemax)
        print2("\tcallcache  = %d max\n", g.xcallcachemax);
    if (g.yrespcachemax)
        print2("\trespcache  = %d max\n", g.yrespcachemax);
    print2("\n");

    signal(SIGALRM, sigalarm);
    alarm(g.timeout);
    print2("main: starting %d instance%s ...\n", n, (n == 1) ? "" : "s");
    tarr = (pthread_t *)malloc(n * sizeof(pthread_t));
    if (!tarr) complain(1, "malloc tarr thread array failed");
    is = (struct is_s *)malloc(n *sizeof(*is));    /* array */
    if (!is) complain(1, "malloc 'is' instance state failed");
    memset(is, 0, n * sizeof(*is));

    /* fork off a thread for each instance */
    useprobe_start(&mainuse, RUSAGE_SELF);
    for (lcv = 1 ; lcv < n ; lcv++) {
        is[lcv].n = lcv;
        rv = pthread_create(&tarr[lcv], NULL, run_instance, (void*)&is[lcv]);
        if (rv != 0)
            complain(1, "pthread create failed %d", rv);
    }

    /* just use main thread to run instance 0 */
    run_instance(&is[0]);

    /* now wait for everything to finish */
    print2("main: collecting\n");
    for (lcv = 1 ; lcv < n ; lcv++) {
        pthread_join(tarr[lcv], NULL);
    }
    useprobe_end(&mainuse);
    print2("main: collection done.\n");
    snprintf(mytag, sizeof(mytag), "ALL%s", g.tagsuffix);
    useprobe_print(stdout, &mainuse, mytag, -1);
    if (g.savefp) useprobe_print(g.savefp, &mainuse, mytag, -1);
    print2("main exiting...\n");

    clean_dir_addrs();
#ifdef MPI_RUNNER
    if (g.mpimode > 0) MPI_Finalize();
#endif
    if (g.savefp) {
        fclose(g.savefp);
        g.savefp = NULL;
    }
    exit(0);
}

/*
 * gather_remote_url: use MPI to gather the remote URL so we can pass
 * it down to HG_Addr_lookup().  only used in MPI mode when the "-g" flag
 * is set.
 * note: MPI_Comm_size is 2 (we already checked).
 *
 * on success we return a malloc'd string with the remote address in it.
 * the caller must free the buffer when done with it.
 */
char *gather_remote_url(int n) {
#ifndef MPI_RUNNER
    return(NULL);    /* nothing-doing if we don't have MPI */
#else
    /*
     * we exchange local info using tbar, and then we recreate a
     * cut down version of mssg here to use MPI to exchange info
     * between processes...
     */
    int myrank, tmpmax;
    hg_addr_t myaddr;
    hg_size_t asz;
    char *myname, *rv;

    /* recover rank from mpi mode value */
    myrank = g.mpimode - 1;

    /* get our address string size */
    if (HG_Addr_self(is[n].hgclass, &myaddr) != HG_SUCCESS)
        complain(1, "HG_Addr_self failed?!");
    asz = 0;
    if (HG_Addr_to_string(is[n].hgclass, NULL, &asz, myaddr) != HG_SUCCESS)
        complain(1, "addr to string failed to give needed size");

    pthread_mutex_lock(&g.gs->tb.tlck);
    if (asz > g.gs->maxsize)
        g.gs->maxsize = asz;
    pthread_mutex_unlock(&g.gs->tb.tlck);

    /* sync up threads to collect max value across them */
    if (tbar_bar(&g.gs->tb, n) < 0)
        complain(1, "gather: tbar_bar 1 failed");

    /* now t0 uses MPI to chose max value between processes */
    if (n == 0) {
        if (MPI_Allreduce(&g.gs->maxsize, &tmpmax, 1, MPI_INT,
                          MPI_MAX, MPI_COMM_WORLD) != MPI_SUCCESS)
            complain(1, "MPI_Allreduce failed?");
        g.gs->maxsize = tmpmax;  /* no need to lock, as we are in tbar */
        /* we need 1 buffer per thread for 2 MPI procs */
        g.gs->urlbuf = (char *)malloc(g.gs->maxsize * g.ninst * 2);
        if (!g.gs->urlbuf)
            complain(1, "g.gs->urlbuf malloc failed!");

        /* now that the urlbuf is in place, release the threads */
        tbar_unbar(&g.gs->tb, n);
    }

    /*
     * figure out where to put this thread's info.  we put all of MPI
     * rank 0's threads first, then MPI rank 1's.  within a rank, the
     * strings are in instance order.
     */
    myname = g.gs->urlbuf + (g.gs->maxsize * g.ninst * myrank) +
                            (g.gs->maxsize * n);
    asz = g.gs->maxsize;
    if (HG_Addr_to_string(is[n].hgclass, myname, &asz, myaddr) != HG_SUCCESS)
        complain(1, "addr to string failed");
    /* don't need myaddr anymore */
    if (HG_Addr_free(is[n].hgclass, myaddr) != HG_SUCCESS)
        complain(0, "warning: HG_Addr_free failed");

    /* sync up threads again so that the two t0's can exchange all addr strs */
    if (tbar_bar(&g.gs->tb, n) < 0)
        complain(1, "gather: tbar_bar 2 failed");

    /* now t0 can do the MPI operation to exchange address URLs now */
    if (n == 0) {
        if (MPI_Allgather(MPI_IN_PLACE, g.gs->maxsize * g.ninst, MPI_BYTE,
                          g.gs->urlbuf, g.gs->maxsize * g.ninst, MPI_BYTE,
                          MPI_COMM_WORLD) != MPI_SUCCESS)
            complain(1, "allgather failed!?");

        /* now that the urlbuf is gathered, release the threads */
        tbar_unbar(&g.gs->tb, n);
    }

    rv = g.gs->urlbuf;
    if (myrank == 0)
        rv = rv + (g.gs->maxsize * g.ninst);  /* rank 0 wants rank 1's addrs */
    rv = rv + (g.gs->maxsize * n);            /* select correct instance */
    rv = strdup(rv);                          /* and make a copy */
    if (!rv) complain(1, "strdup failed?");

    /* sync up threads one last time so we can free g.gs->urlbuf */
    if (tbar_bar(&g.gs->tb, n) < 0)
        complain(1, "gather: tbar_bar 3 failed");

    if (n == 0) {
        free(g.gs->urlbuf);
        g.gs->urlbuf = NULL;

        /* now that the urlbuf is freed, release the threads */
        tbar_unbar(&g.gs->tb, n);
    }

    return(rv);
#endif
}

/*
 * save_dir_addr: write my server address to a file (only used if g.dir).
 * will exit on failure...
 */
void save_dir_addr(int n) {
    const char *clname;
    hg_size_t clnamelen, asz, put;
    hg_addr_t myaddr;
    char *tmpbuf, file[128], *colon, *plus;
    int has_classname;
    FILE *fp;

    /*
     * XXX: mercury behavior has changed.  in some older versions
     * HG_Addr_to_string() doesn't include the class name.  in newer
     * versions it does.  we handle both cases.
     */
    clname = HG_Class_get_name(is[n].hgclass);
    if (!clname) complain(1, "can't get class name");
    clnamelen = strlen(clname);

    /* get localaddr size, malloc buf, then put string in malloc'd buf */
    if (HG_Addr_self(is[n].hgclass, &myaddr) != HG_SUCCESS)
        complain(1, "HG_Addr_self failed?!");
    if (HG_Addr_to_string(is[n].hgclass, NULL, &asz, myaddr) != HG_SUCCESS)
        complain(1, "addr to string failed to give needed size");
    if ((tmpbuf = (char *)malloc(asz+4)) == NULL)
        complain(1, "malloc %d failed", asz+4);
    if (HG_Addr_to_string(is[n].hgclass, tmpbuf,
                                          &asz, myaddr) != HG_SUCCESS)
        complain(1, "addr to string failed");

    /* determine if tmpbuf already has class name in it or not */
    colon = strchr(tmpbuf, ':');
    plus = strchr(tmpbuf, '+');
    has_classname = (plus && (!colon || plus < colon));

    /* write the data to the file */
    snprintf(file, sizeof(file), "s.%s.%d", g.localtag, n);
    fp = fopen(file, "w");
    if (!fp) complain(1, "fopen failed: %s", strerror(errno));

    if (has_classname) {
        put = fprintf(fp, "%s", tmpbuf);  /* note: asz includes null */
        if (put + 1 != asz)
            complain(1, "fprintf failed: %d != %d", put + 1, asz);
    } else {
        put = fprintf(fp, "%s+%s", clname, tmpbuf);
        if (put + 1 != clnamelen + 1 + asz)
            complain(1, "fprintf failed: %d != %d",
                     put + 1, clnamelen + 1 + asz);
    }

    if (fclose(fp) != 0)
        complain(1, "fclose failed");

    /* done, free and return */
    free(tmpbuf);
    if (HG_Addr_free(is[n].hgclass, myaddr) != HG_SUCCESS)
        complain(0, "warning: HG_Addr_free failed");
}

/*
 * load_dir_addr: load remote address from a directory, return malloc'd buf
 */
char *load_dir_addr(int n) {
    char file[IDBUFSZ*2], *retbuf;
    struct stat st;
    FILE *fp;
    snprintf(file, sizeof(file), "s.%s.%d", is[n].remoteid, n);
    if (stat(file, &st) < 0)
        complain(1, "can't stat %s: %s", file, strerror(errno));
    retbuf = (char *)malloc(st.st_size+1);
    if (retbuf == NULL)
        complain(1, "load_dir_addr: malloc %d failed", st.st_size+1);
    retbuf[st.st_size] = '\0';   /* null at end */
    fp = fopen(file, "r");
    if (fp == NULL)
        complain(1, "load_dir_addr: fopen %s: %s", file, strerror(errno));
    if (fread(retbuf, 1, st.st_size, fp) != st.st_size)
        complain(1, "load_dir_addr: fread failed");
    fclose(fp);
    print2("%d: resolved remote tag %s to %s\n", n, is[n].remoteid, retbuf);
    return(retbuf);
}

/*
 * clean_dir_addrs: remove the addr files (e.g when exiting)
 */
static void clean_dir_addrs() {
    int lcv;
    char file[IDBUFSZ*2];
    if (!is || g.dir == NULL)
        return;
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
        snprintf(file, sizeof(file), "s.%s.%d", is[lcv].remoteid, lcv);
        unlink(file);   /* ignore errors */
    }
}

/*
 * run_instance: the main routine for running one instance of mercury.
 * we pass the instance state struct in as the arg...
 */
void *run_instance(void *arg) {
    struct is_s *isp = (struct is_s *)arg;
    int n = isp->n;               /* recover n from isp */
    int lcv, rv;
    char *remoteurl = NULL;
    hg_return_t ret;
    struct lookup_state lst;
    hg_op_id_t lookupop;
    struct useprobe rp;
    struct callstate *cs;
    unsigned char data;
    struct respstate *rs;
#ifdef MERCURY_PROGRESSOR
    struct progressor_stats ps;
#endif

    print2("%d: instance running\n", n);
    is[n].n = n;    /* make it easy to map 'is' structure back to n */

    /*
     * build mercury url specs.  if just running in client mode, move
     * local port up more so we can run on just one system (e.g. localhost).
     * note: remote may be NULL if server mode.
     */
    if (g.mode == MR_CLIENT) {
        snprintf(is[n].myid, sizeof(is[n].myid), g.localspec,
                 g.ninst + n + g.baseport);
    } else {
        snprintf(is[n].myid, sizeof(is[n].myid), g.localspec, n + g.baseport);
    }
    if (g.remotespec)
        snprintf(is[n].remoteid, sizeof(is[n].remoteid), g.remotespec,
                 n + g.baseport);

    print2("%d: init local endpoint: %s\n", n, is[n].myid);
#if (HG_VERSION_MAJOR >= 1)
    is[n].hgclass = HG_Init_opt(is[n].myid,
                                ((g.mode == MR_CLIENT) ? HG_FALSE : HG_TRUE),
                                &g.initinfo);
#else
    is[n].hgclass = HG_Init(is[n].myid,
                            (g.mode == MR_CLIENT) ? HG_FALSE : HG_TRUE);
#endif
    if (is[n].hgclass == NULL)  complain(1, "HG_init failed");
    is[n].hgctx = HG_Context_create(is[n].hgclass);
    if (is[n].hgctx == NULL)  complain(1, "HG_Context_create failed");

    /* make a funcion name and register it */
    snprintf(is[n].myfun, sizeof(is[n].myfun), "f%d", n);
    is[n].myrpcid = HG_Register_name(is[n].hgclass, is[n].myfun,
                                     hg_proc_rpcin_t, hg_proc_rpcout_t,
                                     rpchandler);
    /* we use registered data to pass instance to server callback */
    if (HG_Register_data(is[n].hgclass, is[n].myrpcid,
                         &is[n], NULL) != HG_SUCCESS)
        complain(1, "unable to register n as data");

    /* init locking and fork off network progress/trigger thread */
    if (pthread_mutex_init(&is[n].slock, NULL) != 0)
        complain(1, "slock mutex init");
    if (pthread_cond_init(&is[n].scond, NULL) != 0) complain(1, "scond init");
    is[n].sends_done = 0;
    is[n].recvd = is[n].responded = 0;
    /* client-only mode: nothing to recv, so mark recvs_done now */
    is[n].recvs_done = (g.mode == MR_CLIENT) ? 1 : 0;
#ifdef MERCURY_PROGRESSOR
    is[n].ph = mercury_progressor_init(is[n].hgclass, is[n].hgctx);
    if (is[n].ph == NULL) complain(1, "mercury_progressor_init failed");
    if (mercury_progressor_needed(is[n].ph) != HG_SUCCESS)
        complain(1, "mercury_progressor_needed failed");
    print2("%d: network thread running\n", n);
#else
    is[n].stop_progthread = 0;
    is[n].nprogress = is[n].ntrigger = 0;
    rv = pthread_create(&is[n].nthread, NULL, run_network, (void*)&n);
    if (rv != 0) complain(1, "pthread create srvr failed %d", rv);
#endif /* MERCURY_PROGRESSOR */

    /* MPI gather the remoteurl, if requested */
    if (g.mpixgather) {
        remoteurl = gather_remote_url(n);
        if (!remoteurl)
            complain(1, "MPI gather of remote URL failed!");
        print2("%d: collected remote URL via MPI gather\n", n);
    }

    /* servers handle the g.dir option by writing our addr to a file */
    if (g.dir != NULL && (g.mode & MR_SERVER) != 0) {
        save_dir_addr(n);
    }

    if (g.mode != MR_SERVER) {    /* plain server-only can start right away */
        /* poor man's barrier */
        print2("%d: init done.  sleeping 10\n", n);
        sleep(10);
    }

    /*
     * resolve the remote address for client ... only need to do this
     * once, since it is fixed for this program...
     */
    if (g.mode & MR_CLIENT) {
        if (remoteurl == NULL) {  /* MPI gather may have set it already */
            remoteurl = (g.dir) ? load_dir_addr(n) : is[n].remoteid;
        }
        print2("%d: remote address lookup %s\n", n, remoteurl);
        if (pthread_mutex_init(&lst.lock, NULL) != 0)
            complain(1, "lst.lock mutex init");
        pthread_mutex_lock(&lst.lock);
        lst.n = n;
        lst.done = 0;
        if (pthread_cond_init(&lst.lkupcond, NULL) != 0)
            complain(1, "lst.lkupcond cond init");

        ret = HG_Addr_lookup(is[n].hgctx, lookup_cb, &lst,
                             remoteurl, &lookupop);
        if (ret != HG_SUCCESS) complain(1, "HG addr lookup launch failed");
        while (lst.done == 0) {
            if (pthread_cond_wait(&lst.lkupcond, &lst.lock) != 0)
                complain(1, "lst.lkupcond cond wait");
        }
        if (lst.done < 0) complain(1, "lookup failed");
        pthread_cond_destroy(&lst.lkupcond);
        pthread_mutex_unlock(&lst.lock);
        pthread_mutex_destroy(&lst.lock);
        if (remoteurl != is[n].remoteid) free(remoteurl);
        remoteurl = NULL;
        print2("%d: done remote address lookup\n", n);

        /* poor man's barrier again... */
        print2("%d: address lookup done.  sleeping 10 again\n", n);
        sleep(10);
    }

#ifdef RUSAGE_THREAD
    useprobe_start(&rp, RUSAGE_THREAD);
#else
    useprobe_start(&rp, RUSAGE_SELF);
#endif

    if (g.mode == MR_SERVER) {
        print2("%d: server mode, skipping send step\n", n);
        goto skipsend;
    }

    print2("%d: sending...\n", n);
    is[n].nsent = 0;
    is[n].scond_mode = SM_OFF;
    /* starting lcv at 1, indicates number we are sending */
    for (lcv = 1 ; lcv <= g.count ; lcv++) {

        cs = get_callstate(&is[n]);  /* from free list or freshly malloc'd */

        cs->in.seq = (g.extend_rpcin) ? (lcv | RPC_EXTENDED) : lcv;
        if (g.extend_rpcin && cs->rd_rmabuf) {
            data = random();
            *((char *)cs->rd_rmabuf) = data;  /* data for sanity check */
            if (!g.quiet)
                print2("%d: prelaunch %d: set data to %d\n", n, lcv, data);
        }


        if (!g.quiet)
            print2("%d: launching %d\n", n, lcv);
        ret = HG_Forward(cs->callhand, forw_cb, cs, &cs->in);
        is[n].nstarted++;
        if (ret != HG_SUCCESS) complain(1, "hg forward failed");
        if (!g.quiet)
            print2("%d: launched %d (size=%d)\n", n, lcv, (int)cs->in.sersize);
        if (g.clistat && (is[n].nstarted % g.clistat) == 0) {
            useprobe_end(&rp);   /* not really the end... */
            print2("%d: RPC %d @ %d secs\n", n, is[n].nstarted,
                   rp.t1.tv_sec - rp.t0.tv_sec);
        }

        /* flow control */
        pthread_mutex_lock(&is[n].slock);
        /* while in-flight >= limit */
        while ((lcv - is[n].nsent) >= g.limit) {
            is[n].scond_mode = SM_SENTONE;      /* as soon as room is there */
            if (pthread_cond_wait(&is[n].scond, &is[n].slock) != 0)
                complain(1, "client send flow control cond wait");
        }
        pthread_mutex_unlock(&is[n].slock);
    }

    /* wait until all sends are complete (already done if serialsend) */
    pthread_mutex_lock(&is[n].slock);
    while (is[n].nsent < g.count) {
        is[n].scond_mode = SM_SENTALL;
        if (pthread_cond_wait(&is[n].scond, &is[n].slock) != 0)
            complain(1, "snd cond wait");
    }
    pthread_mutex_unlock(&is[n].slock);
    is[n].sends_done = 1;
    print2("%d: all sends complete\n", n);

skipsend:
    /* done sending, wait for network thread to finish and exit */
    pthread_mutex_lock(&is[n].slock);
    while (is[n].recvs_done == 0) {
        if (pthread_cond_wait(&is[n].scond, &is[n].slock) != 0)
            complain(1, "recvs_done cond wait");
    }
    pthread_mutex_unlock(&is[n].slock);

#ifdef MERCURY_PROGRESSOR
    if (mercury_progressor_getstats(is[n].ph, &ps) != HG_SUCCESS)
        complain(1, "mercury_progressor_getstats failed");
    if (mercury_progressor_idle(is[n].ph) != HG_SUCCESS)
        complain(1, "mercury_progressor_idle failed");
    print2("%d: network thread complete (nprogress=%d, ntrigger=%d)\n", n,
           mercury_progressor_nprogress(is[n].ph),
           mercury_progressor_ntrigger(is[n].ph));
    progstats_print(stdout, &ps, "net", n);
    if (g.savefp) progstats_print(g.savefp,  &ps, "net", n);
    if (mercury_progressor_freehandle(is[n].ph) != HG_SUCCESS)
        complain(1, "mercury_progressor_freehandle failed");
#else
    is[n].stop_progthread = 1;
    pthread_join(is[n].nthread, NULL);
#ifdef RUSAGE_THREAD
    useprobe_print(stdout, &is[n].netuse, "net", n);
    if (g.savefp) useprobe_print(g.savefp, &is[n].netuse, "net", n);
#endif
#endif
    if (is[n].remoteaddr) {
        HG_Addr_free(is[n].hgclass, is[n].remoteaddr);
        is[n].remoteaddr = NULL;
    }
    /* done with locking, we are the only thread left in this instance */
    pthread_cond_destroy(&is[n].scond);
    pthread_mutex_destroy(&is[n].slock);
    useprobe_end(&rp);
    print2("%d: all recvs complete\n", n);

    /* dump the callstate cache */
    while ((cs = is[n].cfree) != NULL) {
        is[n].cfree = cs->next;
        free_callstate(cs);
    }
    is[n].ncfree = 0;     /* just to be clear */

    /* dump the respstate cache */
    while ((rs = is[n].rfree) != NULL) {
        is[n].rfree = rs->next;
        free_respstate(rs);
    }
    is[n].nrfree = 0;     /* just to be clear */

    print2("%d: destroy context and finalize mercury\n", n);
    HG_Context_destroy(is[n].hgctx);
    HG_Finalize(is[n].hgclass);

    if (g.mode & MR_CLIENT) {
        double rtime = (rp.t1.tv_sec + (rp.t1.tv_usec / 1000000.0)) -
                       (rp.t0.tv_sec + (rp.t0.tv_usec / 1000000.0));
        print2("%d: client%s: %d rpc%s in %f sec (%f sec per op)\n",
               n, g.tagsuffix, g.count, (g.count == 1) ? "" : "s",
               rtime, rtime / (double) g.count);
    }

#ifdef RUSAGE_THREAD
    useprobe_print(stdout, &rp, "instance", n);
    if (g.savefp) useprobe_print(g.savefp, &rp, "instance", -1);
#endif
    print2("%d: instance done\n", n);
    return(NULL);
}

/*
 * lookup_cb: this gets called when HG_Addr_lookup() completes.
 * we need to stash the results and wake the caller.
 */
static hg_return_t lookup_cb(const struct hg_cb_info *cbi) {
    struct lookup_state *lstp = (struct lookup_state *)cbi->arg;
    int n;

#if 0
    /*
     * XXX: bug, hg_core_trigger_lookup_entry() sets it to HG_CB_BULK
     * instead of HG_CB_LOOKUP.  bug.  jerome fixed feb 2, 2017
     */
    if (cbi->type != HG_CB_LOOKUP)
        errx(1, "lookup_cb mismatch %d", cbi->type);
#endif

    pthread_mutex_lock(&lstp->lock);
    if (cbi->ret != HG_SUCCESS) {
        complain(0, "lookup_cb failed %d", cbi->ret);
        lstp->done = -1;
    } else {
        n = lstp->n;
        is[n].remoteaddr = cbi->info.lookup.addr;
        lstp->done = 1;
    }
    pthread_mutex_unlock(&lstp->lock);
    pthread_cond_signal(&lstp->lkupcond);

    return(HG_SUCCESS);
}

/*
 * forw_cb: this gets called on the client side when HG_Forward() completes
 * (i.e. when we get the reply from the remote side).
 */
static hg_return_t forw_cb(const struct hg_cb_info *cbi) {
    struct callstate *cs = (struct callstate *)cbi->arg;
    hg_handle_t hand;
    struct is_s *isp;
    hg_return_t ret;
    rpcout_t out;
    int oldmode;
    unsigned char data;

    if (cbi->ret != HG_SUCCESS) complain(1, "forw_cb failed");
    if (cbi->type != HG_CB_FORWARD) complain(1, "forw_cb wrong type");
    hand = cbi->info.forward.handle;
    if (hand != cs->callhand) complain(1, "forw_cb mismatch hands");
    isp = cs->isp;

    ret = HG_Get_output(hand, &out);
    if (ret != HG_SUCCESS) complain(1, "get output failed");

    if (!g.quiet) {
        if (cs->wr_rmabuf) {
            data = *((char *)cs->wr_rmabuf);
            print2("%d: forw complete (code=%d,reply_size=%d, data=%d)\n",
                   isp->n, ~out.ret & RPC_SEQMASK, (int)out.sersize,
                   data);
        } else {
            print2("%d: forw complete (code=%d,reply_size=%d)\n",
                   isp->n, ~out.ret & RPC_SEQMASK, (int)out.sersize);
        }
    }

    HG_Free_output(hand, &out);

    /* update records and see if we need to signal client */
    pthread_mutex_lock(&isp->slock);
    isp->nsent++;
    if (isp->scond_mode != SM_OFF) {
        oldmode = isp->scond_mode;
        if (oldmode == SM_SENTONE || isp->nsent >= g.count) {
            isp->scond_mode = SM_OFF;
            pthread_cond_signal(&isp->scond);
        }
    }

    /* put cs in cache if the cache is enabled and not already full */
    if (g.xcallcachemax >= 0 &&
        (g.xcallcachemax == 0 || isp->ncfree < g.xcallcachemax)) {

        cs->next = isp->cfree; /* cache for reuse */
        isp->cfree = cs;
        isp->ncfree++;
        cs = NULL;             /* cache now owns cs */
    }
    pthread_mutex_unlock(&isp->slock);

    /* free cs if it is still set (i.e. wasn't put in the cache) */
    if (cs) {
        free_callstate(cs);
        cs = NULL;
    }

    return(HG_SUCCESS);
}

#if !defined(MERCURY_PROGRESSOR)
/*
 * run_network: network support pthread.   need to call progress to push the
 * network and then trigger to run the callback.  we do this all in
 * one thread (meaning that we shouldn't block in the trigger function,
 * or we won't make progress).  since we only have one thread running
 * trigger callback, we do not need to worry about concurrent access to
 * "got" ...
 */
static void *run_network(void *arg) {
    int n = *((int *)arg);
    unsigned int actual;
    hg_return_t ret;
    actual = 0;

    print2("%d: network thread running\n", n);
#ifdef RUSAGE_THREAD
    useprobe_start(&is[n].netuse, RUSAGE_THREAD);
#endif

    /* run until we are asked to stop */
    while (is[n].stop_progthread == 0) {

        do {
            ret = HG_Trigger(is[n].hgctx, 0, 1, &actual);
            is[n].ntrigger++;
        } while (ret == HG_SUCCESS && actual);

        /* recheck, trigger may set stop_progthread */
        if (is[n].stop_progthread == 0) {
            HG_Progress(is[n].hgctx, 100);
            is[n].nprogress++;
        }

    }

#ifdef RUSAGE_THREAD
    useprobe_end(&is[n].netuse);
#endif
    print2("%d: network thread complete (nprogress=%d, ntrigger=%d)\n", n,
           is[n].nprogress, is[n].ntrigger);
    return(NULL);
}
#endif /* !defined(MERCURY_PROGRESSOR) */

/*
 * server side funcions....
 */

/*
 * rpchandler: called on the server when a new RPC comes in
 */
static hg_return_t rpchandler(hg_handle_t handle) {
    struct is_s *isp;
    const struct hg_info *hgi;
    struct respstate *rs;
    hg_return_t ret;
    int32_t inseq;

    /* gotta extract "isp" using handle, 'cause that's the only way pass it */
    hgi = HG_Get_info(handle);
    if (!hgi) complain(1, "rpchandler: bad hgi");
    isp = (struct is_s *) HG_Registered_data(hgi->hg_class, hgi->id);
    if (!isp) complain(1, "rpchandler: bad isp");

    /* currently safe: only one network thread and we are in it */
    isp->recvd++;

    rs = get_respstate(isp);
    rs->callhand = handle;
    ret = HG_Get_input(handle, &rs->in);
    if (ret != HG_SUCCESS) complain(1, "rpchandler: HG_Get_input failed");

    inseq = rs->in.seq & RPC_SEQMASK;
    if (!g.quiet)
        print2("%d: got remote input %d (size=%d)\n", isp->n, inseq,
               (int)rs->in.sersize);

    rs->out.ret = ~inseq & RPC_SEQMASK;
    if (g.extend_rpcout)
        rs->out.ret |= RPC_EXTENDED;

    rs->phase = RS_READCLIENT;

    ret = advance_resp_phase(rs);

    return(ret);

}

/*
 * advance_resp_phase: push the rs forward
 */
static hg_return_t advance_resp_phase(struct respstate *rs) {
    const struct hg_info *hgi;
    hg_size_t tomove;
    hg_return_t rv;
    hg_op_id_t dummy;
    int32_t inseq;
    unsigned char data;

    hgi = HG_Get_info(rs->callhand);  /* to get remote's host address */
    if (!hgi)
        complain(1, "advance_resp_phase: HG_Get_info failed?");

 again:

    switch (rs->phase) {

    case RS_READCLIENT:
        rs->phase++;
        if (rs->in.nread == 0 || rs->in.bread == HG_BULK_NULL)
            goto again;    /* nothing to read from client, move on */
        if (g.blrmasz == 0) {
            complain(0, "advance_resp_phase: no lbuf to rma read in (skip)");
            goto again;
        }
        tomove = rs->in.nread;
        if (g.blrmasz < tomove) {
            complain(0, "advance_resp_phase: lbuf too small, trunc by %d",
                     (int)tomove - g.blrmasz);
            tomove = g.blrmasz;

        }
        if (!g.quiet)
            print2("%d: %d: starting RMA read %" PRId64 " bytes\n",
                   rs->isp->n, rs->in.seq & RPC_SEQMASK, tomove);

        rv = HG_Bulk_transfer(rs->isp->hgctx, reply_bulk_cb, (void *)rs,
                              HG_BULK_PULL, hgi->addr, rs->in.bread,
                              0, rs->lrmabufhand, 0, tomove, &dummy);

        if (rv != HG_SUCCESS)
            complain(1, "HG_Bulk_tranfer failed? (%d)", rv);
        break;

    case RS_WRITECLIENT:
        rs->phase++;
        if (rs->in.nwrite == 0 || rs->in.bwrite == HG_BULK_NULL)
            goto again;   /* nothing to write to client, move on */
        if (g.blrmasz == 0) {
            complain(0, "advance_resp_phase: no lbuf to rma write in (skip)");
            goto again;
        }
        tomove = rs->in.nwrite;
        if (g.blrmasz < tomove) {
            complain(0, "advance_resp_phase: lbuf too small, trunc by %d",
                     (int)tomove - g.blrmasz);
            tomove = g.blrmasz;

        }
        data = random();
        *((char *)rs->lrmabuf) = data;  /* data for sanity check */
        if (!g.quiet)
            print2("%d: %d: starting RMA write %" PRId64 " bytes, data=%d\n",
                   rs->isp->n, rs->in.seq & RPC_SEQMASK, tomove, data);

        rv = HG_Bulk_transfer(rs->isp->hgctx, reply_bulk_cb, (void *)rs,
                              HG_BULK_PUSH, hgi->addr, rs->in.bwrite,
                              0, rs->lrmabufhand, 0, tomove, &dummy);

        if (rv != HG_SUCCESS)
            complain(1, "HG_Bulk_tranfer failed? (%d)", rv);

        break;

    default:   /* must be RS_RESPOND */
        inseq = rs->in.seq & RPC_SEQMASK;
        rv = HG_Free_input(rs->callhand, &rs->in);

        /* the callback will bump "got" after respond has been sent */
        rv = HG_Respond(rs->callhand, reply_sent_cb, rs, &rs->out);
        if (rv != HG_SUCCESS) complain(1, "rpchandler: HG_Respond failed");
        if (!g.quiet)
            print2("%d: responded to %d (size=%d)\n", rs->isp->n, inseq,
                   (int)rs->out.sersize);

    }

    return(HG_SUCCESS);
}

/*
 * reply_bulk_cb: called after the server completes a bulk op
 */
static hg_return_t reply_bulk_cb(const struct hg_cb_info *cbi) {
    struct respstate *rs;
    struct is_s *isp;
    int oldphase;
    unsigned char data;

    if (cbi->type != HG_CB_BULK)
        complain(1, "reply_bulk_cb:unexpected sent cb");

    rs = (struct respstate *)cbi->arg;
    isp = rs->isp;
    oldphase = rs->phase - 1;

    if (oldphase == RS_READCLIENT) {
        data = *((char *)rs->lrmabuf);
        if (!g.quiet)
            print2("%d: %d: server bulk read from client complete (data=%d)\n",
                   rs->isp->n, rs->in.seq & RPC_SEQMASK, data);
    } else {
        if (!g.quiet)
            print2("%d: %d server bulk write to client complete\n",
                   rs->isp->n, rs->in.seq & RPC_SEQMASK);
    }

    return(advance_resp_phase(rs));
}


/*
 * reply_sent_cb: called after the server's reply to an RPC completes.
 */
static hg_return_t reply_sent_cb(const struct hg_cb_info *cbi) {
    struct respstate *rs;
    struct is_s *isp;

    if (cbi->type != HG_CB_RESPOND)
        complain(1, "reply_sent_cb:unexpected sent cb");

    rs = (struct respstate *)cbi->arg;
    isp = rs->isp;

    /*
     * currently safe: there is only one network thread and we
     * are in it (via trigger fn).
     */
    isp->responded++;

    if (cbi->info.respond.handle != rs->callhand)
        complain(1, "reply_send_cb sanity check failed");

    /* return handle to the pool for reuse */
    HG_Destroy(rs->callhand);
    rs->callhand = NULL;

    /* either put rs in cache for reuse or free it */
    if (g.yrespcachemax < 0 ||
        (g.yrespcachemax != 0 && isp->ncfree >= g.yrespcachemax)) {

        free_respstate(rs);    /* get rid of it */

    } else {

        rs->next = isp->rfree; /* cache for reuse */
        isp->rfree = rs;
        isp->nrfree++;

    }

    /* did we finish? */
    if (isp->responded >= g.count) {
        pthread_mutex_lock(&isp->slock);
        isp->recvs_done = 1;
        pthread_cond_signal(&isp->scond);  /* in case client is waiting */
        pthread_mutex_unlock(&isp->slock);
    }

    return(HG_SUCCESS);
}
