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
 *  -l limit     limit # of concurrent client RPC requests ("-l 1" = serial)
 *  -m mode      c, s, cs (client, server, or both)
 *  -p baseport  base port number
 *  -q           quiet mode - don't print during RPCs
 *  -r n         enable tag suffix with this run number
 *  -t secs      timeout (alarm)
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
 */

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <mercury.h>
#include <mercury_macros.h>

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */

char *argv0;    /* argv[0], program name */

/*
 * vcomplain/complain about something.  if ret is non-zero we exit(ret) after
 * complaining....
 */
void vcomplain(int ret, const char *format, va_list ap) {
    fprintf(stderr, "%s: ", argv0);
    vfprintf(stderr, format, ap);
    fprintf(stderr, "\n");
    if (ret)
        exit(ret);
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
struct useprobe {
    int who;                /* flag to getrusage */
    struct timeval t0, t1;
    struct rusage r0, r1;
};

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
    long nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw;

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

    fprintf(out, "%s%s: times: wall=%f, usr=%f, sys=%f (secs)\n", nstr, tag,
        end - start, uend - ustart, send - sstart);
    fprintf(out,
      "%s%s: minflt=%ld, majflt=%ld, inb=%ld, oub=%ld, vcw=%ld, ivcw=%ld\n",
      nstr, tag, nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw);
}

/*
 * end of helper/utility functions.
 */

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

/*
 * g: shared global data (e.g. from the command line)
 */
struct g {
    int ninst;               /* number of instances */
    char *localspec;         /* local address spec */
    char *remotespec;        /* remote address spec */
    int baseport;            /* base port number */
    int count;               /* number of msgs to send/recv in a run */
    int mode;                /* operation mode (MR_CLIENT, etc.) */
    char modestr[4];         /* mode string */
    int limit;               /* limit # of concurrent RPCs at client */
    int quiet;               /* don't print so much */
    int rflag;               /* -r spec'd */
    int rflagval;            /* value for -r */
    int timeout;             /* alarm timeout */
    char tagsuffix[64];      /* tag suffix: ninst-count-mode-limit-run# */
} g;

/*
 * is: per-instance state structure.   we malloc an array of these at
 * startup.
 */
struct is {
    int n;                   /* our instance number (0 .. n-1) */
    hg_class_t *hgclass;     /* class for this instance */
    hg_context_t *hgctx;     /* context for this instance */
    hg_id_t myrpcid;         /* the ID of the instance's RPC */
    pthread_t nthread;       /* network thread */
    char myid[256];          /* my local merc address */
    char remoteid[256];      /* remote merc address */
    hg_addr_t remoteaddr;    /* encoded remote address */
    char myfun[64];          /* my function name */
    int nprogress;           /* number of times through progress loop */
    int ntrigger;            /* number of times trigger called */
    int got;                 /* number of RPCs server has got */

    /* client side sending flow control */
    pthread_mutex_t slock;   /* lock for this block of vars */
    pthread_cond_t scond;    /* client blocks here if waiting for network */
    int scond_mode;          /* mode for scond */
    int nsent;               /* number of RPCs successfully sent */
#define SM_OFF      0        /* don't signal client */
#define SM_SENTONE  1        /* finished sending an RPC */
#define SM_SENTALL  2        /* finished sending all RPCs */

    /* no mutex since only the main thread can write it */
    int sends_done;          /* set to non-zero when nsent is done */
};
struct is *is;    /* an array of state */

/*
 * lookup_state: for looking up an address
 */
struct lookup_state {
    pthread_mutex_t lock;    /* protect state */
    int n;                   /* instance number that owns lookup */
    int done;                /* set non-zero if done */
    pthread_cond_t lkupcond; /* caller waits on this */
};

/*
 * input and output structures (this also generates XDR fns using boost pp)
 */
MERCURY_GEN_PROC(rpcin_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(rpcout_t, ((int32_t)(ret)))

/*
 * forward prototypes here, so we can structure the source code to
 * be more linear to make it easier to read.
 */
static void *run_instance(void *arg);   /* run one instance */
static void *run_network(void *arg);    /* per-instance network thread */
static hg_return_t lookup_cb(const struct hg_cb_info *cbi);  /* client cb */
static hg_return_t forw_cb(const struct hg_cb_info *cbi);  /* client cb */
static hg_return_t rpchandler(hg_handle_t handle); /* server cb */
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
    fprintf(stderr, "\t-l limit    limit # of client concurrent RPCs\n");
    fprintf(stderr, "\t-m mode     mode c, s, or cs (client/server)\n");
    fprintf(stderr, "\t-p port     base port number\n");
    fprintf(stderr, "\t-q          quiet mode\n");
    fprintf(stderr, "\t-r n        enable tag suffix with this run number\n");
    fprintf(stderr, "\t-t sec      timeout (alarm), in seconds\n");
    fprintf(stderr, "\nuse '-l 1' to serialize RPCs\n");
    exit(1);
}


/*
 * main program.  usage:
 *
 * ./merc-test n-instances local-addr-spec remote-addr-spec
 *
 * the address specs use a %d for port (e.g. 'bmp+tcp://%d')
 */
int main(int argc, char **argv) {
    int ch, n, lcv, rv;
    pthread_t *tarr;
    char *c;
    struct useprobe mainuse;
    char mytag[128];
    argv0 = argv[0];

    /* we want lines, even if we are writing to a pipe */
    setlinebuf(stdout);

    g.count = DEF_COUNT;
    g.mode = MR_CLISRV;
    g.baseport = DEF_BASEPORT;
    g.quiet = 0;
    g.limit = 0;
    g.rflag = 0;
    g.timeout = DEF_TIMEOUT;
    g.tagsuffix[0] = '\0';
    while ((ch = getopt(argc, argv, "c:l:m:p:qr:t:")) != -1) {
        switch (ch) {
            case 'c':
                g.count = atoi(optarg);
                if (g.count < 1) usage("bad count");
                break;
            case 'l':
                g.limit = atoi(optarg);
                if (g.limit < 1) usage("bad limit");
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
            default:
                usage(NULL);
        }
    }
    argc -= optind;
    argv += optind;

    /* remotespec is optional if in server mode */
    if ((g.mode == MR_SERVER && (argc < 2 || argc > 3)) ||
        (g.mode != MR_SERVER && argc != 3))
        usage("bad args");

    g.ninst = n = atoi(argv[0]);
    g.localspec = argv[1];
    g.remotespec = (argc == 3) ? argv[2] : NULL;
    if (!g.limit)
        g.limit = g.count;    /* max value */
    snprintf(g.modestr, sizeof(g.modestr), "%s%s",
            (g.mode & MR_CLIENT) ? "c" : "", (g.mode & MR_SERVER) ? "s" : "");
    if (g.rflag) {
        snprintf(g.tagsuffix, sizeof(g.tagsuffix), "-%d-%d-%s-%d-%d",
                 n, g.count, g.modestr, g.limit, g.rflagval);
    }

    printf("\n%s options:\n", argv0);
    printf("\tninstances = %d\n", n);
    printf("\tlocalspec  = %s\n", g.localspec);
    printf("\tremotespec = %s\n", (g.remotespec) ? g.remotespec : "<none>");
    printf("\tbaseport   = %d\n", g.baseport);
    printf("\tcount      = %d\n", g.count);
    printf("\tmode       = %s\n", g.modestr);
    if (g.limit == g.count)
        printf("\tlimit      = <none>\n");
    else
        printf("\tlimit      = %d\n", g.limit);
    printf("\tquiet      = %d\n", g.quiet);
    if (g.rflag)
        printf("\tsuffix     = %s\n", g.tagsuffix);
    printf("\ttimeout    = %d\n", g.timeout);
    printf("\n");

    alarm(g.timeout);
    printf("main: starting %d instance%s ...\n", n, (n == 1) ? "" : "s");
    tarr = (pthread_t *)malloc(n * sizeof(pthread_t));
    if (!tarr) complain(1, "malloc tarr thread array failed");
    is = (struct is *)malloc(n *sizeof(*is));    /* array */
    if (!is) complain(1, "malloc 'is' instance state failed");
    memset(is, 0, n * sizeof(*is));

    /* fork off a thread for each instance */
    useprobe_start(&mainuse, RUSAGE_SELF);
    for (lcv = 0 ; lcv < n ; lcv++) {
        is[lcv].n = lcv;
        rv = pthread_create(&tarr[lcv], NULL, run_instance, (void*)&is[lcv]);
        if (rv != 0)
            complain(1, "pthread create failed %d", rv);
    }

    /* now wait for everything to finish */
    printf("main: collecting\n");
    for (lcv = 0 ; lcv < n ; lcv++) {
        pthread_join(tarr[lcv], NULL);
    }
    useprobe_end(&mainuse);
    printf("main: collection done.\n");
    snprintf(mytag, sizeof(mytag), "ALL%s", g.tagsuffix);
    useprobe_print(stdout, &mainuse, mytag, -1);
    printf("main exiting...\n");

    exit(0);
}

/*
 * run_instance: the main routine for running one instance of mercury.
 * we pass the instance state struct in as the arg...
 */
void *run_instance(void *arg) {
    struct is *isp = (struct is *)arg;
    int n = isp->n;               /* recover n from isp */
    int lcv, rv;
    hg_return_t ret;
    struct lookup_state lst;
    hg_op_id_t lookupop;
    struct useprobe rp;

    printf("%d: instance running\n", n);
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

    printf("%d: init local endpoint: %s\n", n, is[n].myid);
    is[n].hgclass = HG_Init(is[n].myid,
                            (g.mode == MR_CLIENT) ? HG_FALSE : HG_TRUE);
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

    /* fork off network progress/trigger thread */
    is[n].sends_done = 0;   /* run_network reads this */
    rv = pthread_create(&is[n].nthread, NULL, run_network, (void*)&n);
    if (rv != 0) complain(1, "pthread create srvr failed %d", rv);

    if (g.mode != MR_SERVER) {    /* plain server can start right away */
        /* poor man's barrier */
        printf("%d: init done.  sleeping 10\n", n);
        sleep(10);
    }

    /*
     * resolve the remote address for client ... only need to do this
     * once, since it is fixed for this program...
     */
    if (g.mode & MR_CLIENT) {
        printf("%d: remote address lookup %s\n", n, is[n].remoteid);
        if (pthread_mutex_init(&lst.lock, NULL) != 0)
            complain(1, "lst.lock mutex init");
        pthread_mutex_lock(&lst.lock);
        lst.n = n;
        lst.done = 0;
        if (pthread_cond_init(&lst.lkupcond, NULL) != 0)
            complain(1, "lst.lkupcond cond init");

        ret = HG_Addr_lookup(is[n].hgctx, lookup_cb, &lst,
                             is[n].remoteid, &lookupop);
        if (ret != HG_SUCCESS) complain(1, "HG addr lookup launch failed");
        while (lst.done == 0) {
            if (pthread_cond_wait(&lst.lkupcond, &lst.lock) != 0)
                complain(1, "lst.lkupcond cond wait");
        }
        if (lst.done < 0) complain(1, "lookup failed");
        pthread_cond_destroy(&lst.lkupcond);
        pthread_mutex_unlock(&lst.lock);
        pthread_mutex_destroy(&lst.lock);
        printf("%d: done remote address lookup\n", n);

        /* poor man's barrier again... */
        printf("%d: address lookup done.  sleeping 10 again\n", n);
        sleep(10);
    }

#ifdef RUSAGE_THREAD
    useprobe_start(&rp, RUSAGE_THREAD);
#else
    useprobe_start(&rp, RUSAGE_SELF);
#endif

    if (g.mode == MR_SERVER) {
        printf("%d: server mode, skipping send step\n", n);
        goto skipsend;
    }

    printf("%d: sending...\n", n);
    if (pthread_mutex_init(&is[n].slock, NULL) != 0)
        complain(1, "slock mutex init");
    is[n].nsent = 0;
    is[n].scond_mode = SM_OFF;
    if (pthread_cond_init(&is[n].scond, NULL) != 0) complain(1, "scond init");
    /* starting lcv at 1, indicates number we are sending */
    for (lcv = 1 ; lcv <= g.count ; lcv++) {
        hg_handle_t rpchand;
        rpcin_t in;
        ret = HG_Create(is[n].hgctx, is[n].remoteaddr,
                        is[n].myrpcid, &rpchand);
        if (ret != HG_SUCCESS) complain(1, "hg create failed");

        in.ret = lcv;

        if (!g.quiet)
            printf("%d: launching %d\n", n, in.ret);
        ret = HG_Forward(rpchand, forw_cb, &is[n], &in);
        if (ret != HG_SUCCESS) complain(1, "hg forward failed");
        if (!g.quiet)
            printf("%d: launched %d\n", n, in.ret);

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
    pthread_cond_destroy(&is[n].scond);
    pthread_mutex_unlock(&is[n].slock);
    pthread_mutex_destroy(&is[n].slock);
    is[n].sends_done = 1;
    printf("%d: all sends complete\n", n);

skipsend:
    /* done sending, wait for network thread to finish and exit */
    pthread_join(is[n].nthread, NULL);
    if (is[n].remoteaddr) {
        HG_Addr_free(is[n].hgclass, is[n].remoteaddr);
        is[n].remoteaddr = NULL;
    }
    useprobe_end(&rp);
    printf("%d: all recvs complete\n", n);
    HG_Context_destroy(is[n].hgctx);
    HG_Finalize(is[n].hgclass);

    if (g.mode & MR_CLIENT) {
        double rtime = (rp.t1.tv_sec + (rp.t1.tv_usec / 1000000.0)) -
                       (rp.t0.tv_sec + (rp.t0.tv_usec / 1000000.0));
        printf("%d: client%s: %d rpc%s in %f sec (%f sec per op)\n",
               n, g.tagsuffix, g.count, (g.count == 1) ? "" : "s",
               rtime, rtime / (double) g.count);
    }

#ifdef RUSAGE_THREAD
    useprobe_print(stdout, &rp, "instance", n);
#endif
    printf("%d: instance done\n", n);
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
    struct is *isp = (struct is *)cbi->arg;
    hg_handle_t hand;
    hg_return_t ret;
    rpcout_t out;
    int oldmode;

    if (cbi->ret != HG_SUCCESS) complain(1, "forw_cb failed");
    if (cbi->type != HG_CB_FORWARD) complain(1, "forw_cb wrong type");
    hand = cbi->info.forward.handle;

    ret = HG_Get_output(hand, &out);
    if (ret != HG_SUCCESS) complain(1, "get output failed");

    if (!g.quiet)
        printf("%d: forw complete (code=%d)\n", isp->n, out.ret);

    HG_Free_output(hand, &out);

    if (HG_Destroy(hand) != HG_SUCCESS) complain(1, "forw_cb destroy hand");

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
    pthread_mutex_unlock(&isp->slock);

    return(HG_SUCCESS);
}

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
#ifdef RUSAGE_THREAD
    struct useprobe rn;
#endif
    unsigned int actual;
    hg_return_t ret;
    is[n].got = actual = 0;
    is[n].nprogress = is[n].ntrigger = 0;

    printf("%d: network thread running\n", n);
#ifdef RUSAGE_THREAD
    useprobe_start(&rn, RUSAGE_THREAD);
#endif

    /* while (not done sending or not done recving */
    while ( ((g.mode & MR_CLIENT) && !is[n].sends_done  ) ||
            ((g.mode & MR_SERVER) && is[n].got < g.count) ) {

        do {
            ret = HG_Trigger(is[n].hgctx, 0, 1, &actual);
            is[n].ntrigger++;
        } while (ret == HG_SUCCESS && actual);

        /* recheck, since trigger can change is[n].got */
        if (!is[n].sends_done || is[n].got < g.count) {
            HG_Progress(is[n].hgctx, 100);
        }

        is[n].nprogress++;
    }

#ifdef RUSAGE_THREAD
    useprobe_end(&rn);
#endif
    printf("%d: network thread complete (nprogress=%d, ntrigger=%d)\n", n,
           is[n].nprogress, is[n].ntrigger);
#ifdef RUSAGE_THREAD
    useprobe_print(stdout, &rn, "net", n);
#endif
}

/*
 * server side funcions....
 */

/*
 * rpchandler: called on the server when a new RPC comes in
 */
static hg_return_t rpchandler(hg_handle_t handle) {
    struct is *isp;
    struct hg_info *hgi;
    hg_return_t ret;
    rpcin_t in;
    rpcout_t out;

    /* gotta extract "isp" using handle, 'cause that's the only way pass it */
    hgi = HG_Get_info(handle);
    if (!hgi) complain(1, "rpchandler: bad hgi");
    isp = (struct is *) HG_Registered_data(hgi->hg_class, hgi->id);
    if (!isp) complain(1, "rpchandler: bad isp");

    ret = HG_Get_input(handle, &in);
    if (ret != HG_SUCCESS) complain(1, "rpchandler: HG_Get_input failed");
    if (!g.quiet)
        printf("%d: got remote input %d\n", isp->n, in.ret);
    out.ret = in.ret * -1;
    ret = HG_Free_input(handle, &in);

    /* the callback will bump "got" after respond has been sent */
    ret = HG_Respond(handle, reply_sent_cb, isp, &out);
    if (ret != HG_SUCCESS) complain(1, "rpchandler: HG_Respond failed");

    return(HG_SUCCESS);
}

/*
 * reply_sent_cb: called after the server's reply to an RPC completes.
 */
static hg_return_t reply_sent_cb(const struct hg_cb_info *cbi) {
    struct is *isp;
    if (cbi->type != HG_CB_RESPOND)
        complain(1, "reply_sent_cb:unexpected sent cb");
    isp = (struct is *)cbi->arg;

    /*
     * currently safe: there is only one network thread and we
     * are in it (via trigger fn).
     */
    isp->got++;

    /* return handle to the pool for reuse */
    HG_Destroy(cbi->info.respond.handle);
}

