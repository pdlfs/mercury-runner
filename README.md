# mercury-runner

The mercury-runner program is a multi-instance mercury send/recv
test program.  It contains both a mercury RPC client and RPC server.
The client sends "count" number of RPC requests and exits when
all replies are in.  The server receives "count" number of RPC
  requests and exits when all requests have been processed.

To use the program you need to run two copies of it.  By
default both client and server are active so RPCs flow
in both directions (so each process is a peer).   If you only
want a one way flow of RPC calls, run one copy of the program
as a client and run the other as a server.

The mercury-runner program can run multiple instances of mercury
in the same process.   In this case, server port numbers are assigned
sequentially starting at the "baseport" (default value 19900).
On the command line, specify the port numbers as a printf "%d"
and the program will fill in the value.  For client-only mode,
we init the client side with port numbers that are just past
the server port numbers.

Note: the program currently requires that the number of instances
and number of RPC requests to create and send must match between
the processes.

By default the client side of the program sends as many RPCs as
possible in parallel.  You can limit the number of active RPCs
using the "-l" flag.  Specifying "-l 1" will cause the client side
of the program to fully serialize all RPC calls.

# command line usage

```
   usage: mercury-runner [options] ninst localspec [remotespec]

   options:
    -c count     number of RPCs to perform
    -l limit     limit # of concurrent client RPC requests ("-l 1" = serial)
    -m mode      c, s, cs (client, server, or both)
    -p baseport  base port number
    -q           quiet mode - don't print during RPCs
    -t secs      timeout (alarm)

   note that "remotespec" is optional if mode is "s" (server-only)
```

The program prints the current set of options at startup time.
The default count is 5 RPCs, the default limit is set to the count
(the largest possible value), and the default timeout is 120 seconds.
The timeout is to prevent the program from hanging forever if
there is a problem with the transport.   Quiet mode can be used
to prevent the program from printing during the RPCs (so that printing
does not impact performance).

# examples

 one client and one server mode, serialized sending, one instance:

```
      client:
      ./mercury-runner -l 1 -c 50 -q -m c 1 cci+tcp://10.93.1.210:%d \
                             cci+tcp://10.93.1.233:%d
      server:
      ./mercury-runner -c 50 -q -m s 1 cci+tcp://10.93.1.233:%d
```
Note that the -c's must match on both sides.

both processes send and recv RPCs (client and server), one
instance, both sides sending in parallel:

```
      ./mercury-runner -c 50 -q -m cs 1 cci+tcp://10.93.1.210:%d \
                             cci+tcp://10.93.1.233:%d

      ./mercury-runner -c 50 -q -m cs 1 cci+tcp://10.93.1.233:%d \
                             cci+tcp://10.93.1.210:%d
```

Check the Mercury documentation to see what transports are supported
(cci, bmi, gni, etc.).

# to compile

First, you need to know where mercury is installed and you need cmake.
To compile with a build subdirectory, starting from the top-level
source dir:

```
  mkdir build
  cd build
  cmake -DCMAKE_PREFIX_PATH=/path/to/mercury-install ..
  make
```

That will produce binaries in the current directory.  "make install"
will install the binaries in CMAKE_INSTALL_PREFIX/bin (defaults to
/usr/local/bin) ... add a -DCMAKE_INSTALL_PREFIX=dir to change the
install prefix from /usr/local to something else.
