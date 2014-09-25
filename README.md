PyWPipe
=====
This module makes it much cleaner and easier to use named pipes under Windows. It does not offer
a feature rich API, but I may add more methods to support various other features such as setting
the security of the pipe and support byte mode. At the moment only message mode is supported.

API
=====

    wpipe.Server(name, mode, *, maxclients = 5, maxmessagesz = 4096, maxtime = 100):
    wpipe.Client(name, mode, *, maxmessagesz = 4096)

Examples
=====

An example of a master and slave pipe mode.

    import wpipe

The server code:

    pserver = wpipe.Server('darkguard', pipe.Mode.Slave)
    while True:
        for client in pserver:
            while client.canread():
                rawmsg = client.read()
                client.write(b'hallo')    
        pserver.waitfordata()

The client code:

    pclient = wpipe.Client('darkguard', pipe.Mode.Master)
    while True:
        pclient.write(b'hello')
        reply = pclient.read()

The client blocks on `pclient.read` but you can perform a non-blocking operation by
using `pclient.canread` before. The server client items are removed if the pipe is
dead and not data can be read so they will not be iterated over.

In master and slave mode a single write or read is allowed before performing the
opposite operation. The master writes a message and is then expected to read one
message, and the slave reads one message and is expected to write one message.

You may notice the server is placed into slave mode and the client into master which
may seem backwards however in some cases this is desired and useful. You could place
the server into master mode and the client into slave mode.    
