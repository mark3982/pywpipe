import ctypes
import threading
import queue
import struct
import time
import sys

hk32 = ctypes.windll.LoadLibrary('kernel32.dll')

PIPE_ACCESS_DUPLEX =                    0x00000003
FILE_FLAG_FIRST_PIPE_INSTANCE =         0x00080000
PIPE_TYPE_BYTE =                        0x00000000
PIPE_TYPE_MESSAGE =                     0x00000004
PIPE_READMODE_MESSAGE =                 0x00000002
OPEN_EXISTING =                         0x00000003
GENERIC_READ =                          0x80000000
GENERIC_WRITE =                         0x40000000

if sys.maxsize > 2**32:
    def ctypes_handle(handle):
        return ctypes.c_ulonglong(handle)
else:
    def ctypes_handle(handle):
        return ctypes.c_uint(handle)

class Mode:
    Master =            0
    Slave =             1
    Reader =            2
    Writer =            3
    SingleTransaction = 4

    def is_slave(mode):
        return mode & 3 == Mode.Slave
    def is_master(mode):
        return mode & 3 == Mode.Master
    def is_reader(mode):
        return mode & 3 == Mode.Reader
    def is_writer(mode):
        return mode & 3 == Mode.Writer
    def is_strans(mode):
        #return mode & Mode.SingleTransaction == Mode.SingleTransaction
        return True


class Base:
    def readerentry(self, nph, client, mode, server):
        rq = client.rq
        wq = client.wq

        buf = ctypes.create_string_buffer(4096)

        while True:
            '''
                In master mode we wait to start trying to read until
                we get issue a command since trying to read here would
                block any writes. So we are released once one or more
                writes have been completed. If we never need to read
                then we should be using Mode.Writer not Mode.Master.
            '''
            if mode == Mode.Master:
                with client.rwait:
                    client.rwait.wait()

            cnt = b'\x00\x00\x00\x00'
            with client.rlock:
                ret = hk32['ReadFile'](
                    ctypes_handle(nph), buf, 4096, ctypes.c_char_p(cnt), 0
                )

            if ret == 0:
                rq.put(None)                    # signal reader that pipe is dead
                wq.put(None)                    # signal write thread to terminate
                client.alive = False
                return

            cnt = struct.unpack('I', cnt)[0]
            rawmsg = buf[0:cnt]
            rq.put(rawmsg)

            client.pendingread = False

            if server is not None:
                server.hasdata = True

            '''
                In slave mode we wait after reading so that we may be able
                to write a reply if needed. If we never need any replies
                then we should be using Mode.Reader instead of Mode.Slave.
            '''
            if Mode.is_slave(mode):
                with client.rwait:
                    client.rwait.wait()

    def writerentry(self, nph, client, mode):
        wq = client.wq

        while True:
            rawmsg = wq.get()
            if rawmsg is None:
                return

            written = b'\x00\x00\x00\x00'

            ret = hk32['WriteFile'](
                ctypes_handle(nph), ctypes.c_char_p(rawmsg), 
                ctypes.c_uint(len(rawmsg)), 
                ctypes.c_char_p(written),
                ctypes.c_uint(0)
            )

            if ret == 0:
                self.alive = False        # signal the pipe has closed
                client.rq.put(None)       # signal the pipe has closed
                return

            if (Mode.is_slave(mode) or Mode.is_master(mode)) and Mode.is_strans(mode):
                client.endtransaction()


class ServerClient:
    def __init__(self, handle, mode, maxmessagesz):
        self.handle = handle
        self.rq = queue.Queue()
        self.wq = queue.Queue()
        self.alive = True
        self.mode = mode
        self.maxmessagesz = maxmessagesz
        '''

        '''
        self.rwait = threading.Condition()
        '''
            The `pendingread` serves to prevent you from writing 
            again before getting a reply.
        '''
        self.pendingread = False
        '''
            The `rlock` serves to prevents you from issuing a write
            while a read operation is blocking.
        '''
        self.rlock = threading.Lock()

    def isalive(self):
        return self.alive

    def endtransaction(self):
        with self.rwait:
            self.rwait.notify()

    def read(self):
        # only throw exception if no data can be read
        if not self.alive and not self.canread():
            raise Exception('Pipe is dead!')
        if Mode.is_writer(self.mode):
            raise Exception('This pipe is in write mode!')

        return self.rq.get()

    def write(self, message):
        if not self.alive:
            raise Exception('Pipe is dead!')
        if Mode.is_reader(self.mode):
            raise Exception('This pipe is in read mode!')
        if Mode.is_slave(self.mode) and not self.rlock.acquire(blocking = False):
            raise Exception('The pipe is currently being read!')
        if Mode.is_master(self.mode) and self.pendingread:
            raise Exception('Master mode must wait for slave reply!')

        self.pendingread = True
        self.wq.put(message)
        if Mode.is_slave(self.mode):
            self.rlock.release()
        return True

    def canread(self):
        return not self.rq.empty()

    def close(self):
        hk32['CloseHandle'](ctypes_handle(self.handle))

class Client(Base):
    def __init__(self, name, mode, *, maxmessagesz = 4096):
        self.mode = mode
        self.maxmessagesz = maxmessagesz
        self.name = name
        self.handle = hk32['CreateFileA'](
            ctypes.c_char_p(b'\\\\.\\pipe\\' + bytes(name, 'utf8')),
            ctypes.c_uint(GENERIC_READ | GENERIC_WRITE),
            0,                      # no sharing
            0,                      # default security
            ctypes.c_uint(OPEN_EXISTING),
            0,                      # default attributes
            0                       # no template file
        )


        if hk32['GetLastError']() != 0:
            err = hk32['GetLastError']()
            self.alive = False
            raise Exception('Pipe Open Failed [%s]' % err)
            return

        xmode = struct.pack('I', PIPE_READMODE_MESSAGE)
        ret = hk32['SetNamedPipeHandleState'](
            ctypes_handle(self.handle),
            ctypes.c_char_p(xmode),
            ctypes.c_uint(0),
            ctypes.c_uint(0)
        )

        if ret == 0:
            err = hk32['GetLastError']()
            self.alive = False
            raise Exception('Pipe Set Mode Failed [%s]' % err)
            return

        self.client = ServerClient(self.handle, self.mode, self.maxmessagesz)

        if not Mode.is_writer(self.mode):
            thread = threading.Thread(target = self.readerentry, args = (self.handle, self.client, self.mode, None))
            thread.start()

        if not Mode.is_reader(self.mode):
            thread = threading.Thread(target = self.writerentry, args = (self.handle, self.client, self.mode))
            thread.start()

        self.alive = True
        return

    def endtransaction(self):
        self.client.endtransaction()

    def close(self):
        hk32['CloseHandle'](ctypes_handle(self.handle))

    def read(self):
        return self.client.read()

    def write(self, message):
        if not self.alive:
            raise Exception('Pipe Not Alive')
        return self.client.write(message)

class Server(Base):
    def __init__(self, name, mode, *, maxclients = 5, maxmessagesz = 4096, maxtime = 100):
        self.name = name
        self.mode = mode
        self.clients = []
        self.maxclients = maxclients
        self.maxmessagesz = 4096
        self.maxtime = maxtime
        self.shutdown = False
        self.t = threading.Thread(target = self.serverentry)
        self.t.start()
        self.hasdata = False

    def dropdeadclients(self):
        toremove = []
        for client in self.clients:
            if not client.alive and not client.canread():
                toremove.append(client)
        for client in toremove:
            client.close()
            self.clients.remove(client)

    def getclientcount():
        self.dropdeadclients()
        return len(self.clients)

    def getclient(self, index):
        return self.clients[index]

    def __iter__(self):
        for client in self.clients:
            yield client

    def __index__(self, index):
        return self.clients[index]

    def shutdown(self):
        self.shutdown = True

    def waitfordata(self, timeout = None, interval = 0.01):
        if self.hasdata:
            self.hasdata = False
            return True

        st = time.time()
        while not self.hasdata:
            if timeout is not None and time.time() - st > timeout:
                return False
            time.sleep(interval)
        self.hasdata = False
        return True

    def serverentry(self):
        while not self.shutdown:
            self.dropdeadclients()

            nph = hk32['CreateNamedPipeA'](
                ctypes.c_char_p(b'\\\\.\\pipe\\' + bytes(self.name, 'utf8')),
                ctypes.c_uint(PIPE_ACCESS_DUPLEX),
                ctypes.c_uint(PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE),
                ctypes.c_uint(self.maxclients),
                ctypes.c_uint(self.maxmessagesz), ctypes.c_uint(self.maxmessagesz),
                ctypes.c_uint(self.maxtime),
                ctypes.c_uint(0)
            )

            err = hk32['GetLastError']()

            '''
                ERROR_PIPE_BUSY, we have used up all instances
                of the pipe and therefore must wait until one
                before free
            '''
            if err == 231:
                time.sleep(2)
                continue

            # wait for connection
            err = hk32['ConnectNamedPipe'](ctypes.c_uint(nph), ctypes.c_uint(0))

            if err == 0:
                hk32['CloseHandle'](ctypes.c_uint(nph))
                continue

            client = ServerClient(nph, self.mode, self.maxmessagesz)

            if self.mode != Mode.Writer:
                thread = threading.Thread(target = self.readerentry, args = (nph, client, self.mode, self))
                thread.start()

            if self.mode != Mode.Reader:
                thread = threading.Thread(target = self.writerentry, args = (nph, client, self.mode))
                thread.start()

            self.clients.append(client)

def getpipepath(name):
    return '\\\\.\\pipe\\' + name