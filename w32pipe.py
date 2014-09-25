import ctypes
import threading
import queue
import struct
import time

hk32 = ctypes.windll.LoadLibrary('kernel32.dll')

PIPE_ACCESS_DUPLEX =                    0x00000003
FILE_FLAG_FIRST_PIPE_INSTANCE =         0x00080000
PIPE_TYPE_BYTE =                        0x00000000
PIPE_TYPE_MESSAGE =                     0x00000004
PIPE_READMODE_MESSAGE =                 0x00000002
OPEN_EXISTING =                         0x00000003
GENERIC_READ =                          0x80000000
GENERIC_WRITE =                         0x40000000

class Mode:
    Master =            0
    Slave =             1
    Reader =            2
    Writer =            3
    SingleTransaction = 4

class Base:
    def readerentry(self, nph, client, mode):
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
                    ctypes.c_ulonglong(nph), buf, 4096, ctypes.c_char_p(cnt), 0
                )

            if ret == 0:
                rq.put(None)                    # signal reader that pipe is dead
                wq.put(None)                    # signal write thread to terminate
                client.alive = False
                return

            cnt = struct.unpack('I', cnt)[0]
            rawmsg = buf[0:cnt]
            rq.put(rawmsg)
            print('read', rawmsg)

            '''
                In slave mode we wait after reading so that we may be able
                to write a reply if needed. If we never need any replies
                then we should be using Mode.Reader instead of Mode.Slave.
            '''
            if mode == Mode.Slave:
                with client.rwait:
                    client.rwait.wait()

    def writerentry(self, nph, client, mode):
        wq = client.wq

        print('writer start')
        while True:
            rawmsg = wq.get()
            if rawmsg is None:
                return

            written = b'\x00\x00\x00\x00'

            print('writing %s' % rawmsg)

            ret = hk32['WriteFile'](
                ctypes.c_ulonglong(nph), ctypes.c_char_p(rawmsg), 
                ctypes.c_uint(len(rawmsg)), 
                ctypes.c_char_p(written),
                ctypes.c_uint(0)
            )

            print('wrote ret:%s;' % (ret))

            if ret == 0:
                self.alive = False        # signal the pipe has closed
                client.rq.put(None)       # signal the pipe has closed
                return

            if mode | Mode.SingleTransaction:
                client.endtransaction()


class ServerClient:
    def __init__(self, handle, mode, maxmessagesz):
        self.handle = handle
        self.rq = queue.Queue()
        self.wq = queue.Queue()
        self.alive = True
        self.mode = mode
        self.maxmessagesz = maxmessagesz
        self.rwait = threading.Condition()
        self.readingnow = False
        self.rlock = threading.Lock()

    def isalive(self):
        return self.alive

    def endtransaction(self):
        with self.rwait:
            self.rwait.notify()

    def read(self):
        if not self.alive:
            raise Exception('Pipe is dead!')
        if self.mode == Mode.Writer:
            raise Exception('This pipe is in write mode!')

        return self.rq.get()

    def write(self, message):
        if not self.alive:
            raise Exception('Pipe is dead!')
        if self.mode == Mode.Reader:
            raise Exception('This pipe is in read mode!')
        if self.mode == Mode.Slave and not self.rlock.acquire(blocking = False):
            raise Exception('The pipe is currently being read!')

        self.wq.put(message)

        # we must have held the lock to be here if we
        # are a slave.. so release it
        if self.mode == Mode.Slave:
            self.rlock.release()
        return True

    def canread(self):
        print('self.rq.empty', self.rq.empty())
        return not self.rq.empty()

    def close(self):
        hk32['CloseHandle'](ctypes.c_ulonglong(self.handle))

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

        print('self.handle', self.handle)

        if hk32['GetLastError']() != 0:
            err = hk32['GetLastError']()
            self.alive = False
            raise Exception('Pipe Open Failed [%s]' % err)
            return

        xmode = struct.pack('I', PIPE_READMODE_MESSAGE)
        ret = hk32['SetNamedPipeHandleState'](
            ctypes.c_ulonglong(self.handle),
            ctypes.c_char_p(xmode),
            ctypes.c_uint(0),
            ctypes.c_uint(0)
        )

        if ret == 0:
            err = hk32['GetLastError']()
            print('err', err)
            self.alive = False
            return

        self.client = ServerClient(self.handle, self.mode, self.maxmessagesz)

        if self.mode != Mode.Writer:
            thread = threading.Thread(target = self.readerentry, args = (self.handle, self.client, self.mode))
            thread.start()

        if self.mode != Mode.Reader:
            thread = threading.Thread(target = self.writerentry, args = (self.handle, self.client, self.mode))
            thread.start()

        self.alive = True
        return

    def close(self):
        hk32['CloseHandle'](ctypes.c_ulonglong(self.handle))

    def read(self):
        if not self.alive:
            raise Exception('Pipe Not Alive')
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

    def dropdeadclients(self):
        toremove = []
        for client in self.clients:
            if not client.alive:
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

            print('err', err)

            print('nph', nph)

            # wait for connection
            err = hk32['ConnectNamedPipe'](ctypes.c_uint(nph), ctypes.c_uint(0))

            if err == 0:
                hk32['CloseHandle'](ctypes.c_uint(nph))
                continue

            client = ServerClient(nph, self.mode, self.maxmessagesz)

            if self.mode != Mode.Writer:
                thread = threading.Thread(target = self.readerentry, args = (nph, client, self.mode))
                thread.start()

            if self.mode != Mode.Reader:
                thread = threading.Thread(target = self.writerentry, args = (nph, client, self.mode))
                thread.start()

            self.clients.append(client)

def getpipepath(name):
    return '\\\\.\\pipe\\' + name