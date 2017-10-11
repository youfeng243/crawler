import threading
import logging
from common.log import log as logger
from thrift.server import TServer
from socket import error
from i_util.global_defs import STOP_TOKEN


# The python version of thrift server doesn't have a fucking way to be gracefully stopped!
# We have to hack the way out of it. I don't wanna do this either, sigh.
# by dongyun.zdy.

class InterruptableThreadPoolServer(TServer.TThreadPoolServer):

    def __init__(self, *args, **kwargs):
        TServer.TThreadPoolServer.__init__(self, *args, **kwargs)
        self.running = False

    def serveThread(self):
        while self.running:
            try:
                client = self.clients.get()
                if client == STOP_TOKEN:
                    break
                self.serveClient(client)
            except Exception as x:
                logger.exception(x)

    def serve(self):
        # modify the original routine to break on interrupt, in order to exit gracefully.

        threads = []
        self.running = True
        for i in range(self.threads):
            try:
                t = threading.Thread(target=self.serveThread)
                t.setDaemon(self.daemon)
                t.start()
                threads.append(t)
            except Exception as x:
                logger.exception(x)

        # Pump the socket for clients
        self.serverTransport.listen()
        while True:
            try:
                client = self.serverTransport.accept()
                if not client:
                    continue
                self.clients.put(client)
            except error as e:
                if e.errno == 4:
                    logger.info("Thrift server interrupted, shutdown listen socket...")
                    self.serverTransport.close()
                    self.running = False
                    logger.info("Telling thrift workers to die... count = %d" % len(threads))
                    for t in threads:
                        self.clients.put(STOP_TOKEN)
                    logger.info("Waiting for thrift threads to die... count = %d" % len(threads))
                    for t in threads:
                        t.join()
                    logger.info("Thrift threads all stopped. count = %d" % len(threads))
                    break
            except Exception as x:
                logger.exception(x)

