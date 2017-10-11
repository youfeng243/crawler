import os
import platform
import socket
import subprocess
import threading
import time

from queue import Queue, Empty

from i_util.logs import LOG_DIRECTORY
from i_util.tools import get_project_path


class PhantomjsServer(threading.Thread):
    def __init__(self, conf, script_path = None, log_path = None):
        threading.Thread.__init__(self)
        self.daemon = True
        self.conf = conf
        self.running = True
        self._sig_block = Queue(2)
        self.log = conf['log']
        self.restart_interval = self.conf['phantomjs'].get("restart_interval", 3600)
        self.phantomjs_path = self.conf['phantomjs']['path']
        if not os.path.isfile(self.phantomjs_path):
            raise Exception("%s not exist!" %(self.phantomjs_path))
        if not script_path:
            script_path = '%s/i_downloader/downloader/phantom.js' % get_project_path()
        if not os.path.isfile(script_path):
            raise Exception("%s not exist!" %(script_path))
        self.script_path = script_path
        if not log_path:
            log_path = "%sphantom_server.log" % LOG_DIRECTORY
        self._log = open(log_path, "a")
        self._server_info = self._create_server()
        self.start()

    def get_server_info(self):
        return self._server_info

    def run(self):
        self.running = True
        while self.running:
            try:
                self._sig_block.get(timeout=self.restart_interval)
                break
            except Empty as e:
                self.log.info("Time to restart phantomjs")
            except Exception as e:
                self.log.error(e.message)
            self._restart_phantomjs_process()

    def stop(self):
        self.log.debug("Phantomjs server stop")
        self.running = False
        self._sig_block.put(1)
        if self.is_alive():
            self.join()
        if self._server_info:
            self.stop_process(self._server_info.get("process", None))
        if self._log:
            self._log.close()
        self.log.debug("Phantomjs server stop finished")

    def _restart_phantomjs_process(self):
        old_process = None
        if self._server_info:
            old_process = self._server_info.get("process")
        self._server_info = self._create_server()
        self.stop_process(old_process)

    def _create_server(self, proxy = None):
        self.log.debug("Create server")
        process = None
        now = time.time()
        for i in range(3):
            try:
                port = self._free_port()
                process = self._create_server_process(port, proxy=proxy)
                now = time.time()
                break
            except Exception as e:
                self.log.error(e)
        if process is None:
            self.log.error("Failed to create phantomjs server")
        server = {}
        server['process'] = process
        server['server_addr'] = 'http://127.0.0.1:%s' % port
        server['proxy'] = proxy
        server['birth'] = now
        server['stat'] = 0
        return server

    def stop_process(self, process):
        """
                Cleans up the process
                """
        # If its dead dont worry
        if process is None:
            return

        # Tell the Server to properly die in case
        try:
            if process:
                process.stdin.close()
                process.kill()
                process.wait()
        except OSError:
            # kill may not be available under windows environment
            pass

    def _create_server_process(self, port, proxy=None):
        args = []
        args.append(self.phantomjs_path)
        args.append('--load-images=false')
        if proxy:
            args.append('--proxy=%s:%s' % (proxy['host'], proxy['port']))
            args.append('--proxy-auth=%s:%s' % (proxy['username'], proxy['password']))
        args.append(self.script_path)
        args.append(str(port))
        args.append('steady')
        try:
            process = subprocess.Popen(' '.join(args), shell=True,
                                       stdin=subprocess.PIPE, close_fds=platform.system() != 'Windows',
                                        stdout=self._log, stderr=self._log)

        except Exception as e:
            raise Exception("Unable to start phantomjs %s: %s" % (port, e))
        count = 0
        while not self.is_connectable(port):
            count += 1
            time.sleep(0.5)
            if count == 6:
                 self.stop_process(process)
                 raise Exception(
                     "Can not connect to Phantom Server on port {}".format(port))
        self.log.info("create phantomjs server %s success" %(str(port)))
        return process

    def _free_port(self):
        """
        Determines a free port using sockets.
        """
        free_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        free_socket.bind(('0.0.0.0', 0))
        free_socket.listen(5)
        port = free_socket.getsockname()[1]
        free_socket.close()
        return port

    def is_connectable(self, port):
        """
        Tries to connect to the server at port to see if it is running.

        :Args:
         - port: The port to connect.
        """
        result = False
        socket_ = None
        try:
            socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_.settimeout(1)
            socket_.connect(("127.0.0.1", port))
            result = True
        except socket.error:
            result = False
        finally:
            if socket_:
                socket_.close()
        return result
