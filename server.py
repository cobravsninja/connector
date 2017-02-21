import time
import sys
import threading
import socket
import multiprocessing
import signal
import connector
import config

terminate = False

db = getattr(__import__(config.db_type, fromlist=['db']), 'db')

def sig_handler(signum,frame):
    global terminate
    print signum,'has been received, exiting'
    terminate = True

def thread(pid,socket):
    t = threading.currentThread()
    db_connection = db.DB(config.db_user,
            config.db_pass,config.db_host,config.db_instance)
    bashi = connector.connector(pid,t.getName(),db_connection)
    while True:
        client,address = socket.accept()
        client.send(bashi.check_data(client.recv(4096)))
        client.close()

def worker(socket):
    p = multiprocessing.current_process()
    print 'Starting:',p.name,p.pid
    threads = []
    for i in range(config.threads_qt):
        t = threading.Thread(target=thread,args=(p.pid,socket,))
        t.setDaemon(True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket.bind((config.socket_address,config.socket_port))
socket.listen(1)

for i in range(config.forks_qt):
    p = multiprocessing.Process(target=worker,args=(socket,))
    p.daemon = True
    p.start()

while 1:
    signal.signal(signal.SIGINT,sig_handler)
    time.sleep(1)
    if terminate is True:
        print "I'm done, bye"
        sys.exit(0)
