import ftputil
from os import makedirs
from os.path import isfile
import argparse

import sys

from threading import Thread, Lock

from time import time, sleep

class ThreadedWorker():
    def __init__(self, thread_num, host, user, password):
        self.reset_time = 300

        self.ftp_host = host
        self.ftp_user = user
        self.ftp_password = password

        self.load_ftp()

        self.lock = Lock()

        self.thread_idx = thread_num

        self.queue = []

        self.session_age = time()

        self.thread = Thread(target=self.work)
        self.thread.start()

    def thread_print(self, string):
        print('[Thread {}] {}'.format(self.thread_idx, string))
        sys.stdout.flush()

    def load_ftp(self):
        self.ftp = ftputil.FTPHost(self.ftp_host, self.ftp_user, self.ftp_password)
        self.session_age = time()

    def should_reset_session(self):
        return (time() - self.session_age) > self.reset_time

    def reset_session(self):
        self.lock.acquire()
        try:
            self.thread_print("Resetting FTP Session!")
            self.ftp.close()
            self.ftp = None
            self.load_ftp()
        finally:
            self.lock.release()

    def enqueue(self, work):
        self.lock.acquire()
        try:
            self.queue.append(work)
        finally:
            self.lock.release()

    def dequeue(self):
        self.lock.acquire()
        try:
            if len(self.queue):
                return self.queue.pop(0)
            else:
                self.thread_print("Queue empty! Sleeping...")
                return None
        finally:
            self.lock.release()

    def work(self):
        while True:
            if self.should_reset_session():
                self.reset_session()

            item = self.dequeue()
            if item:
                self.thread_print("Downloading {}, Size of queue: {}".format(item[0], len(self.queue)))

                self.ftp.download(item[0], item[1])

                self.thread_print("Finished downloading {}!".format(item[0]))
            else:
                sleep(2)

    def stop(self):
        self.thread.shutdown()


class FTPDownloader():
    def __init__(self, host, username, password, num_threads=5, overwrite=False):
        self.ftp_hostname = host
        self.ftp_username = username
        self.ftp_password = password
        
        self.load_ftp()

        self.worker_idx = 0

        self.overwrite = overwrite

        self.workers = []

        self.number_of_threads = num_threads
        for idx in range(self.number_of_threads):
            print('[Main Thread]: Creating worker {}'.format(idx))

            worker = ThreadedWorker(idx, self.ftp_hostname, self.ftp_username, self.ftp_password)

            self.workers.append(worker)

            self.reset_time = 300

    def should_reset_session(self):
        return (time() - self.session_age) > self.reset_time

    def get_worker_idx(self):
        self.worker_idx += 1

        return self.worker_idx % self.number_of_threads

    def load_ftp(self):
        self.ftp = ftputil.FTPHost(self.ftp_hostname, self.ftp_username, self.ftp_password)
        self.session_age = time()

    def reset_session(self):
        print("[Main Thread]: Resetting FTP Session!")
        self.ftp.close()
        self.ftp = None
        self.load_ftp()

    def send_to_worker(self, item):
        idx = self.get_worker_idx()

        print("[Main Thread]: Sending {} to worker {}".format(item, idx))

        self.workers[idx].enqueue(item)

    def get_file(self, filepath, save_folder):
        if self.should_reset_session():
            self.reset_session()

        local_filepath = '{}/{}'.format(save_folder, filepath)

        self.send_to_worker( (filepath, local_filepath) )

    def get_all_from_folder(self, ftp_folder, save_folder='.'):
        if self.should_reset_session():
            self.reset_session()

        print('\n[ {} ]'.format(ftp_folder))

        files = self.ftp.listdir(ftp_folder)

        print('-- Downloading...')
        for name in files:
            filename = '{}/{}'.format(ftp_folder, name)

            if self.ftp.path.isdir(filename):
                makedirs(filename, exist_ok=True)
                self.get_all_from_folder(filename)
                continue

            local_filepath = '{}/{}'.format(save_folder, filename)

            local_dirs_to_make = '/'.join(local_filepath.split('/')[:-1])

            makedirs(local_dirs_to_make, exist_ok=True)

            self.send_to_worker( (filename, local_filepath) )

