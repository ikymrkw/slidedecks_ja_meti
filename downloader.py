import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Timer, Semaphore

import requests

class TimedGate():
    def __init__(self, wait_sec=1):
        self.wait_sec = wait_sec
        self.semaphore = Semaphore()
        self.open_gates = []
    def open(self, wait_msg=None):
        return self.OpenGate(self, wait_msg)
    def close(self):
        for og in self.open_gates:
            og._close()
    def __enter__(self):
        return self
    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.close()

    class OpenGate():
        def __init__(self, gate, wait_msg):
            self.wait_msg = wait_msg
            self.gate = gate
            self.timer = None
            gate.open_gates.append(self)
        def __enter__(self):
            if self.gate.semaphore.acquire(blocking=False):
                return self
            if self.wait_msg is not None:
                print(self.wait_msg)
            self.gate.semaphore.acquire(blocking=True)
            return self
        def __exit__(self, ex_type, ex_value, ex_traceback):
            self.timer = Timer(self.gate.wait_sec, self._release)
            self.timer.start()
        def _release(self):
            self.gate.semaphore.release()
            self.gate.open_gates.remove(self)
            self.timer = None
        def _close(self):
            if self.timer is not None:
                self.timer.cancel()


class Downloader():
    def __init__(self, wait_sec = 5, wait_message = None, user_agent=None):
        self.wait_sec = wait_sec
        self.wait_message = wait_message
        self.user_agent = user_agent
        self.timed_gate = TimedGate(wait_sec)

    def download(self, url, filepath):
        headers = {}
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        with self.timed_gate.open(self.wait_message):
            with requests.get(url, stream=True, headers=headers) as r:
                with open(filepath, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
        return filepath

    def close(self):
        self.timed_gate.close()


if __name__ == "__main__":
    # Simple test for human eyes
    with TimedGate(1) as gate:
        for _ in range(5):
            with gate.open("waiting the gate open..."):
                print("passed the gate and doing something")

