import queue
import subprocess
import sys
import threading
import time
import unittest

from trainer.tab import TrainTab, _TrainingCancelled


class TrainStopTests(unittest.TestCase):
    def test_preprocess_process_can_be_cancelled(self):
        tab = object.__new__(TrainTab)
        tab.log_queue = queue.Queue()
        tab.prep_process = None
        tab._cancel_event = threading.Event()
        tab._process_lock = threading.Lock()
        errors = []

        def run_blocking():
            try:
                tab._run_blocking(
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    ".",
                    "test",
                )
            except Exception as error:
                errors.append(error)

        worker = threading.Thread(target=run_blocking)
        worker.start()
        deadline = time.monotonic() + 10
        while tab.prep_process is None and time.monotonic() < deadline:
            time.sleep(0.05)

        self.assertIsNotNone(tab.prep_process)
        proc = tab.prep_process
        tab._cancel_event.set()
        tab._terminate_process_tree(proc)
        worker.join(timeout=10)

        self.assertFalse(worker.is_alive())
        self.assertIsNotNone(proc.poll())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], _TrainingCancelled)


if __name__ == "__main__":
    unittest.main()
