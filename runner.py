import time
import subprocess
import sys
import signal

SUBPROCESSES = [
    "listener",
    "subscriber"
]

def launch_subprocesses():
    procs = []
    for script in SUBPROCESSES:
        print(f"Launching {script}...")
        proc = subprocess.Popen([sys.executable, "-m", script])
        procs.append(proc)
        time.sleep(0.5)  # slight delay to stagger startup
    return procs

def terminate_subprocesses(procs):
    print("\nTerminating subprocesses...")
    for proc in procs:
        proc.send_signal(signal.SIGINT)
        proc.wait()
    print("All subprocesses terminated.")

def run_demo():
    procs = launch_subprocesses()
    try:
        print("\nServices running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        terminate_subprocesses(procs)

if __name__ == "__main__":
    run_demo()