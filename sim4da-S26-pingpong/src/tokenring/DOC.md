Run locally with: cd C:\Users\valen\IdeaProjects\2026S-DS\sim4da-S26-pingpong; javac -d build\manual-check src\tokenring\*.java; ..\.venv\Scripts\python.exe scripts\run_tokenring_experiment.py

RingNode behavior was extended to support a final multicast that is broadcasted by node 0 as last action to kill the entire ring. Each node cleans up after itself closing all udp endpoints and releasing them for the next RingNode initialization.

The listener has been extended to also listen for the STOP signal and then endpoint and ports are closed by that node.