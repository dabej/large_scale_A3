from problem1 import Summary
import time
if __name__ == '__main__':
    mr_job = Summary(args=[
        '-r', 'local', '/data/2020-DAT346-DIT873-TLSD/DATASETS/assignment3.dat'])
    with mr_job.make_runner() as runner:
        t0 = time.time()
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
    t1 = time.time()
    print(t1-t0)
