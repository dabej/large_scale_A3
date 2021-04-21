from mrjob.job import MRJob
import statistics
import numpy as np


class Summary(MRJob):

    def mapper(self, _, line):
        id_num, group, value = line.split("\t")
        yield(None, float(value))

    def reducer(self, _, values):
        sm, cnt = 0, 0

        for v in values:
            sm += v
            cnt += 1

        mean = sm/cnt
        var = np.mean([(v-mean)**2 for v in values])

        mn = statistics.mean(values)
        var = np.mean([(v-mn)**2 for v in values])

        summary = {
            "mean": mean,
            "stdev": np.sqrt(var)
        }
        yield("summary", sm)


"""the below 2 lines are ensuring the execution of mrjob, the program will not
execute without them"""
if __name__ == '__main__':
    Summary.run()
