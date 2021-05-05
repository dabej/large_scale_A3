from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob
import statistics
import numpy as np
import math
import time


class Summary(MRJob):

    def configure_args(self):
        super(Summary, self).configure_args()
        self.add_passthru_arg('--group', default='all',
                              help="Specify the group nr")

    def mapper(self, _, line):
        id_num, group, value = line.split("\t")
        if self.options.group == 'all' or self.options.group == group:
            yield(None, float(value))

    def combiner(self, key, values):
        smsq, sm, cnt = 0, 0, 0
        min_val = math.inf
        max_val = - math.inf
        all_vals = []
        for v in values:
            all_vals.append(v)
            smsq += v**2
            sm += v
            cnt += 1
            if min_val > v:
                min_val = v
            if max_val < v:
                max_val = v
        summary = {
            "sm": sm,
            "smsq": smsq,
            "cnt": cnt,
            "min_val": min_val,
            "max_val": max_val,
            "all_vals": all_vals
        }
        yield (None, summary)

    def reducer(self, _, values):

        smsq, sm, cnt = 0, 0, 0
        min_val = math.inf
        max_val = - math.inf
        all_vals = []

        for v in values:
            all_vals += v["all_vals"]
            sm += v["sm"]
            smsq += v["smsq"]
            cnt += v["cnt"]
            if min_val > v["min_val"]:
                min_val = v["min_val"]
            if max_val < v["max_val"]:
                max_val = v["max_val"]

        mn = sm/cnt
        stdev = np.sqrt(smsq/cnt-mn**2)
        histogram_counts = np.histogram(all_vals)[0].tolist()

        summary = {
            "mean": mn,
            "stdev": stdev,
            "min": min_val,
            "max": max_val,
            "counts": histogram_counts
        }
        yield("summary", summary)


"""the below 2 lines are ensuring the execution of mrjob, the program will not
execute without them"""
if __name__ == '__main__':
    # for c in [1, 2, 4]:
    t0 = time.time()
    # args=['--num-cores', str(c)]
    Summary().run()
    t1 = time.time()
    print("duration: ", t1-t0)
