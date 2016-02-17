__author__ = 'Lo'

from matplotlib import pyplot as plt

x = list()
y = list()

rerank = 1
pre_rank = 1
pre_frequency = None
'''
popularity_rank file example:
1  5000
2  4840
3  4820
4  4315
5  4315
6  3788
7  3715
8  3396
...
...
'''
with open('popularity_rank', 'rb') as INPUT:
    for line in INPUT.readlines():
        rank, frequency = line.split()
        # correct the same frequency might have different ranks
        if frequency == pre_frequency:
            rerank = pre_rank
        else:
            rerank = rank
            pre_frequency = frequency

        print rerank, frequency
        x.append(rerank)
        y.append(frequency)
        pre_rank = rerank


plt.loglog(x, y, basex=10, basey=10)  # log-log plot
plt.title('Popularity distribution')
#plt.plot(x, y)  # normal x-y plot
plt.show()

