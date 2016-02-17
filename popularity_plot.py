__author__ = 'Lo'

from matplotlib import pyplot as plt

x = list()
y = list()

rerank = 1
pre_rank = 1
pre_frequency = None
with open('popularity_rank', 'rb') as INPUT:
    for line in INPUT.readlines():
        rank, frequency = line.split()
        if frequency == pre_frequency:
            rerank = pre_rank
        else:
            rerank = rank
            pre_frequency = frequency

        print rerank, frequency
        x.append(rerank)
        y.append(frequency)
        pre_rank = rerank


plt.loglog(x, y, basex=10, basey=10)
plt.title('Popularity distribution')
#plt.plot(x, y)
plt.show()

