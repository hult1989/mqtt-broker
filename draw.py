import matplotlib.pyplot as plt


rtt = []
filename = 'rtt.txt'
with open(filename) as f:
    for line in f:
        rtt.append(int(line))

rtt.sort()

yaxis = [100.0 * i/len(rtt) for i in xrange(len(rtt))]

plt.plot(rtt, yaxis)
plt.axis([0, max(rtt), 0, 100])
plt.xlabel('Delay(milliseconds)')
plt.ylabel('Cumulative fraction of messages')
plt.grid(True)
plt.show()
