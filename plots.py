import matplotlib.pyplot as plt 

def plot(data_x, data_y, data_y2, Title, xlabel, ylabel):
    plt.plot(data_x, data_y, label = 'Phase1 time')
    plt.plot(data_x, data_y2, label = 'Total time')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(Title)
    plt.legend()
    plt.show()

# inputs = [ 5, 50, 500, 1000, 2000, 5000]
# inputs = [10, 25, 50, 100, 250, 500]
inputs = [2,3,4,5,6,7,8,9]
phase1 = [
4.607,
4.559,
4.556,
4.458,
4.573,
4.530,
4.461,
4.413
]
times = [
5.629,
5.660,
5.801,
5.785,
5.942,
5.986,
5.956,
6.186
]

# plot(inputs, phase1, times, "File Size vs Time (Memory 50 MB)", "File Size (MB)", "Time (in sec)")
# plot(inputs, phase1, times, "Memory vs Time (Fixed File Size 500 MB)", "Main Memory (MB)", "Time (in sec)")
plot(inputs, phase1, times, "Number of Threads vs Time (File Size 50 MB, Memory Size 30 MB)", "Number of Threads", "Time (in sec)")