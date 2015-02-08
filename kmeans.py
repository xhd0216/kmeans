import math, random
import threading
from threading import Thread
def worker(data, KGroups, rg, assignment, dist_method, cols):
    i = rg[0]
    result = []
    while i <= rg[1]:
        m = -1.0
        g = -1
        ##print 'working on row ', i
        for h in range(len(KGroups)):
            di = distance(dist_method, data[i], KGroups[h], cols)
            if m < 0 or di < m:
                g = h
                m = di
        result.append(g)
        ##print 'row ', i, data[i], 'goes to group', g, m
        i+=1
    assignment[rg[0]:rg[1]+1] = result
def divideData(nr, nt):
    result = []
    start = 0
    for i in range(nt):
        end = int(math.ceil(nr / nt * (i+1)))
        if i == nt -1:
            end = nr-1
        result.append([start, end])
        start = end + 1
    return result
def distance(dm, a, b, cols):
    if dm == 'manhattan':
        s = 0.0
        for i in cols:
            s += abs(a[i] - b[i])
        return s
    if dm == 'euclidean':
        s = 0.0
        for i in cols:
            s += (a[i] - b[i]) * (a[i] - b[i])
        return math.sqrt(s)
    else:
        print 'unknown distance method'
        return -1
def initialK(data, k, nrows):
     t = random.sample(range(nrows), k)
     return [data[i] for i in t]
def ListHashing(a):
    s = '-'.join([str(b) for b in a])
    return s
def updateCenters(assignments, data, KGroups, k):
    for i in range(k):
        temp = [data[j] for j in range(len(data)) if assignments[j] == i]
        n = len(temp)
        if n == 0:
            KGroups[i] = [0 for j in range(len(data[0]))]
        else:
            KGroups[i] = [sum([t[j] for t in temp])/float(n) for j in range(len(temp[0]))]
def kmeans(data, k, dist = 'manhattan', hasLabel = False, n_threads = 4, maxRounds = 100):
    nrows = len(data)
    ncols = len(data[0])
    rows = range(nrows)
    ##----should validate parameters here
    cols = range(ncols)
    if hasLabel:
        cols = range(1, ncols)
        ncols -= 1
    ## get initial k groups
    KGroups = initialK(data, k, nrows)
    nRounds = 0
    assignments = [0 for i in range(nrows)]
    checked = set()
    d = divideData(nrows, n_threads)
    while nRounds < maxRounds:
        nRounds += 1
        print '-------round ', nRounds
        print 'new centers are:'
        for j in KGroups:
            print j
        start = 0
        ths = []
        for i in range(n_threads):
            t = threading.Thread(target=worker, args=(data, KGroups, d[i], assignments, dist, cols,))
            ths.append(t)
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        del ths
        updateCenters(assignments, data, KGroups, k)
        st = ListHashing(assignments)
        if st in checked:
            break
        checked.add(st)
    return assignments
        
            
