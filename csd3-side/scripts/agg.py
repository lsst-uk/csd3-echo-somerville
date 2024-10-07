a = [1, 2, 1, 2, 2, 1, 1]
b = [['a'], ['b'], ['c'], ['d'], ['e'], ['f'], ['g']]
m = 3

at = []
bt = []

current_sum = 0
current_list = []

for i in range(len(a)):
    while a[i] > 0:
        if current_sum + a[i] <= m:
            current_sum += a[i]
            current_list.extend(b[i])
            a[i] = 0
        else:
            remaining = m - current_sum
            current_sum += remaining
            current_list.extend(b[i][:remaining])
            b[i] = b[i][remaining:]
            a[i] -= remaining
        
        if current_sum <= m:
            at.append(current_sum)
            bt.append(current_list)
            current_sum = 0
            current_list = []

# Handle any remaining values if the last sum didn't reach m
if current_sum > 0:
    at.append(current_sum)
    bt.append(current_list)

print("at:", at)
print("bt:", bt)