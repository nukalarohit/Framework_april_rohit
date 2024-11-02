l=[1,2,1,2,3,4]

remdup=[]
dup=[]
uniq=[]

for i in l:
    if i not in remdup:
        remdup.append(i)
    else:
        dup.append(i)
    for j in remdup:
        if j not in dup:
            uniq.append(j)

print(remdup)
print(dup)
print(uniq)