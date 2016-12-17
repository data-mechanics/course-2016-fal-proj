

def write_file(filename,collection):
    filename = filename + '_OUTPUT.json'
    out = open(filename,'w')
    print('Attempting to write', filename + '...')
    check = len(collection)
    
    for x in range(len(collection)):
        collection[x] = str(collection[x])
        if x == 0:
            collection[x] = '[' + collection[x]
            #print(collection[x])
        if x!= check-1:
            
            collection[x] +=','
        if x == check-1:
            collection[x] += ']'
        print(type(collection[x]))
        print(collection[x],'orig')
        collection[x] = collection[x].replace("'",'"')
        print(collection[x],'replace quotes')
        collection[x] = collection[x].replace("ObjectId(","{")
        print(collection[x],'replace obj id')
        collection[x] = collection[x].replace(")","}")
        print(collection[x],'replace ) with }')
        out.write(collection[x])
    
    print('Wrote',filename + '!')
