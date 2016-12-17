
'''
MONGODB_COLLECTION_TO_JSON_FORMAT

This python file takes in a filename of a .json file from a mongodb collection,
and converts it to proper json format.

'''
path = input("Enter filename of file to be changed: ")
original = path
path+= '.json'
f = open(path,'r')
text = f.readlines()
f.close()
text[0] = '[' + text[0]
text[-1] = text[-1] + ']'

out = open(original + '_OUTPUT.json', 'w')
check = len(text)

for x in range(len(text)):
    if x!= check-1:
        text[x] += ','
        print(text[x])
        print(x)
    out.write(text[x])
print(text)
out.close()

