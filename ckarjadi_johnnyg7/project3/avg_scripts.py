def get_avg(data):
        final = {}
        for x in data:
                if len(x) != 3:
                        continue
                z_c = x['zipcode']
                val = int(x['av_total'])
                if val !=0:
                        if z_c not in final:
                                final[z_c]= [val,1]
                        elif z_c in final:
                                final[z_c] = [final[z_c][0]+val,final[z_c][1]+1]

        fin_c = []

        for x in final:
                average = final[x][0] // final[x][1]
                d = {'zip_code': x, 'avg': average}
                fin_c+=[d]

        return fin_c

def get_Col(db,repo):
    '''input: string representing database name db
       output: returns list with all key,values from database db
    '''
    col_list = []
    for elem in repo['ckarjadi_johnnyg7.' + db].find({}):
        col_list.append(elem)
    return col_list
def get_Short(db, repo,limit):
        '''input: string representing database name db
        
       output: returns list with all key,values from database db
       '''
        assert(limit >0)
        col_list = []
        count = 0
        for elem in repo['ckarjadi_johnnyg7.' + db].find({}):
                count+=1
                col_list.append(elem)
                if count == limit:
                        break
                        
        return col_list

