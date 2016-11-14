def calculate(data2012, data2013, data2014, data2015):
    num_points = len(data2012[1]) 
    years_data = [data2012,data2013,data2014,data2015]

    #results = 45 lists
    trends = [[]]*num_points
    for year in range(0,3): 
        if year==0:
            data2012 = years_data[year][1]
            data2013 = years_data[year+1][1]
            for i in range(0,num_points):
                p = data2012[i]
                distances = []
                for q in data2013:
                    distances += [dist(p,q)]
                if len(distances)==0: continue
                index = distances.index(min(distances))
                trends[i] = [p,data2013[index]]
                data2013.pop(index)
        else:
            for i in range(0,num_points):
                p = trends[i][year]
                distances = []
                data_year = years_data[year+1][1]
                for q in data_year: 
                    distances += [dist(p,q)]
                if len(distances)==0: continue
                index = distances.index(min(distances))
                trends[i] += [data_year[index]] 
                data_year.pop(index) 
    return trends

def dist(p,q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2