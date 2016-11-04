import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]



def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

##client = dml.pymongo.MongoClient()
##repo = client.repo
##repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
##startTime = datetime.datetime.now()

def get_Col(db,repo):
    '''input: string representing database name db
       output: returns list with all key,values from database db
    '''
    col_list = []
    for elem in repo['ckarjadi_johnnyg7.' + db].find({}):
        col_list.append(elem)
    return col_list

def get_avg(key,col):
    '''input string key; list col representing collections
       ouput the average of those key's values   '''
    total = 0
    for x in col:
        total+=int(x[key])
    return total // len(col)
def get_match(key1,key2,col1,col2):
    '''input string key1,key2; list col representing a collection
       output: union of two key,value sets from 2 collections if key1's
       value = key2's value'''
    final_col = []
    #zip_codes =[]
    for x in range(len(col1)):
        for y in range(len(col2)):
            if col1[x][key1]==col2[y][key2]:
                #print(col1[x][key1],col2[y][key2],x,y)
                #zip_codes +=col1[x][key1]
                f_i = x
                p_i=y
                final_col+=[[col1[f_i],col2[p_i]]]
                #final_col+=[{'food_id': x,'prop_id': y}]

    return final_col


    
def get_value(col,value):
    final = []
    for x in col:
        if x[value] not in final:
            final.append(x[value])
    return final
##def create_dic(source,col1,col2):
##    final = []
##    check=[]
##    for z in source:
##        p_i = z['prop_id']
##        f_i = z['food_id']
##        final+=[[col1[f_i],col2[p_i]]]
##        check+=[col1[f_i]['zip_code']==col2[p_i]['zipcode']]
##    print(check)
##    return final
##        
##prop_Val = get_Col("propVal")
##foodPan = get_Col("foodPan")
##hospitals = get_Col("Hospitals")
###print(prop_Val[0]['gross_tax'])
##
##tax_avg = get_avg('gross_tax',prop_Val)
##
##prop_Val_under = [x for x in prop_Val if int(x['gross_tax'])<tax_avg]
##
##same_zips = get_match('zip_code','zipcode',foodPan,prop_Val_under)

def get_more(key1,col1,key2,col2):
    '''inputs; strings key1 and key2; col1 is [[{x},{y}]]; col2 is [{},{}]
'''
    final_col=[]
    for x in col1:
        for y in col2:
            
            if x[0][key1] == y[key2]:
                final_col+=[x+[y]]
    return final_col
##add_hospitals = get_more('zip_code',same_zips,'location_zip',hospitals)
#print(add_hospitals)
def create(col):
    fin = []
    
    for x in col:
        d={}
        zip_c = x[0]['zip_code']
##        if zip_c not in d:
        d['zip']=zip_c
        d['food_area']=x[0]['area']
        d['prop_addr']=x[1]['owner_mail_address']
        d['hosp_area']=x[2]['neigh']
        d['prop_val'] = x[1]['gross_tax']
        fin+=[d]
#        print(zip_c)
#        print(d['prop_addr'])
##        else:
##            d['food_area']+=x[0]['area']
##            d['prop_area']+=x[1]['owner_mail_cs']
##            d['hosp_area']+=x[2]['neigh']
    return fin
##final = create(add_hospitals)
###print(final)
##repo.dropPermanent("Food_Prop_Hosp_ZipCodes")
##repo.createPermanent("Food_Prop_Hosp_ZipCodes")
##repo['ckarjadi_johnnyg7.Food_Prop_Hosp_ZipCodes'].insert_many(final)


##for x in add_hospitals:
##    repo['ckarjadi_johnnyg7.Food_Prop_Hosp_ZipCodes'].insert_one(add_hospitals)
##
#add_hospitals is now in the format: [[{foodpan},{prop_val},{hosp}],
#[{foodpan},{prop_val},{hosp}]]
# each element in add_h is a list containing 3 respective dicts; all same zip

##for x in add_hospitals:
##
##    print(x[0]['zip_code'],x[1]['zipcode'],x[2]['location_zip'])
##                


#same zips is a list in which each element has two dictionaries;
#element[0] = foodPan entry; [1] = prop_Val_under entry
#both entries have the same zip code
#[ [{foodPan},{propVal}], [{foodpan},{propVal}]] etc
##print(same_zips)
##for x in same_zips:
##    print(x[0]['zip_code'],x[0]['name'],x[1]['full_address'])
##    
##print(len(same_zips))
##for x in same_zips:
##    print(x[0]['zip_code'],x[1]['zipcode'])




def create_final(col,i_key,c_key,name):
    d={}
    fin=[]
    check=[]
    for x in col:
        
        
        temp = x[c_key]
        
        #temp is the zipcode
        if temp not in check:
            d[temp]=[temp,1]
            check+=[temp]
        else:
            
            d[temp][1]+=1
        
    
   
    for x in d:
        
        fin+=[{i_key:d[x][0],name:d[x][1]}]

    
    return [fin,check]
    
def create_food(col,i_key,name,compare,final):
    check=[]
    
    d={}
    for x in col:
        if x[i_key] in compare and x[i_key] not in check:
            d[i_key]=1
            check+=[i_key]
        elif x[i_key] in compare and x[i_key] in check:
            d[i_key]+=1
    
    for x in final:

        x[name]=d[i_key]
    
    return final

def create_prop(col,c_key,i_key,name,compare,final):
    check=[]
    
    d={}
    for x in col:
        try:
            zip_c = x[c_key]
            if x[c_key] in compare and x[c_key] not in check:
                
                d[zip_c]=[x['gross_tax'],1]
                check+=[c_key]
            elif x[c_key] in compare and x[c_key] in check:
                d[zip_c][0]+=x['gross_tax']
                d[zip_c][1]+=1
        except KeyError:
            continue
    for x in d:
        d[x]=int(d[x][0]) // int(d[x][1])

    for x in final:
        zip_code = x[i_key]
        if zip_code in d:
            x[name]=d[zip_code]
    
    return final
class same_zip(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = []
    writes = ['ckarjadi_johnnyg7.Food_Prop_Hosp_ZipCodes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        prop_Val = get_Col("propVal",repo)
        foodPan = get_Col("foodPan",repo)
        hospitals = get_Col("Hospitals",repo)
        
    
        master = create_final(hospitals,'zip_code','location_zip','hospitals')
        master_hosp = master[0]
        master_zips = master[1]

        master = create_food(foodPan,'zip_code','food_Pantries',master_zips,master_hosp)
        master = create_prop(prop_Val,'zipcode','zip_code','propery_value_average',master_zips,\
                             master)
        
        #print(master)                             
#ef create_prop(col,c_key,i_key,name,compare,final):
        #tax_avg = get_avg('gross_tax',prop_Val)

        #prop_Val_under = [x for x in prop_Val if int(x['gross_tax'])<tax_avg]
##        prop_Val_under=prop_Val
##        same_zips = get_match('zip_code','zipcode',foodPan,prop_Val_under)
##        add_hospitals = get_more('zip_code',same_zips,'location_zip',hospitals)

##        final = create(add_hospitals)
        #print(final)
        repo.dropPermanent("Food_Prop_Hosp_ZipCodes")
        repo.createPermanent("Food_Prop_Hosp_ZipCodes")
        repo['ckarjadi_johnnyg7.Food_Prop_Hosp_ZipCodes'].insert_many(master)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}




    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        #<ckarjadi_johnnyg7>#<somefile_name>
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        this_script = doc.agent('alg:same_zip', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:hospital_reduction', \
            {'prov:label':'Food Pantries, Property Values under average, Hospitals reduced by Zip Code', \
             prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
        get_FPH = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_FPH, this_script)
        doc.usage(get_FPH, resource1, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'???'
                }
            )
        FPH = doc.entity('dat:ckarjadi_johnnyg7#FPH', {prov.model.PROV_LABEL:'Food Pantries, Property Values under average, Hospitals reduced by Zip Code', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(FPH, this_script)
        doc.wasGeneratedBy(FPH, get_FPH, endTime)
        doc.wasDerivedFrom(FPH, resource1, get_FPH, get_FPH, get_FPH)
        repo.record(doc.serialize())
        repo.logout()
        return doc
same_zip.execute()
doc = same_zip.provenance()
#print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
