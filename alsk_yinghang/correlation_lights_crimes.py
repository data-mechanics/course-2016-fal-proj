import json
import dml
import prov.model
import datetime
import pandas as pd
from bson.son import SON
import uuid
import scipy.stats

class correlation_lights_crimes(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = ['alsk_yinghang.crime_lights']
    writes = ['alsk_yinghang.lights_crimes']

    @staticmethod
    def execute(trial=False):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')
        n = 0
        lights_crimes = {}
        for doc in repo['alsk_yinghang.crime_lights'].find():
            if n > 10000 and trial:
              break
            num_of_lights = doc['num_of_lights']
            if num_of_lights not in lights_crimes.keys():
                lights_crimes[num_of_lights] = 1
            else:
                lights_crimes[num_of_lights] += 1
            n+=1

        temp = []
        for num_of_lights in lights_crimes.keys():
            num_crimes = lights_crimes[num_of_lights]
            temp.append({'num_of_lights': num_of_lights, 'num_of_crimes': num_crimes})

        repo.dropPermanent("lights_crimes")
        repo.createPermanent("lights_crimes")
        repo['alsk_yinghang.lights_crimes'].insert_many(temp)

        print('Determining correlation.....')
        data = [(num_of_lights, lights_crimes[num_of_lights]) for num_of_lights in lights_crimes.keys()]
        num_lights = [x for (x, y) in data]
        num_crimes = [y for (x, y) in data]

        result = scipy.stats.pearsonr(num_lights, num_crimes)

        correlation_cofficient = result[0]

        pvalue = result[1]

        print('The correlation coefficient is: ' + str(correlation_cofficient) + '.')
        if correlation_cofficient < 0:
            print('There is a negative correlation between the number of lights and the number of crimes.')
        elif correlation_cofficient > 0:
            print('There is a positive correlation between the number of lights and the number of crimes.')
        else:
            print('This tells us that there does not appear to be any correlation between the number of lights and the number of crimes.')
        print('The p-value obtained is: ' + str(pvalue) + '.')
        if pvalue < 0.01:
            print("The result is statistically highly significant ")
        print('According to scipy online documentation, "The p-value roughly indicates the probability of an uncorrelated system producing datasets that have a Pearson correlation at least as extreme as the one computed from these datasets. "')


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        return

correlation_lights_crimes.execute()
