#!/usr/bin/env python3

import os
import time
import argparse
import pandas as pd #importing neccisary libraries 
import numpy as np
import ast
import datetime
import calendar
from neo4j import GraphDatabase 
from progress.bar import Bar
import logging

logging.basicConfig(level=logging.WARNING)
CSVS =['feb2020.csv', 'april2020.csv', 'july2020.csv','june2021.csv']


def get_db_session(db_uri="bolt://localhost:7687", auth=("neo4j", "test")):
    """"""
    driver = GraphDatabase.driver(uri=db_uri, auth=auth) 
    return driver.session()


def process_safegraph_month(safegraph_csv_fp):
    """"""
    session = get_db_session()
    df = pd.read_csv(safegraph_csv_fp) # read csv to pandas

    #isolate specified colums will be changing them to more meanningful names

    # Will add another part to have all the csv be read at once so I do not have to keep loading it.
    #This for loop creates and loads all nodes and edges

    # breakpoint()
    #len(df)
    #this for loop I am createing all the places and cities and zips

    with Bar(f'Processing {len(df)} rows', max=len(df)) as bar:
        for i, row in df.iterrows():

            # DEBUG: benchmark performance
            start = time.time()

            #setting variables that I will use to create the nodes. 
            miamivv = row.visitor_home_cbgs
            pk= row.placekey
            name = row.location_name
            zipc= row.postal_code
            address= row.street_address
            lat= row.latitude
            long= row.longitude
            topCat= row.top_category
            subCat= row.sub_category
            city= row.city
            if((str(row.poi_cbg) == 'nan') == False):
                poicbg = str(row.poi_cbg)
                poiST= str(poicbg[:2])
                poiCTY= str(poicbg[2:5])
                check = 0 # This is telling the program that this row has the information we need
                
            
            else:        
                check = 1 # if not we skip over this row
                    
        
            if (check == 0):
                #These are the queries that I will pass to neo4j that will create all the place, city and zip nodes    
                q1='MERGE(p:Place{name:'+'"'+name+'"' +', city:'+'"' +city+'"'+', zipcode:'+"'" +str(zipc)+"'" +', placeKey:'+"'" +pk+"'" +', address:'+'"' +address+'"' +', topCatagory:'+'"'+str(topCat)+'"'+', subCatagory:'+'"'+str(subCat)+'"' +', cbg: '+"'" +str(poicbg) +"'" +', location:point({x:toFloat('+str(lat)+'),y:toFloat('+str(long)+')})})'
                q2='MERGE(z:Zip{name: '+"'" +str(zipc)+"'" +', zipC:'+"'"+str(zipc)+"'"+', city:'+"'" +city+"'"+'});'
                q3='MERGE(c:City{name: '+'"' +city+'"'+',  city:'+"'" +city+"'"+'});'
                q4='MATCH (p:Place), (z:Zip) WHERE p.name = '+'"' +name +'"' + 'AND p.city = '+"'" + city +"'"+ 'AND z.city = '+"'" +city+"'" + 'AND p.zipcode = '+"'" +str(zipc)+"'" +' AND z.name = '+"'" +str(zipc)+"'" +' MERGE(p)-[r:IS_WITHIN]->(z);'
                q5='MATCH (c:City), (z:Zip) WHERE c.name = '+'"' +city +'"' +' AND z.city = '+"'" +city+"'" + ' AND z.name = '+"'" +str(zipc)+"'" +' MERGE(z)-[r:IS_WITHIN]->(c);'             
                q6='MERGE(g:CensusBG{cbg:'+"'" +str(poicbg)+"'"+ ', State:'+"'"+str(poiST)+"'"+' , County:'+"'"+str(poiCTY)+"'"+' })'
                q7='MATCH (p:Place), (g:CensusBG) WHERE p.placeKey = '+'"' +pk+'"' +' AND g.State = '+"'" +str(poiST)+"'" +' AND g.cbg = '+"'" +str(poicbg)+"'" + ' MERGE(p)-[r:IS_WITHIN]->(g);'
                #Match staments are the edges that connect the two nodes
        
                #running the querries 
                session.run(q1)
                session.run(q2)
                session.run(q3)
                session.run(q4)
                session.run(q5)
                session.run(q6)
                session.run(q7)

                logging.info(f'run queries: {(time.time() - start):.4f}')
        
                # miamivv =  df.visitor_home_cbgs
                #json ->dictionary-> nparray
                if isinstance(miamivv, float):
                    iti= 0

                else:
                    test=ast.literal_eval(miamivv)
                    test2 = list(test.items())
                    testnpp = np.array(test2)
                    iti = len(testnpp)


                if iti != 0:

                    da=0
                    dasum=0
                    percentage = []
                    rawVists = row.raw_visit_counts
                    dates= row.date_range_start
                    rawVisitors = row.raw_visitor_counts
            
                    #parsing the date month and year
                    year=dates[:4]
                    month=dates[5:7]
                    monthName=calendar.month_name[int(month)]
            
                    for x in range(iti):

                        da = int(testnpp[x][1])   
                        dasum += da # sum 
                        percentage =[]
                    
            
                    for j in range(len(testnpp)):
                        #here I am createing Census block nodes and connecting them with the places they have been. 
                        odds = (float((testnpp[j][1]))/dasum)  
                        percentage += [odds]
                        p1= float(percentage[j] *100)
                        evc= float(((p1/100) * rawVists))
                        NoCBGVisitors = float(100*((rawVisitors-dasum)/rawVisitors))            
                        node= testnpp[j][0] # this is the keys
                            
                        if(node[0].isupper() == True):
                            nodee =(node)
                            state = node[:2]
                            county = node[3:]
                    
                
                        else:                
                            nodee = float(node)
                            prop= testnpp[j][1] # this is the values         
                            state = node[:2]
                            county = node[2:5]
                
                        qq1= 'MERGE(g:CensusBG{cbg:'+"'" +str(nodee)+"'"+ ', State:'+"'"+state+"'"+' , County:'+"'"+county+"'"+' })'
                        qq2 ='MATCH (p:Place), (g:CensusBG) WHERE p.placeKey = '+"'" +pk +"'"  +' AND g.cbg = '+"'" +str(nodee)+"'" +' MERGE(g)-[b:Visited{ PercentofVisits:'+str(p1)+', EstimatedVisits:'+str(evc)+', NoCbgPercentage:'+str(NoCBGVisitors)+', month:'+"'"+str(monthName)+"'"+', year:'+str(year)+'}]->(p)'
                
                        session.run(qq1) 
                        session.run(qq2)
                    logging.info(f'until node/edge creation: {(time.time() - start):.4f}')
            logging.info(f'full iter: {(time.time() - start):.4f}')
            # assert 0
            bar.next()


def main(safegraph_csv_fp, n_threads, **kwargs):
    # data_dir = os.path.join('data', 'safegraph')
    # safegraph_csv_fp = os.path.join(data_dir, 'feb2020.csv')
    assert os.path.isfile(safegraph_csv_fp), f"file at {safegraph_csv_fp} does not exist"
    _ = process_safegraph_month(safegraph_csv_fp=safegraph_csv_fp)


def get_opts() -> dict:
    """Get options from command line"""
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("-v", "--verbose", action="store_true", required=False, help="")
    # parser.add_argument('file', type=argparse.FileType('r'), help="")
    parser.add_argument("-f", "--safegraph-csv-fp", type=str, required=True, help="path(s) to safegraph CSV file")
    parser.add_argument("-n", "--n-threads", type=int, default=16, required=False, help="number of threads to use")
    return vars(parser.parse_args())


if __name__ == '__main__':
    main(**get_opts())
