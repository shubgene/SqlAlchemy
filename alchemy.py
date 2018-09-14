# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 00:42:48 2018

@author: shurastogi
"""
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite:///C:\\Users\\shurastogi\\Desktop\\dataAdult.db', echo=True)
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
class Adult(Base):
    __tablename__='sqladb'
    age=Column(Integer)
    workclass=Column(String)
    fnlwgt=Column(Integer,primary_key=True)
    education=Column(String)
    education_num =Column(Integer)
    marital_status=Column(String)
    occupation =Column(String)
    relationship =Column(String)
    race =Column(String)
    sex =Column(String)
    capital_gain =Column(Integer)
    capital_loss  =Column(Integer)
    hours_per_week =Column(Integer) 
    native_country =Column(String)
    label =Column(String)

Adult.__table__ 
Base.metadata.create_all(engine)

session = sessionmaker()
session.configure(bind=engine)
s = session()
try:
    data = pd.read_csv("https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data",sep=",\s",header=None,engine='python')
    count=0
    for index, ad in data.iterrows():
        adult_row = Adult(age=ad['age'],workclass=ad['workclass'],fnlwgt=ad['fnlwgt'],education=ad['education'],education_num=ad['education_num'],marital_status=ad['marital_status'],occupation=ad['occupation'],relationship=ad['relationship'],race=ad['race'],sex=ad['sex'],capital_gain=ad['capital_gain'],capital_loss=ad['capital_loss'],hours_per_week=ad['hours_per_week'],native_country=ad['native_country'],label=ad['label'])
        s.add(adult_row)
except:
    s.rollback() 
finally:
    s.close() 

##2 Update Queries
##update all asian-pac_islander to white_asian
from sqlalchemy import update
from sqlalchemy import and_, or_, not_
stmt = update(Adult).where(Adult.race=='Asian-Pac-Islander').values(race='white_asian')
engine.execute(stmt)
## update salary off all employee who works in india for more than 50 hours.perweek
stmt = update(Adult).where(and_(Adult.native_country=='India',Adult.hours_per_week>50)).values(label='>50K')
engine.execute(stmt)


##2 Delete Queries
#delete all rows with no workclass
from sqlalchemy import delete
stmt_del=delete(Adult).where(Adult.workclass=='?')
engine.execute(stmt_del)

#delete all row where private employee work less than 2 hour in week
stmt_del=delete(Adult).where(and_(Adult.workclass=='Private',Adult.hours_per_week<2))
engine.execute(stmt_del)

##4 Filter Queries
#find education for all male who work in private sector
private_male_education = s.query(Adult).filter(and_(Adult.workclass=='Private',Adult.sex=='Male')).all()
for row in private_male_education:
    print(row.education)
    
#find all people income working in private sector below age 18
private_below_18=s.query(Adult).filter(Adult.age<18)
for row in private_below_18:
    print(row.label)


## function queries
from sqlalchemy import func
private_male_avg_hour = s.query(func.avg(Adult.hours_per_week)).filter(and_(Adult.workclass=='Private',Adult.sex=='Male')).all()
print(private_male_avg_hour)

#minimum age by sector
min_age=s.query(func.min(Adult.age).label('min_a')).group_by(Adult.workclass).all()
print(min_age[0].min_a)