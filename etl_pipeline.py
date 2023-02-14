#!/usr/bin/env python
# coding: utf-8

# In[37]:


#Importing necessary libraries 
import pandas as pd
import os 

#obtaining cwd for writing files
cwd = os.getcwd()
path = cwd + "/"

#Loading DataFrames 
cons_df = pd.read_csv('cons.csv', 
                      usecols = ['cons_id', 'subsource'])

emails_df = pd.read_csv('cons_email.csv',
                        usecols = ['cons_id','is_primary','email','create_dt','modified_dt'])

sub_df = pd.read_csv('cons_email_chapter_subscription.csv', 
                     usecols = ['cons_email_id','isunsub','chapter_id'])

#Cleaning column names for to make merging dataframes easier
sub_df.rename(columns={'cons_email_id':'cons_id'},inplace=True)

#only including subscription records where chapter_id is 1
sub_df = sub_df[sub_df['chapter_id'] == 1]

#merging...
people_df = pd.merge(pd.merge(cons_df,emails_df, on='cons_id'),sub_df, on='cons_id')

#saving 'people.csv' to current working directory 
people_df.to_csv(path + 'people.csv')

#Subsetting dataframe for acquisition_facts.csv
acq_facts_df = pd.DataFrame(people_df['create_dt'])

#simplifying the "create_dt" timestamp to only include month-date-year
acq_facts_df['create_dt'] = pd.DatetimeIndex(acq_facts_df['create_dt']).normalize()

#creating filler data to assist aggregation 
acq_facts_df['acquisitions'] = 1

#grouping acquisitions by their creat_dt date 
acq_groupby = acq_facts_df.groupby(by='create_dt').sum()

#saving 'acquisition_facts.csv' to current working directory 
acq_groupby.to_csv(path +'acquisition_facts.csv')

