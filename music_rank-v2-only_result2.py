
# coding: utf-8

# In[14]:

from bs4 import BeautifulSoup
import sys, urllib.request
import requests
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.animation as anim
from collections import Counter
get_ipython().magic('matplotlib inline')


# In[25]:

rank_no=[]
artist=[]
music=[]
twitter=[]


# In[26]:

def get_single_item_data(item_url, number_start =0,num_end =100):
        source_code = urllib.request.urlopen(item_url)  # get all source code
        source_text = source_code.read()
        #plain_text = source_text.decode('utf-8')
        #output = open('plain_text','w+')
        #output.write(plain_text)
        #output.close()
        soup = BeautifulSoup(source_text,"html.parser")
        #toplist = soup.find_all('table',{'class':'smv'}).find_all("tr")[0].find_all("td")td[3]
        toplist = soup.find_all('table',{'class':'smv'})
        Top_Emerging = toplist[0] # Emerging
        Top_Trending = toplist[1] # Trending updating fast in 6 seconds
        Top_Overall = toplist[2] # overall Comprehensive result
        #print(toplist[2])
        rank_list = toplist[1].find_all('tr')
        for rank in rank_list[number_start:num_end]:
            rank_no.append(rank.find_all('td')[0].text)
            artist.append(rank.find_all('td')[1].text)
            music_text = rank.find_all('td')[2].text
            music.append(music_text[0:(music_text.find("\n"))-1])
            twitter.append(rank.find_all('td')[1].find('a').get("href"))
        for i in np.arange(len(artist)).tolist()[number_start:number_start+1]:   ######print first 6-10
            print("ranking No:"+str(i+1)+"   artist:"+artist[i]+"  music:"+music[i]+'\n')
            #print(twitter[i]+'\n')


# In[27]:

#print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$  11-30 song and Singers need to be promoted  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n")
get_single_item_data("http://realtime.billboard.com/?chart=trending140", 5, 45)


# In[19]:

Top_Trending_df = pd.DataFrame({'No_':rank_no, 'artist': artist, 'music':music, 'Twitter':twitter})


# In[20]:

def addprice_discount_pd (raw_pd, price_low, price_high):
    pd_length = len(list(raw_pd.index))
    price = ['{0:.2f}'.format(np.random.uniform(price_low,price_high)) for i in list(range(pd_length))]  
    # price distribution can be optimized currently uniform
    discount =['{0:.1f}'.format(1.01-0.01*int(rank)) for rank in list(raw_pd.No_)]  # discount will be optimized later
    raw_pd["price"] = pd.Series(price, index=raw_pd.index)
    raw_pd["discount"] = pd.Series(discount, index=raw_pd.index)
    return 
  


# In[21]:

addprice_discount_pd(Top_Trending_df, 8,10)


# In[22]:

#Top_Trending_df[0:10]


# In[24]:

#Top_Trending_df.describe()


# In[10]:

'''count = dict(Counter(artist))
#print(count)

xVals = list(count.keys())
yVals = list(count.values())
print("------------top 20 songs------"+str(len(np.unique(xVals)))+" singers in total---------")
#print(np.unique(xVals))
x = tuple(np.arange(len(xVals)))
width = 0.4
ax = plt.subplot(111)
ax.set_position([5,2,2.5,1])
ax.bar(x, yVals, width)
ax.set_xticks([i+width/2 for i in x])
ax.set_xticklabels(xVals)
plt.rcParams['font.size'] = 80'''


# In[11]:

#sns.factorplot(y='artist', data=Top_Trending_df, kind='count',size=10,linewidth=1)


# In[12]:

Top_Trending_df.to_csv("Top_Trending_top6_45_price_discount.csv", index=False)


# In[ ]:




# In[ ]:



