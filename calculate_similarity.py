class text_similarity:
    def __init__(self,df,list_obj,similarity):#text_similarity(df,list_obj,similarity).df
        df.fillna('',inplace=True)
        self.df=df
        
        # if len(df)<5:
            # print('数据长度为',len(df),'不进行相似度标注')
        if len(df)>0:    
            try:
                t0=time.time()
                df=df.reset_index(drop=True)
                list_obj_modified=[]
                column=''
                for obj in list_obj:#合并列
                    if obj in df.columns:
                        list_obj_modified.append(obj)
                        column=column+obj+','
                df['combine']=''
                for obj in list_obj_modified:
                    df['combine']=df['combine']+df[obj].fillna('').astype(str).str.lower()
                df_dict={} 
                print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'开始判断相似度')
                for index,row in df.iterrows(): 
                    try:
                        if index==0:
                            print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'开始分词')
                        # print(i) 
                        single_wordlist=[]
                        a=str(df['combine'][index])   
                        change=re.compile("[^\u4e00-\u9fa5^.^a-z^A-Z^0-9]")
                        a = change.sub('', a)
                        seg_list = list(set(jieba.cut(a))) # 默认是精确模式
                        for x in seg_list:
                            if len(x)==1 or len(x)>10:
                                seg_list.remove(x)
                        if len(seg_list)<6:
                            while('  ' in a):
                                a=a.replace('  ',' ')
                            seg_list=a.split(' ')
                            if len(seg_list)<5:
                                seg_list=list(set(a))
                        df.at[index,'word_length']=len(seg_list)
                        df.at[index,'word']=','.join(seg_list)
                    except Exception as e:
                        print('分词第',index,'失败',e)
                    
                df.sort_values(by="word_length",  ascending=False, inplace=True)
                df=df.reset_index(drop=True)
                
                for index,row in df.iterrows():   
                    word_list=row['word'].split(',')
                    df_dict[index]={word_list[0]}
                    for word in word_list:
                        df_dict[index].add(word)
                  
                print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'判断相似度---分词完成')
                error_code=1
                try: 
                    df.drop(columns=['topic#','topic'],inplace=True)
                except:pass
                try: 
                    df.insert(0,'topic#', 1)
                except:pass
                try: 
                    df.insert(0,'topic', '0-topic')
                except:pass
                dict_topic={}
                tagged_topic=[]
                for i in df_dict:
                    if  len(df_dict[i])<3:
                        # stop=i
                        break
                t1=time.time()
                error_code=2
                if len(df)<2000:
                    x=2001
                else:
                    x=200
                for i in df_dict:   
                    if i==x:
                        error_code=3
                        t2= time.time()
                        esti_time=round(0.9*((len(df)/x)*(t2-t1)/60),1)
                        print('-----预计此次判断相似度还需耗时',esti_time,'min-----')
                        
                    for count in [0.25,0.5,0.75]:
                        error_code=4
                        if i==round(len(df)*count):
                            print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'已完成',round(count*100,0),'%')
                    
                    # if i==stop:
                    #     break
                    if  df['topic'][i]!='0-topic':       
                        continue
                    error_code=5
                    # print('    判断相似度---判断数据',i)
                    dict_topic['topic'+str(i)]={i}
                    error_code=6
                    for j in df_dict:
                        if j>i:
                            error_code=7
                            if  df['topic'][j]!='0-topic': 
                                error_code=7.1
                                continue     
                            if len(df_dict[i])<2*len(df_dict[j]):
                                error_code=7.2
                                if len(df_dict[i])+len(df_dict[j])-len(set.union(df_dict[i],df_dict[j]))>(1-float(similarity))*len(df_dict[j]):
                                    error_code=7.3
                                    dict_topic['topic'+str(i)].add(j)
                    error_code=8
                    if len(dict_topic['topic'+str(i)])>1:
                        df['topic'].loc[dict_topic['topic'+str(i)]]='topic'+str(i)
                        df['topic#'].loc[dict_topic['topic'+str(i)]]=len(dict_topic['topic'+str(i)])
                # print('    判断相似度---判断数据完成')
                error_code=9
                df.drop(columns=['combine', 'word_length' ,'word'],inplace=True)
                df.sort_values(by=['topic#','topic'], ascending=False, inplace=True)
                t3=time.time()
                print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'判断相似度完成，共用时',round((t3-t0)/60,1),'min')
                self.df=df
            except Exception as e:
                print(e,'\n',error_code)
