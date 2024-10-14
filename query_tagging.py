class tagsys0402:
    # tag_result=tagsys0402(df,tag,list_obj,stats_export_path)           
    # df=tag_result.df     
    # tag_poster= tag_result.tag
    def __init__(self,df,tag,list_obj,stats_export_path,cover):#
        df.fillna('',inplace=True)

        if stats_export_path!='':# and (path.exists(stats_export_path))==True:
            os.chdir(stats_export_path)
        tag=process_tag(tag).tag
        preprocess_df_result=preprocess_df(df,list_obj)
        df=preprocess_df_result.df
        series_content=preprocess_df_result.series_content
        # print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'开始打标签预处理')
        t1= time.time()
        df_dict= {}
        
        if len(tag)>20:
            x=10
        else:
            x=2
        if len(tag)==1:
            x=1
        if len(tag)>100:
            x=20
        if len(tag)>600:
            x=50
        if len(tag)>1500:
            x=100 
        error_code='total0'
        # print(cover)
        list_tag=tag['字段名'].to_list()
        list_tag=list(set(list_tag))
        self.added_columns=list_tag.copy()
        while( 'keyword关键词' in self.added_columns):
            self.added_columns.remove('keyword关键词')
        if cover!=True:               
            list_tag.append('keyword关键词' )
            list_tag=list(set(list_tag))
            
            for i in range(len(list_tag)):
                error_code='total0.1'
                fild_name=list_tag[i]
                error_code='total0.2'
                # def file_name 
                if fild_name in df.columns:
                    # print('      ',fild_name,'标签已存在于原数据,正在处理')
                    df_dict[fild_name]={}
                    error_code='total0.3'
                    for j,row in df.iterrows():
                        try:
                            for each in str(df[fild_name][j]).split(','):
                                if 'total' not in df_dict[fild_name]:
                                    df_dict[fild_name]['total']={j}                                 
                                error_code='total0.4'
                                each=each.lower()
                                
                                if each=='' or each=='nan' :
                                    continue
                                if each in df_dict[fild_name]:
                                    df_dict[fild_name][each].add(j)
                                    df_dict[fild_name]['total'].add(j)
                                else:
                                    df_dict[fild_name][each] = {j}
                                    df_dict[fild_name]['total'].add(j)
                        except Exception as e:
                            print(e,error_code,fild_name)
                        
                  
                    #
        # print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'开始打标签')    
        tag=tag.reset_index(drop=True)
        df=df.reset_index(drop=True)

        for i in range(len(tag)):#'字段名',	'标签名'	,'关键词',	'排除词',	'nearrule']    
            try:                     
                if i==x:
                    t2= time.time()
                    esti_time=round(1.5*((len(tag)/x)*(t2-t1)/60),1)
                    print('-----预计此次打标签需耗时',esti_time,'min-----')
                    
                for count in [0.25,0.5,0.75]:
                    if i==round(len(tag)*count):
                        print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'已完成',round(count*100,0),'%')
                exclude=tag['排除词'][i]
                keyword=tag['关键词'][i]
                fild_name=tag['字段名'][i]
                tag_name=tag['标签名'][i]
                nearrule=tag['nearrule'][i]         
                error_code='total0'
                # print(i,keyword)
                try:
                    error_code='total1'
                    if (i >0 and (keyword!=tag['关键词'][i-1] or exclude!=tag['排除词'][i-1] or nearrule!=tag['nearrule'][i-1])) or i==0:
                        error_code='total1.1'
                        tag_result=self.tag_single_word(keyword,exclude,series_content,nearrule)
                        error_code='total1.2'
                        self.tag_result=tag_result
                        error_code='total1.3'
                        series_TF=tag_result[0]  
                        error_code='total1.4'
                        if series_TF.any():    
                            error_code='total1.5'                                                                 
                            series_keyword=tag_result[1]
                    error_code='total2'       
                    if series_TF.any(): 
                        error_code='total2.1'
                        if fild_name not in df_dict:
                            df_dict[fild_name]={}
                            
                            for index, value in series_TF.items():
                                df_dict[fild_name]['total']={index}                                                            
                                df_dict[fild_name][tag_name]={index}
                                break                    
                        else:
                            if tag_name not in df_dict[fild_name]:
                                for index, value in series_TF.items():
                                    df_dict[fild_name][tag_name]={index}                              
                                    break
                        error_code='total2.2'
                        for index, value in series_TF.items():
                            df_dict[fild_name]['total'].add(index) 
                            df_dict[fild_name][tag_name].add(index) 
                                  
                        error_code='total2.3' 
                        
                            
                        for index, value in series_keyword.items():
                            if 'keyword关键词' not in df_dict:
                                df_dict['keyword关键词']={}
                            if value not in df_dict['keyword关键词']:
                                df_dict['keyword关键词'][value]={index}
                            else:
                                df_dict['keyword关键词'][value].add(index)                                     
                except Exception as e:
                    print('failtag',keyword,'i:',i,'\nfail code:',error_code,e,'\n')
            except Exception as e:
                print('fail tag',keyword,'i:',i,'\nfail code:',error_code,e,'\n')
        print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'已完成 100%')
        # try:
        #     print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'开始输出统计表格')
        #     error_code='total3' 
        #     df_table=pd.DataFrame()
        #     df_table['column']=''
        #     df_table['key']=''
        #     df_table=df_table.set_index(['column','key'])
        #     df_table.at[('total','total'),'buzz']=len(df)
        #     for  column in df_dict:
        #         try:
        #             error_code='total3.1'
        #             if column =='keyword关键词':
        #                 continue
        #             for key in df_dict[column]:
        #                 error_code='total3.2'
        #                 try:
        #                     error_code='total3.3'
        #                     df_table.at[(column,key),'buzz']=len(df_dict[column][key])
        #                 except Exception as e:
        #                     print('fail export stats',column,key,'fail code:',error_code,e)
        #         except Exception as e:
        #             print('fail export stats',column,'fail code:',error_code,e)
        #     exportname='统计'+time.strftime("_%H.%M.%S", time.localtime())+'.xlsx'                
        #     df_table.to_excel(exportname, merge_cells=False) 
        #     print('\n 已输出统计表格',exportname,'\n 文件位置：',os.getcwd(),'\n')    
        # except Exception as e:
        #     print('fail export stats','fail code:',error_code,e)
        try:
            print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'开始生成数据')
            error_code='total4'
            for column in df_dict:
                try:
                    error_code='total4.1'
                    if column in df.columns:                       
                        df.drop(columns=[column],inplace=True)   #***************2022.7.21 改过 
                        df[column]=''
                    else:
                        df[column]=''
                        
                    for key in df_dict[column]:
                        if key=='total':
                            continue
                        try:
                            error_code='total4.1.1'
                            df_add=pd.DataFrame(['']*len(df),columns=['temp'])
                            error_code='total4.1.2'
                            indexi=df_dict[column].get(key) 
                            error_code='total4.1.3'
                            df_add.loc[indexi,'temp']=key+','
                            error_code='total4.1.4'
                            df[column]=df_add['temp']+df[column] 
                        except Exception as e:
                            print('fail去重1',column,key,'fail code:',error_code,e,type(df_add['temp']),type(df[column]))  
                            self.errorframe1=df_add['temp']
                            self.errorframe2=df[column] 
                            self.errorframe3=df
                            self.errorframe4=df_dict
                except Exception as e:
                    print('fail去重2',column,'fail code:',error_code,e)
                    # self.errorframe1=df_add['temp']
                    # self.errorframe2=df[column] 
                    self.errorframe3=df
                    self.errorframe4=df_dict
        except Exception as e:
            print('fail去重3','\nfail code:',error_code,e)
            # self.errorframe1=df_add['temp']
            # self.errorframe2=df[column] 
            self.errorframe3=df
            self.errorframe4=df_dict
        added_columns=[]
        for c in  df_dict:
            added_columns.append(c)
        self.added_columns=added_columns   
        self.df_dict=df_dict 
        self.df=df
        self.tag=tag
        t3=time.time()
        print('-----',time.strftime("%m-%d %H:%M:%S", time.localtime()),'实际运行时间',round((t3-t1)/60,2),'min-----\n')
    def tag_single_word(self,keyword,exclude,series_content,nearrule):       
        try:
            series_keyword_final=pd.Series('')
            series_tag=pd.Series([True]*len(series_content))
            series_exclude=pd.Series([True]*len(series_content))
            keyword=keyword.replace('+','(and)').replace('(AND)','(and)')
            keyword=keyword.replace('\(and)','\+')
            list_keyword=  keyword.split('(and)')
            
            for i in range(len(list_keyword)):
                for metacharacter in ['.',  '*','+','?','\\', '[', ']','^','$', '{', '}','(', ')',]:
                    list_keyword[i]=str(list_keyword[i]).replace(metacharacter,"\\"+ metacharacter)
            
            for seperated_keyword in list_keyword:
                if seperated_keyword=='':
                    continue
                series_temp=series_content.str.contains(str(seperated_keyword), regex=True) 
                if series_temp.any():
                    series_tag=series_tag&series_temp
                else:
                    series_tag=series_temp
                    break                    
            error_code='tsw1'
            if series_tag.any() and exclude!='' and exclude!='nan':
                error_code='tsw1.1'
                series_content=series_content.loc[series_tag==True]
                error_code='tsw1.2'
                exclude=exclude.replace('+','(and)').replace('(AND)','(and)')
                error_code='tsw1.3'
                exclude=exclude.replace('\(and)','\+')
                error_code='tsw1.4'
                list_exclude= exclude.split('(and)')
                for seperated_exclude in list_exclude:
                    if seperated_exclude=='':
                        continue
                    series_temp_exclude=series_content.str.contains(seperated_exclude, regex=True) 
                    series_exclude=series_exclude&series_temp_exclude
                series_tag=series_tag&(~series_exclude)
            error_code='tsw2'    
            try:
                nearrule=float(nearrule) 
                error_code='tsw2.0' 
            except  Exception as  e:
                # print(error_code,e,'err tsw 2.0 这里不影响',nearrule)
                nearrule=0      
                pass                      
            if series_tag.any() and nearrule!=0 and len(list_keyword)>0 :
                series_content=series_content.loc[series_tag==True]
                list_near_keyword=[]
                list_near=[]
                nearrule=int(nearrule)
                error_code='tsw2.1'
                for i in range(len(list_keyword)):
                    for j in range(len(list_keyword)):
                        if i!=j:
                            list_near.append([list_keyword[i],list_keyword[j]])
                error_code='tsw2.2'
                for list_near_kw in list_near: 
                        temp_list_near_kw=[]           
                        temp_list_near_kw.append('('+str(list_near_kw[0])+').{0,'+str(nearrule)+'}('+str(list_near_kw[1])+')')
                        temp_list_near_kw.append('('+str(list_near_kw[1])+').{0,'+str(nearrule)+'}('+str(list_near_kw[0])+')')
                        list_near_keyword.append(temp_list_near_kw)
                error_code='tsw2.3'        
                for list_near_keyword_single in list_near_keyword:        
                       series_temp_1=series_content.str.contains(list_near_keyword_single[0], regex=True) 
                       series_temp_2=series_content.str.contains(list_near_keyword_single[1], regex=True)
                       series_temp=series_temp_1|series_temp_2            
                       series_tag=series_tag&series_temp
                       if series_tag.any()==False:
                           break
            error_code='tsw3'           
            if series_tag.any():
                series_content=series_content.loc[series_tag==True]
                for j in range(len(list_keyword)):
                    seperated_keyword=list_keyword[j]
                    series_keyword=series_content.str.findall(seperated_keyword)
                    for index, value in series_keyword.items():
                        if value==[]:
                            series_keyword[index]=''
                        else:        
                            if len(list_keyword)>1:
                                series_keyword[index]='|'.join(list(set(series_keyword[index])))
                            else:
                                series_keyword[index]=','.join(list(set(series_keyword[index])))
                    if j==0:
                        series_keyword_final=series_keyword
                    else:
                        series_keyword_final=series_keyword_final+'+'+series_keyword
            else:
                series_keyword_final=pd.Series()
            series_tag=series_tag.loc[series_tag==True]
        except Exception as e:
            print('tag_single_word FAIL ','failcode:',error_code,e,'\n',keyword,exclude,nearrule,list_keyword,'\n')
            pass
        return series_tag,series_keyword_final
