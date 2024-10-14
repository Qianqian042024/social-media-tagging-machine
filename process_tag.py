class process_tag:
    def __init__(self,tag):

        print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'开始预处理标签')
        tag=tag.reset_index(drop=True)
        tag_modified=pd.DataFrame()#生成一个空的标签df
        error_code='process_tag1'
        for c in tag.columns:
            tag=tag.rename(columns={c:str(c).lower()})
            error_code='process_tag2'
        for c in tag.columns:
            tag[c]=tag[c].replace('\n','')
            error_code='process_tag3'
        tag['nearrule']=tag['nearrule'].fillna(0)
        tag.fillna('',inplace=True)
        for col in tag.columns:#tag转小写
            error_code='process_tag4'
            if col=='nearrule':
                continue
            try:tag[col]=tag[col].str.lower()
            except:pass
        
        tag_modified=pd.DataFrame()#生成一个空的标签df
        if ('字段名' in tag.columns and '标签名' in tag.columns and '关键词' in tag.columns and '排除词' in tag.columns and 'nearrule' in tag.columns) or ('一级标签' in tag.columns and '二级标签' in tag.columns and '三级标签' in tag.columns and 'kw' in tag.columns and 'exclude' in tag.columns and 'nearrule' in tag.columns):        
            if '一级标签' in tag.columns and '二级标签' in tag.columns and '三级标签' in tag.columns and 'kw' in tag.columns and 'exclude' in tag.columns and 'nearrule' in tag.columns:
                for i in range(len(tag)):
                    error_code='process_tag4'
                    if tag['二级标签'][i]=='':
                        temptag=pd.DataFrame()
                        temptag.at[0,'字段名']=tag['一级标签'][i]
                        temptag.at[0,'标签名']=tag['三级标签'][i]
                        temptag.at[0,'关键词']=tag['kw'][i]
                        temptag.at[0,'排除词']=tag['exclude'][i]
                        temptag.at[0,'nearrule']=tag['nearrule'][i]
                        tag_modified=tag_modified.append(temptag,ignore_index=True)
                    if tag['二级标签'][i]!='':
                        error_code='process_tag5'
                        temptag=pd.DataFrame()
                        temptag.at[0,'字段名']=tag['一级标签'][i]+'-'+tag['二级标签'][i]
                        temptag.at[0,'标签名']=tag['三级标签'][i]
                        temptag.at[0,'关键词']=tag['kw'][i]
                        temptag.at[0,'排除词']=tag['exclude'][i]
                        temptag.at[0,'nearrule']=tag['nearrule'][i]
                        tag_modified=tag_modified.append(temptag,ignore_index=True)
                        temptag=pd.DataFrame()
                        temptag.at[0,'字段名']=tag['一级标签'][i]
                        temptag.at[0,'标签名']=tag['二级标签'][i]
                        temptag.at[0,'关键词']=tag['kw'][i]
                        temptag.at[0,'排除词']=tag['exclude'][i]
                        temptag.at[0,'nearrule']=tag['nearrule'][i]
                        tag_modified=tag_modified.append(temptag,ignore_index=True)
                tag=tag_modified
            for i in range(len(tag)):
               error_code='process_tag6'
               if tag['关键词'][i]=='':
                    tag.at[i,'关键词']=tag['标签名'][i]
               if tag['标签名'][i]=='':
                   tag.at[i,'标签名']=tag['关键词'][i]
               kw= tag['关键词'][i]
               tag.at[i,'关键词']=kw.replace('||','|').replace('|+','+').replace('+|','+').replace('++','+').replace('| ','|').replace(' |','|')
               try:
                   error_code='process_tag7'
                   if kw[len(kw)-1]=='|':
                       tag.at[i,'关键词']=kw[:len(kw)-1]
               except Exception as e:
                    print(error_code,e,'关键词:','[',tag['关键词'][i],']')
                    
                    pass
            error_code='process_tag8'
            tag.drop_duplicates(tag.columns.to_list(),keep='first',inplace=True)#
            tag.sort_values(by=['关键词'],inplace=True)
            error_code='process_tag9'
            tag=tag.reset_index(drop=True)
            for index, row in tag.iterrows():
                error_code='process_tag10'
                for c in tag.columns:
                    tag.at[index,c]=str(row[c]).replace('\n','')
        else:
           
            print(error_code,'标签不符合规范')
        self.tag=tag
    



