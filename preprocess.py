class preprocess_df:#初步处理df
    def __init__(self,df,list_obj):#

        # print(time.strftime("%m-%d %H:%M:%S", time.localtime()),'开始预处理数据')
        df=df.reset_index(drop=True)
        list_obj_modified=[]

        for obj in list_obj:#合并列
            if obj in df.columns:
                list_obj_modified.append(obj)
        for obj in list_obj_modified:
            df[obj]=df[obj].fillna('').astype(str).str.lower()
        series_content=pd.Series(['']*len(df))#一定要指明len长度
        for obj in list_obj_modified:
            series_content=series_content+df[obj]
        # if 'keyword关键词' not in df.columns:
        #     df['keyword关键词']=''
        self.df=df
        self.series_content=series_content

