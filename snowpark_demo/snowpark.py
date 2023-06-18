from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, FloatType
from snowflake.snowpark.functions import col
#import pandas as pd
snowfalake_con_params={
    'account':'fxquddr-dl44953',
    'user':'sai',
    'password':'Sairam@8662',
    'database':'CX_DB',
    'schema':'student',
    'warehouse':'COMPUTE_WH'
}

class snowpark_etl:

    def initiate_data_ingestion(self,file_path,snowflake_stagename):
            #creating snowpark session
            global snowpark_df,snowpark_session
            
            snowpark_session=Session.builder.configs(snowfalake_con_params).create()
            print('snowpark session created')
            snowpark_session.file.put(file_path,snowflake_stagename, auto_compress=False,overwrite=True)
            print('uploaded data file into snowflake stage {} {}'.format(' ',snowflake_stagename ))
            #declaring schema for the dataframe
            schema=StructType([StructField("id", IntegerType()), StructField("Gender", StringType()), StructField("race_ethnicity", StringType()),StructField("parental_level_of_education", StringType()),
                            StructField("lunch",StringType()),StructField("test_preparation_course", StringType()),StructField("math_score", IntegerType()),
                            StructField("reading_score", IntegerType()),StructField("writing_score", IntegerType())])
        
            #creating dataframe using csv file from snowflake stage 
            

            snowpark_df=snowpark_session.read.option("pattern", ".*[.]csv").schema(schema).csv(snowflake_stagename)
            
            #pass data file name and snowflake stage name while calling function
        
    def data_transformation(self):
        global df
        #adding eextra column to dataframe
        print('data transformation started')
        print('adding new column to the existing dataframe')
        df=snowpark_df.with_column('total_score',(snowpark_df['math_score']+snowpark_df['reading_score']+snowpark_df['writing_score']))
        
    def saving_as_table(self):
          '''
          snowpark write method has 3 modes
          1. append= creats new table 
          2. overwrite=is used to replace the contents of an existing table with the new data
          3. ignore=mode does not perform any operation if the table already exists,If the table does not exist, it creates a new table with the given name and adds the data to it.
          '''
          print('writing dataframe to snowflake table')
          df_result=df.count()
          if df_result==0:
                print('data ingestion into table is unsuccessfull')
          else:
                df.write.mode("ignore").save_as_table("student_performace")
                print('data ingestion into table is successfull')
    def data_extraction(self):
         
         print('data extraction started')
         poor_performance=snowpark_session.table('student_performace').filter((col('total_score')<150)).sort(col('total_score').asc())
         good_performance=snowpark_session.table('student_performace').filter((col('total_score')>=200)).sort(col('total_score').asc())

         print('converting snowflake dataframe to pandas dataframe')
         poor_performance_df=poor_performance.to_pandas()
         good_performance_df=good_performance.to_pandas()

         print('saving dataframe to csv file')
         poor_performance_df.to_csv('data\poor_performance.csv')
         good_performance_df.to_csv('data\good_performance.csv')

         #view creation
         print('creating view with snowpark dataframe')
         good_performance.create_or_replace_view('good_performace')
         
            

if __name__=="__main__":
      obj=snowpark_etl()
      obj.initiate_data_ingestion(file_path='data\student.csv',snowflake_stagename='@cx')
      obj.data_transformation()
      obj.saving_as_table()
      obj.data_extraction()
    




