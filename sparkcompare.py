'''
Created on May 28, 2019

@author: n767675
'''

from __future__ import print_function
from pyspark.sql import functions as F
from functools import reduce
import six
import pandas as pd

class SparkCompare(object):
    '''
    classdocs
    '''
    

    def __init__(
        self, 
        spark_session,
        base_df,
        compare_df,
        join_columns,
        column_mapping=None,
        ignore_columns=None,
        cache_intermediates=False
    ):
        
        '''
        Constructor
        '''
        self._original_base_df = base_df
        self._original_compare_df = compare_df
        self._join_columns = self._tuplizer(join_columns)
        self._join_column_names = [name[0] for name in self._join_columns]
        
        #rename compare df columns with base df column names
        if column_mapping:
            for mapping in column_mapping:
                compare_df = compare_df.withColumnRenamed(mapping[1], mapping[0])
            self._column_mapping = dict(column_mapping)
        else:
            self._column_mapping = {}
               
        if ignore_columns:
            self._ignore_columns = self._tuplizer(ignore_columns)
            to_drop_base = [mapping[0] for mapping in self._ignore_columns]
            to_drop_compare = [mapping[1] for mapping in self._ignore_columns]
            base_df = base_df.drop(*to_drop_base)
            compare_df = compare_df.drop(*to_drop_compare)
        else:
            self._ignore_columns = []
        
        #rename compare df join columns with base df join column names
        for mapping in self._join_columns:
            if mapping[1] != mapping[0]:
                compare_df = compare_df.withColumnRenamed(mapping[1], mapping[0])
        
        self._columns_to_compare = set(base_df.columns) & set(compare_df.columns)
        
        #Only select columns in column mapping or the ones that have common names
        self.base_df = base_df.select(sorted(self._columns_to_compare))
        self.compare_df = compare_df.select(sorted(self._columns_to_compare))
               
        self.spark = spark_session
        self._joined_dataframe = None
        
        # drop the duplicates before actual comparison made.
        self.base_df = base_df.dropDuplicates(self._join_column_names)
        self.compare_df = compare_df.dropDuplicates(self._join_column_names)
        
        self._cache_intermediates = cache_intermediates
        
        if cache_intermediates:
            self.base_df.cache()            
            self.compare_df.cache()
        
        self._base_row_count = self.base_df.count()
        self._compare_row_count = self.compare_df.count()
        
        if self._base_row_count == 0 or self._compare_row_count == 0:
            if self._base_row_count == 0 and self._compare_row_count == 0:
                raise Exception('Both your dataframes have count 0!!')
            elif self._base_row_count == 0:
                raise Exception('Base dataframe has count 0!!')
            else:
                raise Exception('Compare dataframe has count 0!!')
        
    
    def _tuplizer(self, input_list):
        tupled_list = []
        for val in input_list or []:
            if isinstance(val, six.string_types):
                tupled_list.append((val, val))
            else:
                tupled_list.append(val)

        return tupled_list

    
    @property
    def get_base_only_rows(self):
        """Get rows from base dataframe that are not there in compare dataframe"""
        return self.base_df.join(self.compare_df, self._join_column_names, how='left_anti')
    
    
    @property
    def get_compare_only_rows(self):
        """Get rows from base dataframe that are not there in compare dataframe"""
        return self.compare_df.join(self.base_df, self._join_column_names, how='left_anti')
    
    def _get_mismatch_base_rows(self):
        """Get the rows only from base data frame, includes both mismatch and new"""
        return self.base_df.join(self.compare_df, ['hash'] + self._join_column_names, how='left_anti')


    def _get_mismatch_compare_rows(self):
        """Get the rows only from compare data frame, includes both mismatch and new"""
        return self.compare_df.join(self.base_df, ['hash'] + self._join_column_names, how='left_anti')
    
    
    def _rename_columns_with_suffix(self, df, suffix):
        oldColumns = df.columns
        newColumns = [name+'_'+suffix for name in oldColumns]
        return reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)
         
    
    def _calculate_base_hash(self):
        self.base_df = self.base_df.select('*', F.sha2(F.concat_ws('', *self.base_df.columns), 256).alias('hash'))
        
    
    def _calculate_compare_hash(self):
        self.compare_df = self.compare_df.select('*', F.sha2(F.concat_ws('', *self.compare_df.columns), 256).alias('hash'))
    
    
    @property
    def joined_dataframe(self):
        """Will be None if called before calling report"""
        return self._joined_dataframe
    
    
    def report(self, path="report.xlsx"):
        """Generate a text report of differences"""
        #creating the hash key on concatenated value of all the columns
        self._calculate_base_hash()
        self._calculate_compare_hash()
        
        base_only_rows = self._get_mismatch_base_rows()
        compare_only_rows = self._get_mismatch_compare_rows()
        
        base_only_rows = base_only_rows.drop('hash')
        compare_only_rows = compare_only_rows.drop('hash')
        
        if self._cache_intermediates:
            base_only_rows.cache()
            compare_only_rows.cache()
        
        self._base_only_rows_count = base_only_rows.count()
        self._compare_only_rows_count = compare_only_rows.count()
        
        if self._base_only_rows_count == 0:
            print("Both the dataframes are exactly equal")
            return
        
        base_alias = 'base'
        compare_alias = 'compare'
        
        renamed_base, renamed_compare = self._rename_columns_with_suffix(base_only_rows, base_alias), self._rename_columns_with_suffix(compare_only_rows, compare_alias)
        
        join_conditions = [renamed_base[key+'_'+base_alias] == renamed_compare[key+'_'+compare_alias] for key in self._join_column_names]
        
        self._joined_dataframe = renamed_base.join(renamed_compare, join_conditions, how='inner')
        
        if self._cache_intermediates:
            self._joined_dataframe.cache()
        
        differences_df = self._joined_dataframe.collect()
        
        report_cols = self._join_column_names + ['base df column', 'base df value', 'compare df column', 'compare df value']
        
        report_list=[]

        base_keys = sorted(renamed_base.columns)
        compare_keys = sorted(renamed_compare.columns)
        
        #Comparing column values
        for row in differences_df:
            for key_base, key_compare in zip(base_keys, compare_keys):
                if row[key_base] != row[key_compare]:
                    L2 = [row[key+'_'+base_alias] for key in self._join_column_names]
                    L2.append(key_base.replace('_'+base_alias,''))
                    L2.append(row[key_base])
                    L2.append(self._column_mapping[key_base.replace('_'+base_alias,'')] if key_base.replace('_'+base_alias,'') in self._column_mapping else key_compare.replace('_'+compare_alias,''))
                    L2.append(row[key_compare])
                    report_list.append(L2)

        #Report Generation 
        report_df=pd.DataFrame(report_list, columns=report_cols)
        report_df.to_excel(path, index=False)
