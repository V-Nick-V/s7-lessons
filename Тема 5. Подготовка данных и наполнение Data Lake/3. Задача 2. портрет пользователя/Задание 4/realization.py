def compare_df(df1, df2,):
    return (df1.schema == df2.schema) and (df1.collect() == df2.collect())
