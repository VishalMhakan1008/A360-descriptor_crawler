import dask.dataframe as dd
from dask import delayed

df_1 = dd.read_csv(r'E:\\dbo\\alco_notice_addressing\\DBO_ALCO_NOTICE_ADDRESSING-0-00000001.csv',
                   usecols=['NOTICE_DEFINITION_ID'])
df_2 = dd.read_csv(r'E:\\dbo\\alco_notice_trigger\\DBO_ALCO_NOTICE_TRIGGER-0-00000001.csv',
                   usecols=['NOTICE_DEFINITION_ID'])

df_1 = df_1.drop_duplicates()
df_2 = df_2.drop_duplicates()

# Partitioning
chunk_size = '1GB'
df_1 = df_1.repartition(partition_size=chunk_size)
df_2 = df_2.repartition(partition_size=chunk_size)


@delayed
def count_matching_rows(chunk_1, chunk_2):
    matching_rows = len(chunk_1.merge(chunk_2, how='inner'))
    return matching_rows


matching_counts = []

for part_1, part_2 in zip(df_1.to_delayed(), df_2.to_delayed()):
    matching_counts.append(count_matching_rows(part_1, part_2))

total_matching_count = delayed(sum)(matching_counts)

result = total_matching_count.compute()

print("Overall Matching Count:", result)
