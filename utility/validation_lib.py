import json
import os
from pyspark.sql import SparkSession
from utility.general_utility import flatten, read_config, read_schema, fetch_transformation_query_path, fetch_file_path

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()


def count_check(source, target, out, row, validation):
    source_count = source.count()
    target_count = target.count()
    failed = source_count - target_count

    if source_count == target_count:
        print(f"source count {source_count} is EQUAL to target count {target_count}")
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count=source_count, no_of_target_record_count=target_count
                     , failed_count=failed, column=row['key_col_list'], status='PASS', source_type=row['source_type'],
                     target_type=row['target_type'], out=out)
    else:
        print(f"source count {source_count} is NOT EQUAL to target count {target_count}")
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count=source_count, no_of_target_record_count=target_count
                     , failed_count=failed, column=row['key_col_list'], status='FAIL', source_type=row['source_type'],
                     target_type=row['target_type'], out=out)


def duplicate(target, key_columns, out, row, validation):
    print('duplicate check started')
    key_columns = key_columns.split(',')
    target = target.groupBy(key_columns).count().filter('count>1')
    target.show()
    target_count = target.count()
    failed = target.count()
    print(failed)
    if failed > 0:
        print('DUPLICATES are Present')
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count='NOT APPLICABLE', no_of_target_record_count=target_count
                     , failed_count=failed, column=row['key_col_list'], status='FAIL', source_type='NOT APPLICABLE',
                     target_type=row['target_type'], out=out)
    else:
        print('no duplicates')
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count='NOT APPLICABLE', no_of_target_record_count=target_count
                     , failed_count=failed, column=row['key_col_list'], status='PASS', source_type='NOT APPLICABLE',
                     target_type=row['target_type'], out=out)


def uniqueness_check(target, unique_col_list, out, row, validation):
    print('*************  uniqueness_check check started  ***********************')
    unique_col_list = unique_col_list.split(',')
    for col in unique_col_list:
        target = target.groupBy(col).count().filter('count>1')
        target.show()
        target_count = target.count()
        failed = target.count()
        print(failed)
        if failed > 0:
            print(f'UNIQUENESS---DUPLICATES are Present instead of unique values in : {col}')
            write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                         , no_of_source_record_count='NOT APPLICABLE', no_of_target_record_count=target_count
                         , failed_count=failed, column=col, status='FAIL', source_type='NOT APPLICABLE',
                         target_type=row['target_type'], out=out)
        else:
            print('UNIQUENESS---no duplicates')
            write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                         , no_of_source_record_count='NOT APPLICABLE', no_of_target_record_count=target_count
                         , failed_count=failed, column=col, status='PASS', source_type='NOT APPLICABLE',
                         target_type=row['target_type'], out=out)


def records_present_only_in_source(source, target, key_col_list, row, out, validation):
    print('*************  records_present_only_in_source check started  ***********************')
    columns = key_col_list
    key_col_list = key_col_list.split(",")
    source = source.select(key_col_list).groupBy(key_col_list).count().withColumnRenamed("count", "sourcecount")
    target = target.select(key_col_list).groupBy(key_col_list).count().withColumnRenamed("count", "targetcount")
    count_compare = source.join(target, key_col_list, how='full')
    failed = count_compare.filter("targetcount is null").count()
    source_count = source.count()
    target_count = target.count()
    print('failed records',str(failed))
    if failed > 0:
        print("there are some records present in Source but missing in Target")
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count=source_count, no_of_target_record_count=target_count
                     , failed_count=failed, column=columns, status='FAIL', source_type=row['source_type'],
                     target_type=row['target_type'], out=out)
    else:
        print("All Source records present in Target")
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count=source_count, no_of_target_record_count=target_count
                     , failed_count=failed, column=columns, status='PASS', source_type=row['source_type'],
                     target_type=row['target_type'], out=out)

def records_present_only_target(source,target,key_col_list,row,out,validation):
    print('*************  records_present_only_in_target check started  ***********************')
    columns=key_col_list
    key_col_list=key_col_list.split(',')
    sourcetemp=source.select(key_col_list).groupBy(key_col_list).count().withColumnRenamed('count','source_count')
    targettemp=target.select(key_col_list).groupBy(key_col_list).count().withColumnRenamed('count','target_count')
    src_tgt_join=sourcetemp.join(targettemp,key_col_list,how='full')
    failed=src_tgt_join.filter("source_count is null").count()
    source_count = source.count()
    target_count = target.count()
    print('failed records', str(failed))
    if failed>0:
        print("All Source records present in Target")
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count=source_count, no_of_target_record_count=target_count
                     , failed_count=failed, column=columns, status='FAIL', source_type=row['source_type'],
                     target_type=row['target_type'], out=out)
    else:
        write_output(validation_type=validation, source_name=row['source'], target_name=row['target']
                     , no_of_source_record_count=source_count, no_of_target_record_count=target_count
                     , failed_count=failed, column=columns, status='PASS', source_type=row['source_type'],
                     target_type=row['target_type'], out=out)









def write_output(validation_type, source_name, target_name, no_of_source_record_count, no_of_target_record_count
                 , failed_count, column, status, source_type, target_type, out):
    out["validation_type"].append(validation_type)
    out["source_name"].append(source_name)
    out["target_name"].append(target_name)
    out["no_of_source_record_count"].append(no_of_source_record_count)
    out["no_of_target_record_count"].append(no_of_target_record_count)
    out["failed_count"].append(failed_count)
    out["column"].append(column)
    out["status"].append(status)
    out["source_type"].append(source_type)
    out["target_type"].append(target_type)
