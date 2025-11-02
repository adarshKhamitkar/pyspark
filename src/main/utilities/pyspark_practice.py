import pyspark
from pyspark.sql import SparkSession

employees = [['id', 'emp_name', 'dept_id', 'salary'],
             [1, 'John Doe', 1, 50000],
             [2, 'Jane Smith', 2, 60000],
             [3, 'Jim Beam', 1, 55000],
             [4, 'Jill Johnson', 2, 65000],
             [5, 'Jack Doe', 1, 52000],
             [6, 'Jackson', 2, 62000],
             [7, 'Jim Beamson', 3, 57000],
             [8, 'Jill Johndottr', 3, 67000],
             [9, 'Jack Stagg', 2, 54000],
             [10, 'Jill Smithson', 3, 64000]]

departments = [['department_id', 'dept_name'],
              [1, 'HR'],
              [2, 'IT'],
              [3, 'Finance'],
              [4, 'Marketing'],
              [5, 'Sales'],
              [6, 'Engineering'],
              [7, 'Customer Service'],
              [8, 'Legal']]


emp_headers = employees[0]
dept_headers = departments[0]

emps_dict, depts_dict = [], [] 

for emp_row in employees[1:]:
    emp_dict = {emp_headers[i]: emp_row[i] for i in range(len(emp_row))}
    emps_dict.append(emp_dict)

for dept_row in departments[1:]:
    dept_dict = {dept_headers[i]: dept_row[i] for i in range(len(dept_row))}
    depts_dict.append(dept_dict)

res = []
for emp_dict in emps_dict:
    for dept_dict in depts_dict:
        if emp_dict['dept_id'] == dept_dict['department_id']:
            merged = emp_dict.copy()

            for k,v in dept_dict.items():
                merged[k] = v

    res.append(merged)

#print(res)

#Time complexity: O(m*n)
#Space complexity: O(n)

opt_res = []
dept_map = {row['department_id']: row for row in depts_dict}

for emp_dict in emps_dict:
    emp_dept_id = emp_dict['dept_id']

    if emp_dept_id in dept_map:
        merged = emp_dict.copy()

        for k,v in dept_map[emp_dept_id].items():
            merged[k] = v

    opt_res.append(merged)

#print(opt_res)

#Time complexity: O(m+n)
#Space complexity: O(n)

#Spark implementation
spark = SparkSession.builder.appName("Joins").master('local[*]').getOrCreate()

empsDF = spark.createDataFrame(employees[1:],employees[0])
deptsDF = spark.createDataFrame(departments[1:],employees[0])

empsDF.show()
deptsDF.show()