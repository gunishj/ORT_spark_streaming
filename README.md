# spark_streaming

# ORT Design Document
 Structure and Workflow
Input Kafka Stream: ORG_HIER_STREAM
{ "emp": "E3000", "grade": "G8", "effective_dt": "10-Jan-2019", "suprvsr": "E1000", "status":"SUPERVISOR_CHANGE", "posted_dt": "10-Jan-2019" }

We have a consumer process Streaming application in spark which is subscribing to kafka stream and loading the data into our Enterprise DWH.
 	ORG_HIER_TBL: The RPT_END_DT and SUPRVSR_HIER  are generated generated through our streaming application dynamically.



In our consumer process we are subscribing to the Kafka stream and pulling the supervisor hierarchy from our main table in case of supervisor change, role change and new entries, appending his hierarchy with his id in hierarchy column.
The workflow updates the previous entries report end date for existing entries with the day prior to the new start date.
Resigned cases will have report end date same report start date indicating their termination from the role.
BRD (Bi-Weekly reporting Dashboard)
We have scheduled an automatic refresh triggers scheduled for our spark jobs that will prepare the BI report for our end mangers and business leader so as to make effective decisions for laying out the policy guidelines for our workforce.

A.	Report A: Rank each Supervisor (of same Grades) based on number of subordinates reporting. If the number of subordinates is same then rank them based on employee id giving preference to employee who has joined organization earlier.
"""(SELECT EMP, GRADE, COUNT, ROW_NUMBER() OVER (PARTITION BY GRADE ORDER BY COUNT ,RPT_ST_DT ) AS RANK FROM
                       |(
                       |SELECT A.EMP, A.GRADE, COUNT(B.EMP) AS COUNT,A.RPT_ST_DT
                       |FROM ORG_HIER_TBL A
                       |INNER JOIN (SELECT EMP,SUPRVSR FROM ORG_HIER_TBL C WHERE RPT_END_DT ='9999-12-31' GROUP BY EMP,SUPRVSR ) B ON A.EMP = B.SUPRVSR
                       |INNER JOIN (SELECT MIN(RPT_ST_DT),EMP FROM ORG_HIER_TBL GROUP BY EMP) C ON A.EMP = C.EMP
                       |GROUP BY A.EMP, A.GRADE,A.RPT_ST_DT
                       |) AS D ORDER BY GRADE, RANK) as A_query """.
                       
B.	Report B: Find average number of direct subordinates and total (direct + indirect) subordinates for each grade.
"""(WITH RECURSIVE circular_managers(emp, SUPRVSR, depth, path, cycle) AS (
                       | SELECT u.emp, u.SUPRVSR, 1,
                       |  ARRAY[u.emp],
                       |  false
                       |  FROM (select * FROM ORG_HIER_TBL where rpt_End_dt = '9999-12-31') u
                       | UNION ALL
                       | SELECT u.emp, u.SUPRVSR, cm.depth + 1,
                       |  path || u.emp,
                       |  u.emp = ANY(path)
                       | FROM (select * FROM ORG_HIER_TBL where rpt_End_dt = '9999-12-31') u, circular_managers cm
                       | WHERE u.emp = cm.SUPRVSR AND NOT cycle
                       | )
                       |select grd.grade , round(avg(direct_reportees),1) as Avg_Dir_Reportees,round(avg(total_reportees),1) as Avg_Total_Reportees FROM
                       |(select emp ,sum(case when depth =2 then 1 else 0 end) direct_reportees,count(*) as total_reportees
                       |from circular_managers where depth !=1 group by emp) calc
                       |inner join
                       |(select grade,emp from ORG_HIER_TBL where rpt_End_dt = '9999-12-31') grd  on calc.emp = grd.emp
                       |group by grade) as B_QUERY """.


C.	Report C: List of Eligible Supervisors (G10 and above only) who were not assigned any subordinates under them. Order the list based on Grade starting from Grade 10 and so on. 
"""(SELECT A.EMP, A.GRADE FROM ORG_HIER_TBL A LEFT OUTER JOIN (SELECT DISTINCT SUPRVSR FROM ORG_HIER_TBL A) B
                       | ON A.EMP = B.SUPRVSR
                       | WHERE A.GRADE IN ('G10','G11','G12','G13','G14','G15','G16','G17','G18') AND
                       | B.SUPRVSR IS NULL  ORDER BY A.GRADE ) as C_QUERY """.
                       
D.	Report D: Generate a report which can give insight on number of employees that have resigned and joined at different grades in the past 30 days. 
""" (SELECT A.GRADE, sum(CASE WHEN MAX_BLOCK.EMP IS NOT NULL THEN 1 ELSE 0 END) resigned,sum(CASE WHEN MIN_BLOCK.EMP IS NOT NULL THEN 1 ELSE 0 END) JOINED FROM ORG_HIER_TBL A LEFT OUTER JOIN
                       | ( SELECT * FROM (SELECT EMP ,MIN(RPT_ST_DT) AS MIN_DT  FROM ORG_HIER_TBL GROUP BY EMP) MIN1
                       |WHERE MIN_DT >= NOW() - '30 DAYS'::INTERVAL
                       | ) MIN_BLOCK
                       | ON
                       | A.EMP=MIN_BLOCK.EMP AND A.RPT_ST_DT = MIN_BLOCK.MIN_DT
                       | LEFT OUTER JOIN
                       | ( SELECT * FROM (SELECT EMP ,MAX(RPT_END_DT) AS MAX_DT FROM ORG_HIER_TBL GROUP BY EMP) MAX1
                       |WHERE MAX_DT >= NOW() - '30 DAYS'::INTERVAL  AND MAX_DT <= NOW()
                       | ) MAX_BLOCK
                       |ON
                       | A.EMP=MAX_BLOCK.EMP AND A.RPT_END_DT = MAX_BLOCK.MAX_DT
                       | GROUP BY A.GRADE ) as D_QUERY """.

Sample data used to instantiate db tables and  kafka stream is as given below:
{ "emp": "E1000", "grade": "G8", "effective_dt": "2020-10-03", "suprvsr": "E100", "status":"SUPERVISOR_CHANGE", "posted_dt": "2020-10-03" }
{ "emp": "E3", "grade": "G18", "effective_dt": "2020-10-03", "suprvsr": "E1", "status":"NEW", "posted_dt": "2020-10-03" }
{ "emp": "E7", "grade": "G16", "effective_dt": "2020-10-03", "suprvsr": "E1", "status":"NEW", "posted_dt": "2020-10-03" }
{ "emp": "E9", "grade": "G16", "effective_dt": "2020-10-03", "suprvsr": "E1", "status":"NEW", "posted_dt": "2020-10-03" }
{ "emp": "E500", "grade": "G10", "effective_dt": "2020-10-03", "suprvsr": "E100", "status":"RESIGNED", "posted_dt": "2020-10-03" }


Truncate table ORG_HIER_TBL;
Truncate table ORG_HIER_STRM_TBL;
insert into ORG_HIER_TBL values ('E1','G18','2020-10-03','9999-12-31','','');
insert into ORG_HIER_TBL values ('E10','G16','2020-10-03','9999-12-31','E1','E1');
insert into ORG_HIER_TBL values ('E100','G16','2020-10-03','9999-12-31','E10','E10-E1');

 


```
Reports generated from code :
+----+-----+-----+----+
| emp|grade|count|rank|
+----+-----+-----+----+
| E10|  G16|    1|   1|
|E100|  G16|    1|   2|
|  E1|  G18|    4|   1|
+----+-----+-----+----+

+-----+--------------------+--------------------+
|grade|   avg_dir_reportees| avg_total_reportees|
+-----+--------------------+--------------------+
|  G16|1.000000000000000000|1.500000000000000000|
|  G18|4.000000000000000000|6.000000000000000000|
+-----+--------------------+--------------------+

+----+-----+
| emp|grade|
+----+-----+
|E500|  G10|
|  E7|  G16|
|  E9|  G16|
|  E3|  G18|
+----+-----+

+-----+--------+------+
|grade|resigned|joined|
+-----+--------+------+
|  G16|       0|     4|
|   G8|       0|     1|
|  G18|       0|     2|
|  G10|       1|     1|
+-----+--------+------+
```


