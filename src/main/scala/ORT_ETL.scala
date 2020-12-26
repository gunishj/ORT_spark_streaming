import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
object ORT_ETL {

  val spark = SparkSession.builder()
    .appName("Spark SQL etl")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/docker"
  val user = "docker"
  val password = "docker"


  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def WriteTable(dataFrame: DataFrame,saveMode: SaveMode, tableName: String) = spark.read
    .format("jdbc")
    .option("driver",driver)
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable", s"public.$tableName")



  def load_tables() ={
    readTable("org_hier_strm_tbl").createOrReplaceTempView("org_hier_strm_tbl")
    readTable("org_hier_tbl").createOrReplaceTempView("org_hier_tbl")
  }


  import java.util.Properties
  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", "org.postgresql.Driver")
  connectionProperties.put("user", user)
  connectionProperties.put("password", password)


  def ETL_main()={
    val query1 = """(SELECT A.EMP ,A.GRADE ,A.RPT_ST_DT ,COALESCE(B.EFFECTIVE_DT - '1 Days'::INTERVAL, A.RPT_END_DT) AS RPT_END_DT
                   |,A.SUPRVSR ,A.SUPRVSR_HIER FROM ORG_HIER_TBL A LEFT OUTER JOIN
                   |( SELECT * FROM ( SELECT EMP ,GRADE ,EFFECTIVE_DT ,POSTED_DT ,SUPRVSR ,STATUS ,ROW_NUMBER()
                   |OVER (  partition by EMP ORDER BY POSTED_DT DESC ) ROW_NUM
                   |FROM ORG_HIER_STRM_TBL WHERE POSTED_DT <= NOW() - '1 Days'::INTERVAL  and POSTED_DT >= NOW() -  '15 Days'::INTERVAL ) A
                   | WHERE ROW_NUM = 1 ) B ON A.EMP = B.EMP
                   | UNION
                   | SELECT A.EMP ,A.GRADE ,A.EFFECTIVE_DT AS RPT_ST_DT ,
                   | CASE WHEN UPPER(A.STATUS) = 'RESIGNED' THEN A.EFFECTIVE_DT ELSE '9999-12-31' END AS RPT_END_DT ,
                   | A.SUPRVSR ,RTRIM( CONCAT ( A.SUPRVSR,'-' ,COALESCE(B.SUPRVSR_HIER, '') ) , '-' ) AS SUPRVSR_HIER FROM
                   | ORG_HIER_STRM_TBL A LEFT OUTER JOIN ( SELECT * FROM (SELECT EMP  , GRADE  , RPT_ST_DT , RPT_END_DT , SUPRVSR  ,
                   | SUPRVSR_HIER  ,ROW_NUMBER() OVER ( partition by EMP ORDER BY RPT_END_DT DESC ) ROW_NUM   FROM ORG_HIER_TBL ) B
                   | WHERE ROW_NUM = 1 ) B ON A.SUPRVSR = B.EMP WHERE A.POSTED_DT <= NOW() - '1 Days'::INTERVAL  and
                   | A.POSTED_DT >= NOW() -  '15 Days'::INTERVAL ) as query""".stripMargin
    val query1DF = spark.read.jdbc(url, query1, connectionProperties)

    query1DF.write
      .format("jdbc")
      .option("driver",driver)
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable","public.org_hier_tbl_bkp")
      .mode(SaveMode.Overwrite)
      .save()

    spark.read.jdbc(url, "(select * from public.org_hier_tbl_bkp) as query2", connectionProperties).write
      .format("jdbc")
      .option("driver",driver)
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable","public.org_hier_tbl")
      .mode(SaveMode.Overwrite)
      .save()
    //query1DF.show()

  }
  def ReportGenerator()={

    val Report_A_query = """(SELECT EMP, GRADE, COUNT, ROW_NUMBER() OVER (PARTITION BY GRADE ORDER BY COUNT ,RPT_ST_DT ) AS RANK FROM
                           |(
                           |SELECT A.EMP, A.GRADE, COUNT(B.EMP) AS COUNT,A.RPT_ST_DT
                           |FROM ORG_HIER_TBL A
                           |INNER JOIN (SELECT EMP,SUPRVSR FROM ORG_HIER_TBL C WHERE RPT_END_DT ='9999-12-31' GROUP BY EMP,SUPRVSR ) B ON A.EMP = B.SUPRVSR
                           |INNER JOIN (SELECT MIN(RPT_ST_DT),EMP FROM ORG_HIER_TBL GROUP BY EMP) C ON A.EMP = C.EMP
                           |GROUP BY A.EMP, A.GRADE,A.RPT_ST_DT
                           |) AS D ORDER BY GRADE, RANK) as A_query """.stripMargin
    val Report_B_query = """(WITH RECURSIVE circular_managers(emp, SUPRVSR, depth, path, cycle) AS (
                           |	SELECT u.emp, u.SUPRVSR, 1,
                           |		ARRAY[u.emp],
                           |		false
                           |	 FROM (select * FROM ORG_HIER_TBL where rpt_End_dt = '9999-12-31') u
                           |	UNION ALL
                           |	SELECT u.emp, u.SUPRVSR, cm.depth + 1,
                           |		path || u.emp,
                           |		u.emp = ANY(path)
                           |	FROM (select * FROM ORG_HIER_TBL where rpt_End_dt = '9999-12-31') u, circular_managers cm
                           |	WHERE u.emp = cm.SUPRVSR AND NOT cycle
                           |	)
                           |select grd.grade , round(avg(direct_reportees),1) as Avg_Dir_Reportees,round(avg(total_reportees),1) as Avg_Total_Reportees FROM
                           |(select emp ,sum(case when depth =2 then 1 else 0 end) direct_reportees,count(*) as total_reportees
                           |from circular_managers where depth !=1 group by emp) calc
                           |inner join
                           |(select grade,emp from ORG_HIER_TBL where rpt_End_dt = '9999-12-31') grd  on calc.emp = grd.emp
                           |group by grade) as B_QUERY """.stripMargin

    val Report_C_query = """(SELECT A.EMP, A.GRADE FROM ORG_HIER_TBL A LEFT OUTER JOIN (SELECT DISTINCT SUPRVSR FROM ORG_HIER_TBL A) B
                           | ON A.EMP = B.SUPRVSR
                           | WHERE A.GRADE IN ('G10','G11','G12','G13','G14','G15','G16','G17','G18') AND
                           | B.SUPRVSR IS NULL  ORDER BY A.GRADE ) as C_QUERY """.stripMargin

    val Report_D_query = """ (SELECT A.GRADE, sum(CASE WHEN MAX_BLOCK.EMP IS NOT NULL THEN 1 ELSE 0 END) resigned,sum(CASE WHEN MIN_BLOCK.EMP IS NOT NULL THEN 1 ELSE 0 END) JOINED FROM ORG_HIER_TBL A LEFT OUTER JOIN
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
                           | GROUP BY A.GRADE ) as D_QUERY """.stripMargin

    val REPORT_A_DF = spark.read.jdbc(url, Report_A_query, connectionProperties)
    val REPORT_B_DF = spark.read.jdbc(url, Report_B_query, connectionProperties)
    val REPORT_C_DF = spark.read.jdbc(url, Report_C_query, connectionProperties)
    val REPORT_D_DF = spark.read.jdbc(url, Report_D_query, connectionProperties)

    REPORT_A_DF.show()
    REPORT_B_DF.show()
    REPORT_C_DF.show()
    REPORT_D_DF.show()

  }



  def main(args: Array[String]): Unit = {
    ETL_main()
    ReportGenerator()
  }
}

