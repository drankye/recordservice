// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.examples.spark

import com.cloudera.recordservice.mr.RecordServiceConfig
import com.cloudera.recordservice.spark.RecordServiceConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

// Driver to run tpcds queries.
// TODO: it appears that there is no way to pass arbitrary confs via --conf, you can
// only pass key that spark knows about. Fix this.
object TpcdsBenchmark {
  def printUsage(): Unit = {
    System.err.println(
        "Usage:\n" +
        " spark-submit --class com.cloudera.recordservice.examples.spark.TpcdsBenchmark\n" +
        "              -- master <spark master>\n" +
        "              recordservice-spark-<version>.jar\n" +
        "              <RecordServicePlanner host> [Query] [Database]")
  }

  // Registers the tpcds tables in sqlContext using the RecordServiceRelation. This
  // creates each of the tpcds tables with their standard names from db.
  // that is, in sqlContext, there will be a "store_sales" table that is mapped to
  // db."store_sales"
  def registerRecordServiceTable(ctx: SQLContext, db:String, tbl:String, size:Long) = {
    ctx.sql(s"""
         |CREATE TEMPORARY TABLE $tbl
         |USING com.cloudera.recordservice.spark.DefaultSource
         |OPTIONS (
         |  RecordServiceTable '$db.$tbl',
         |  RecordServiceTableSize '$size'
         |)
       """.stripMargin)
  }

  // Set table sizes as the HDFS file size. This is what the builtin parquet relation
  // does.
  // TODO: Add record service API to get this.
  def registerRecordServiceTables(sqlContext: SQLContext, db:String) = {
    System.out.println("Registering record service tables from db: " + db)
    registerRecordServiceTable(sqlContext, db, "customer", 249930460L)
    registerRecordServiceTable(sqlContext, db, "customer_address", 44742838L)
    registerRecordServiceTable(sqlContext, db, "customer_demographics", 7906084L)
    registerRecordServiceTable(sqlContext, db, "date_dim", 2528914L)
    registerRecordServiceTable(sqlContext, db, "household_demographics", 42101L)
    registerRecordServiceTable(sqlContext, db, "item", 3291742L)
    registerRecordServiceTable(sqlContext, db, "promotion", 25460L)
    registerRecordServiceTable(sqlContext, db, "store", 15482L)
    registerRecordServiceTable(sqlContext, db, "store_sales", 198584111372L)
    registerRecordServiceTable(sqlContext, db, "time_dim", 1521558L)
  }

  def registerSparkSQLTable(sqlContext: SQLContext, path:String, tbl:String) = {
    sqlContext.sql(s"""
         |CREATE TEMPORARY TABLE $tbl
         |USING org.apache.spark.sql.parquet.DefaultSource
         |OPTIONS (
         |  path '$path'
         |)
       """.stripMargin)
  }

  // Registers tables in spark sql, specifying the path of each table.
  def registerSparkSQLTables(sqlContext: SQLContext, baseDir:String) = {
    System.out.println("Registering SparkSQL tables from dir: " + baseDir)
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.customer_parquet/", "customer")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.customer_address_parquet/", "customer_address")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.customer_demographics_parquet/", "customer_demographics")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.date_dim_parquet/", "date_dim")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.household_demographics_parquet/", "household_demographics")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.item_parquet/", "item")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.promotion_parquet/", "promotion")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.store_parquet/", "store")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.store_sales_proper_parquet/", "store_sales")
    registerSparkSQLTable(sqlContext,
        baseDir + "tpcds500gb.time_dim_parquet/", "time_dim")
  }

  /**
   * Runs sql, measuring the duration.
   */
  def runQuery(sqlContext:SQLContext, sql:String) = {
    val start = System.currentTimeMillis()
    sqlContext.sql(sql).show()
    val duration = System.currentTimeMillis() - start
    var durationString = ""
    if (duration < 1000) {
      durationString = duration + " ms"
    } else {
      durationString = (duration / 1000.) + " s"
    }
    System.out.println("TpcdsBenchmark: Query took " + durationString + ".")
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      System.err.println("Missing planner host.")
      printUsage()
      System.exit(-1)
    }

    val plannerHost = args(0)
    var queryName = "Query1"
    var db = "tpcds500gb_parquet"

    if (args.length > 1) queryName = args(1)
    if (args.length > 2) db = args(2)
    val sparkConf = new SparkConf().setAppName(queryName)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    if (plannerHost == "SPARK") {
      // Always treat binary as string.
      sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
      // TODO: make this more general. A bit hard since it relies on a particular folder
      // structure.
      registerSparkSQLTables(sqlContext,
          "hdfs://vd0214.halxg.cloudera.com:8020/user/impala/")
    } else {
      RecordServiceConf.setSQLContext(
        sqlContext, RecordServiceConfig.ConfVars.PLANNER_HOSTPORTS_CONF,
        plannerHost + ":12050")
      registerRecordServiceTables(sqlContext, db)
    }

    var error = false
    queryName.toUpperCase match {
      case "QUERY1" => query1(sqlContext)
      case "QUERY2" => query2(sqlContext)
      case "QUERY3" => query3(sqlContext)

      // TPCDS queries below
      case "Q3" => q3(sqlContext)
      case "Q6" => q6(sqlContext)
      case "Q7" => q7(sqlContext)
      case "Q8" => q8(sqlContext)
      case "Q19" => q19(sqlContext)
      case "Q27" => q27(sqlContext)
      case "Q34" => q34(sqlContext)
      case "Q42" => q42(sqlContext)
      case "Q43" => q43(sqlContext)
      case "Q46" => q46(sqlContext)
      case "Q47" => q47(sqlContext)
      case "Q52" => q52(sqlContext)
      case "Q53" => q53(sqlContext)
      case "Q55" => q55(sqlContext)
      case "Q59" => q59(sqlContext)
      case "Q61" => q61(sqlContext)
      case "Q63" => q63(sqlContext)
      case "Q65" => q65(sqlContext)
      case "Q68" => q68(sqlContext)
      case "Q73" => q73(sqlContext)
      case "Q88" => q88(sqlContext)
      case "Q96" => q96(sqlContext)
      case "Q98" => q98(sqlContext)

      case _ =>
        System.err.println("Unrecognized query name: " + queryName)
        error = true
    }
    sc.stop()
    if (error) sys.exit(-1)
  }



  /***********************************************************************************
   * The benchmark queries below.
   ***********************************************************************************/
  def query1(sqlContext: SQLContext) = {
    System.out.println("Running Query1")
    runQuery(sqlContext, "select count(ss_item_sk) from store_sales")
  }
  def query2(sqlContext: SQLContext) = {
    System.out.println("Running Query2")
    runQuery(sqlContext, "select count(*) from store_sales where ss_store_sk = 50")
  }
  def query3(sqlContext: SQLContext) = {
    System.out.println("Running Query3")
    runQuery(sqlContext, "select count(*) from store_sales where ss_sold_time_sk = 36211")
  }

  // TODO: is it easiest to read these from a file?
  def q3(sqlContext:SQLContext) = {
    System.out.println("Running Q3")
    runQuery(sqlContext,
      """
        |select
        |  dt.d_year,
        |  item.i_brand_id brand_id,
        |  item.i_brand brand,
        |  sum(ss_ext_sales_price) sum_agg
        |from
        |  date_dim dt,
        |  store_sales,
        |  item
        |where
        |  dt.d_date_sk = store_sales.ss_sold_date_sk
        |  and store_sales.ss_item_sk = item.i_item_sk
        |  and item.i_manufact_id = 436
        |  and dt.d_moy = 12
        |  -- partition key filters
        |  and (ss_sold_date_sk between 2451149 and 2451179
        |    or ss_sold_date_sk between 2451514 and 2451544
        |    or ss_sold_date_sk between 2451880 and 2451910
        |    or ss_sold_date_sk between 2452245 and 2452275
        |    or ss_sold_date_sk between 2452610 and 2452640)
        |group by
        |  dt.d_year,
        |  item.i_brand,
        |  item.i_brand_id
        |order by
        |  dt.d_year,
        |  sum_agg desc,
        |  brand_id
        |limit 100
      """.stripMargin)
  }

  def q6(sqlContext:SQLContext) = {
    System.out.println("Running Q6")
    runQuery(sqlContext,
      """
        |select * from (
        | select a.ca_state state, count(*) cnt
        | from customer_address a
        |     ,customer c
        |     ,store_sales s
        |     ,date_dim d
        |     ,item i
        | where
        |        a.ca_address_sk = c.c_current_addr_sk
        |        and c.c_customer_sk = s.ss_customer_sk
        |        and s.ss_sold_date_sk = d.d_date_sk
        |        and s.ss_item_sk = i.i_item_sk
        |        and d.d_month_seq =
        |             (select distinct (d_month_seq)
        |              from date_dim
        |               where d_year = 1999
        |                and d_moy = 1
        |               limit 1)
        |        and i.i_current_price > 1.2 *
        |             (select avg(j.i_current_price)
        |             from item j
        |             where j.i_category = i.i_category)
        | group by a.ca_state
        | having count(*) >= 10
        | order by cnt limit 100) as t
      """.stripMargin)
  }

  def q7(sqlContext:SQLContext) = {
    System.out.println("Running Q7")
    runQuery(sqlContext,
      """
        |select
        |  i_item_id,
        |  avg(ss_quantity) agg1,
        |  avg(ss_list_price) agg2,
        |  avg(ss_coupon_amt) agg3,
        |  avg(ss_sales_price) agg4
        |from
        |  store_sales,
        |  customer_demographics,
        |  date_dim,
        |  item,
        |  promotion
        |where
        |  ss_sold_date_sk = d_date_sk
        |  and ss_item_sk = i_item_sk
        |  and ss_cdemo_sk = cd_demo_sk
        |  and ss_promo_sk = p_promo_sk
        |  and cd_gender = 'F'
        |  and cd_marital_status = 'W'
        |  and cd_education_status = 'Primary'
        |  and (p_channel_email = 'N'
        |    or p_channel_event = 'N')
        |  and d_year = 1998
        |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
        |group by
        |  i_item_id
        |order by
        |  i_item_id
        |limit 100
      """.stripMargin)
  }

  def q8(sqlContext:SQLContext) = {
    System.out.println("Running Q8")
    runQuery(sqlContext,
      """
        |select
        |  s_store_name,
        |  sum(ss_net_profit)
        |from
        |  store_sales
        |  join store on (store_sales.ss_store_sk = store.s_store_sk)
        |  join
        |  (select
        |    a.ca_zip
        |  from
        |    (select
        |      substr(ca_zip, 1, 5) ca_zip,
        |      count( *) cnt
        |    from
        |      customer_address
        |      join  customer on (customer_address.ca_address_sk = customer.c_current_addr_sk)
        |    where
        |      c_preferred_cust_flag = 'Y'
        |    group by
        |      ca_zip
        |    having
        |      count(*) > 10
        |    ) a
        |    left semi join
        |    (select
        |      substr(ca_zip, 1, 5) ca_zip
        |    from
        |      customer_address
        |    where
        |      substr(ca_zip, 1, 5) in ('89436', '30868', '65085', '22977', '83927', '77557', '58429', '40697', '80614', '10502', '32779',
        |      '91137', '61265', '98294', '17921', '18427', '21203', '59362', '87291', '84093', '21505', '17184', '10866', '67898', '25797',
        |      '28055', '18377', '80332', '74535', '21757', '29742', '90885', '29898', '17819', '40811', '25990', '47513', '89531', '91068',
        |      '10391', '18846', '99223', '82637', '41368', '83658', '86199', '81625', '26696', '89338', '88425', '32200', '81427', '19053',
        |      '77471', '36610', '99823', '43276', '41249', '48584', '83550', '82276', '18842', '78890', '14090', '38123', '40936', '34425',
        |      '19850', '43286', '80072', '79188', '54191', '11395', '50497', '84861', '90733', '21068', '57666', '37119', '25004', '57835',
        |      '70067', '62878', '95806', '19303', '18840', '19124', '29785', '16737', '16022', '49613', '89977', '68310', '60069', '98360',
        |      '48649', '39050', '41793', '25002', '27413', '39736', '47208', '16515', '94808', '57648', '15009', '80015', '42961', '63982',
        |      '21744', '71853', '81087', '67468', '34175', '64008', '20261', '11201', '51799', '48043', '45645', '61163', '48375', '36447',
        |      '57042', '21218', '41100', '89951', '22745', '35851', '83326', '61125', '78298', '80752', '49858', '52940', '96976', '63792',
        |      '11376', '53582', '18717', '90226', '50530', '94203', '99447', '27670', '96577', '57856', '56372', '16165', '23427', '54561',
        |      '28806', '44439', '22926', '30123', '61451', '92397', '56979', '92309', '70873', '13355', '21801', '46346', '37562', '56458',
        |      '28286', '47306', '99555', '69399', '26234', '47546', '49661', '88601', '35943', '39936', '25632', '24611', '44166', '56648',
        |      '30379', '59785', '11110', '14329', '93815', '52226', '71381', '13842', '25612', '63294', '14664', '21077', '82626', '18799',
        |      '60915', '81020', '56447', '76619', '11433', '13414', '42548', '92713', '70467', '30884', '47484', '16072', '38936', '13036',
        |      '88376', '45539', '35901', '19506', '65690', '73957', '71850', '49231', '14276', '20005', '18384', '76615', '11635', '38177',
        |      '55607', '41369', '95447', '58581', '58149', '91946', '33790', '76232', '75692', '95464', '22246', '51061', '56692', '53121',
        |      '77209', '15482', '10688', '14868', '45907', '73520', '72666', '25734', '17959', '24677', '66446', '94627', '53535', '15560',
        |      '41967', '69297', '11929', '59403', '33283', '52232', '57350', '43933', '40921', '36635', '10827', '71286', '19736', '80619',
        |      '25251', '95042', '15526', '36496', '55854', '49124', '81980', '35375', '49157', '63512', '28944', '14946', '36503', '54010',
        |      '18767', '23969', '43905', '66979', '33113', '21286', '58471', '59080', '13395', '79144', '70373', '67031', '38360', '26705',
        |      '50906', '52406', '26066', '73146', '15884', '31897', '30045', '61068', '45550', '92454', '13376', '14354', '19770', '22928',
        |      '97790', '50723', '46081', '30202', '14410', '20223', '88500', '67298', '13261', '14172', '81410', '93578', '83583', '46047',
        |      '94167', '82564', '21156', '15799', '86709', '37931', '74703', '83103', '23054', '70470', '72008', '49247', '91911', '69998',
        |      '20961', '70070', '63197', '54853', '88191', '91830', '49521', '19454', '81450', '89091', '62378', '25683', '61869', '51744',
        |      '36580', '85778', '36871', '48121', '28810', '83712', '45486', '67393', '26935', '42393', '20132', '55349', '86057', '21309',
        |      '80218', '10094', '11357', '48819', '39734', '40758', '30432', '21204', '29467', '30214', '61024', '55307', '74621', '11622',
        |      '68908', '33032', '52868', '99194', '99900', '84936', '69036', '99149', '45013', '32895', '59004', '32322', '14933', '32936',
        |      '33562', '72550', '27385', '58049', '58200', '16808', '21360', '32961', '18586', '79307', '15492')
        |    ) b
        |  on (a.ca_zip = b.ca_zip)
        |  ) v1 on (substr(store.s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
        |where
        |  ss_sold_date_sk between 2452276 and 2452366
        |group by
        |  s_store_name
        |order by
        |  s_store_name
        |limit 100
      """.stripMargin)
  }

  def q19(sqlContext:SQLContext) = {
    System.out.println("Running Q19")
    runQuery(sqlContext,
      """
        |select
        |  i_brand_id brand_id,
        |  i_brand brand,
        |  i_manufact_id,
        |  i_manufact,
        |  sum(ss_ext_sales_price) ext_price
        |from
        |  date_dim,
        |  store_sales,
        |  item,
        |  customer,
        |  customer_address,
        |  store
        |where
        |  d_date_sk = ss_sold_date_sk
        |  and ss_item_sk = i_item_sk
        |  and i_manager_id = 7
        |  and d_moy = 11
        |  and d_year = 1999
        |  and ss_customer_sk = c_customer_sk
        |  and c_current_addr_sk = ca_address_sk
        |  and substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
        |  and ss_store_sk = s_store_sk
        |  and ss_sold_date_sk between 2451484 and 2451513
        |group by
        |  i_brand,
        |  i_brand_id,
        |  i_manufact_id,
        |  i_manufact
        |order by
        |  ext_price desc,
        |  i_brand,
        |  i_brand_id,
        |  i_manufact_id,
        |  i_manufact
        |limit 100
      """.stripMargin)
  }

  def q27(sqlContext:SQLContext) = {
    System.out.println("Running Q27")
    runQuery(sqlContext,
      """
        |select
        |  i_item_id,
        |  s_state,
        |  -- grouping(s_state) g_state,
        |  avg(ss_quantity) agg1,
        |  avg(ss_list_price) agg2,
        |  avg(ss_coupon_amt) agg3,
        |  avg(ss_sales_price) agg4
        |from
        |  store_sales,
        |  customer_demographics,
        |  date_dim,
        |  store,
        |  item
        |where
        |  ss_sold_date_sk = d_date_sk
        |  and ss_item_sk = i_item_sk
        |  and ss_store_sk = s_store_sk
        |  and ss_cdemo_sk = cd_demo_sk
        |  and cd_gender = 'F'
        |  and cd_marital_status = 'W'
        |  and cd_education_status = 'Primary'
        |  and d_year = 1998
        |  and s_state in ('WI', 'CA', 'TX', 'FL', 'WA', 'TN')
        |  and ss_sold_date_sk between 2450815 and 2451179  -- partition key filter
        |group by
        |  -- rollup (i_item_id, s_state)
        |  i_item_id,
        |  s_state
        |order by
        |  i_item_id,
        |  s_state
        |limit 100
      """.stripMargin)
  }

  def q34(sqlContext:SQLContext) = {
    System.out.println("Running Q34")
    runQuery(sqlContext,
      """
        |select
        |  c_last_name,
        |  c_first_name,
        |  c_salutation,
        |  c_preferred_cust_flag,
        |  ss_ticket_number,
        |  cnt
        |from
        |  (select
        |    ss_ticket_number,
        |    ss_customer_sk,
        |    count(*) cnt
        |  from
        |    store_sales,
        |    date_dim,
        |    store,
        |    household_demographics
        |  where
        |    store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |    and store_sales.ss_store_sk = store.s_store_sk
        |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        |    and (date_dim.d_dom between 1 and 3
        |      or date_dim.d_dom between 25 and 28)
        |    and (household_demographics.hd_buy_potential = '>10000'
        |      or household_demographics.hd_buy_potential = 'unknown')
        |    and household_demographics.hd_vehicle_count > 0
        |    and (case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end) > 1.2
        |    and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
        |    and store.s_county in ('Saginaw County', 'Sumner County', 'Appanoose County', 'Daviess County', 'Fairfield County', 'Raleigh County', 'Ziebach County', 'Williamson County')
        |    and ss_sold_date_sk between 2450816 and 2451910 -- partition key filter
        |  group by
        |    ss_ticket_number,
        |    ss_customer_sk
        |  ) dn,
        |  customer
        |where
        |  ss_customer_sk = c_customer_sk
        |  and cnt between 15 and 20
        |order by
        |  c_last_name,
        |  c_first_name,
        |  c_salutation,
        |  c_preferred_cust_flag desc
        |limit 100000
      """.stripMargin)
  }

  def q42(sqlContext:SQLContext) = {
    System.out.println("Running Q42")
    runQuery(sqlContext,
      """
        |select
        |  dt.d_year,
        |  item.i_category_id,
        |  item.i_category,
        |  sum(ss_ext_sales_price) as sales_price
        |from
        |  date_dim dt,
        |  store_sales,
        |  item
        |where
        |  dt.d_date_sk = store_sales.ss_sold_date_sk
        |  and store_sales.ss_item_sk = item.i_item_sk
        |  and item.i_manager_id = 1
        |  and dt.d_moy = 12
        |  and dt.d_year = 1998
        |  and ss_sold_date_sk between 2451149 and 2451179  -- partition key filter
        |group by
        |  dt.d_year,
        |  item.i_category_id,
        |  item.i_category
        |order by
        |  sales_price desc,
        |  dt.d_year,
        |  item.i_category_id,
        |  item.i_category
        |limit 100
      """.stripMargin)
  }

  def q43(sqlContext:SQLContext) = {
    System.out.println("Running Q43")
    runQuery(sqlContext,
      """
        |select
        |  s_store_name,
        |  s_store_id,
        |  sum(case when (d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
        |  sum(case when (d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
        |  sum(case when (d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
        |  sum(case when (d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
        |  sum(case when (d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
        |  sum(case when (d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
        |  sum(case when (d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
        |from
        |  date_dim,
        |  store_sales,
        |  store
        |where
        |  d_date_sk = ss_sold_date_sk
        |  and s_store_sk = ss_store_sk
        |  and s_gmt_offset = -5
        |  and d_year = 1998
        |  and ss_sold_date_sk between 2450816 and 2451179  -- partition key filter
        |group by
        |  s_store_name,
        |  s_store_id
        |order by
        |  s_store_name,
        |  s_store_id,
        |  sun_sales,
        |  mon_sales,
        |  tue_sales,
        |  wed_sales,
        |  thu_sales,
        |  fri_sales,
        |  sat_sales
        |limit 100
      """.stripMargin)
  }

  def q46(sqlContext:SQLContext) = {
    System.out.println("Running Q46")
    runQuery(sqlContext,
      """
        |select
        |  c_last_name,
        |  c_first_name,
        |  ca_city,
        |  bought_city,
        |  ss_ticket_number,
        |  amt,
        |  profit
        |from
        |  (select
        |    ss_ticket_number,
        |    ss_customer_sk,
        |    ca_city bought_city,
        |    sum(ss_coupon_amt) amt,
        |    sum(ss_net_profit) profit
        |  from
        |    store_sales,
        |    date_dim,
        |    store,
        |    household_demographics,
        |    customer_address
        |  where
        |    store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |    and store_sales.ss_store_sk = store.s_store_sk
        |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        |    and store_sales.ss_addr_sk = customer_address.ca_address_sk
        |    and (household_demographics.hd_dep_count = 5
        |      or household_demographics.hd_vehicle_count = 3)
        |    and date_dim.d_dow in (6, 0)
        |    and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)
        |    and store.s_city in ('Midway', 'Concord', 'Spring Hill', 'Brownsville', 'Greenville')
        |    -- partition key filter
        |    and ss_sold_date_sk in (2451181, 2451182, 2451188, 2451189, 2451195, 2451196, 2451202, 2451203, 2451209, 2451210, 2451216, 2451217,
        |                            2451223, 2451224, 2451230, 2451231, 2451237, 2451238, 2451244, 2451245, 2451251, 2451252, 2451258, 2451259,
        |                            2451265, 2451266, 2451272, 2451273, 2451279, 2451280, 2451286, 2451287, 2451293, 2451294, 2451300, 2451301,
        |                            2451307, 2451308, 2451314, 2451315, 2451321, 2451322, 2451328, 2451329, 2451335, 2451336, 2451342, 2451343,
        |                            2451349, 2451350, 2451356, 2451357, 2451363, 2451364, 2451370, 2451371, 2451377, 2451378, 2451384, 2451385,
        |                            2451391, 2451392, 2451398, 2451399, 2451405, 2451406, 2451412, 2451413, 2451419, 2451420, 2451426, 2451427,
        |                            2451433, 2451434, 2451440, 2451441, 2451447, 2451448, 2451454, 2451455, 2451461, 2451462, 2451468, 2451469,
        |                            2451475, 2451476, 2451482, 2451483, 2451489, 2451490, 2451496, 2451497, 2451503, 2451504, 2451510, 2451511,
        |                            2451517, 2451518, 2451524, 2451525, 2451531, 2451532, 2451538, 2451539, 2451545, 2451546, 2451552, 2451553,
        |                            2451559, 2451560, 2451566, 2451567, 2451573, 2451574, 2451580, 2451581, 2451587, 2451588, 2451594, 2451595,
        |                            2451601, 2451602, 2451608, 2451609, 2451615, 2451616, 2451622, 2451623, 2451629, 2451630, 2451636, 2451637,
        |                            2451643, 2451644, 2451650, 2451651, 2451657, 2451658, 2451664, 2451665, 2451671, 2451672, 2451678, 2451679,
        |                            2451685, 2451686, 2451692, 2451693, 2451699, 2451700, 2451706, 2451707, 2451713, 2451714, 2451720, 2451721,
        |                            2451727, 2451728, 2451734, 2451735, 2451741, 2451742, 2451748, 2451749, 2451755, 2451756, 2451762, 2451763,
        |                            2451769, 2451770, 2451776, 2451777, 2451783, 2451784, 2451790, 2451791, 2451797, 2451798, 2451804, 2451805,
        |                            2451811, 2451812, 2451818, 2451819, 2451825, 2451826, 2451832, 2451833, 2451839, 2451840, 2451846, 2451847,
        |                            2451853, 2451854, 2451860, 2451861, 2451867, 2451868, 2451874, 2451875, 2451881, 2451882, 2451888, 2451889,
        |                            2451895, 2451896, 2451902, 2451903, 2451909, 2451910, 2451916, 2451917, 2451923, 2451924, 2451930, 2451931,
        |                            2451937, 2451938, 2451944, 2451945, 2451951, 2451952, 2451958, 2451959, 2451965, 2451966, 2451972, 2451973,
        |                            2451979, 2451980, 2451986, 2451987, 2451993, 2451994, 2452000, 2452001, 2452007, 2452008, 2452014, 2452015,
        |                            2452021, 2452022, 2452028, 2452029, 2452035, 2452036, 2452042, 2452043, 2452049, 2452050, 2452056, 2452057,
        |                            2452063, 2452064, 2452070, 2452071, 2452077, 2452078, 2452084, 2452085, 2452091, 2452092, 2452098, 2452099,
        |                            2452105, 2452106, 2452112, 2452113, 2452119, 2452120, 2452126, 2452127, 2452133, 2452134, 2452140, 2452141,
        |                            2452147, 2452148, 2452154, 2452155, 2452161, 2452162, 2452168, 2452169, 2452175, 2452176, 2452182, 2452183,
        |                            2452189, 2452190, 2452196, 2452197, 2452203, 2452204, 2452210, 2452211, 2452217, 2452218, 2452224, 2452225,
        |                            2452231, 2452232, 2452238, 2452239, 2452245, 2452246, 2452252, 2452253, 2452259, 2452260, 2452266, 2452267,
        |                            2452273, 2452274)
        |  group by
        |    ss_ticket_number,
        |    ss_customer_sk,
        |    ss_addr_sk,
        |    ca_city
        |  ) dn,
        |  customer,
        |  customer_address current_addr
        |where
        |  ss_customer_sk = c_customer_sk
        |  and customer.c_current_addr_sk = current_addr.ca_address_sk
        |  and current_addr.ca_city <> bought_city
        |order by
        |  c_last_name,
        |  c_first_name,
        |  ca_city,
        |  bought_city,
        |  ss_ticket_number
      """.stripMargin)
  }

  def q47(sqlContext:SQLContext) = {
    System.out.println("Running Q47")
    runQuery(sqlContext,
      """
        |with v1 as (
        | select i_category, i_brand,
        |        s_store_name, s_company_name,
        |        d_year, d_moy,
        |        sum(ss_sales_price) sum_sales,
        |        avg(sum(ss_sales_price)) over
        |          (partition by i_category, i_brand,
        |                     s_store_name, s_company_name, d_year)
        |          avg_monthly_sales,
        |        rank() over
        |          (partition by i_category, i_brand,
        |                     s_store_name, s_company_name
        |           order by d_year, d_moy) rn
        | from item, store_sales, date_dim, store
        | where ss_item_sk = i_item_sk and
        |       ss_sold_date_sk = d_date_sk and
        |       ss_store_sk = s_store_sk and
        |       (
        |         d_year = 2000 or
        |         ( d_year = 2000-1 and d_moy =12) or
        |         ( d_year = 2000+1 and d_moy =1)
        |       )
        | group by i_category, i_brand,
        |          s_store_name, s_company_name,
        |          d_year, d_moy),
        | v2 as(
        | select v1.i_category, v1.i_brand
        |        ,v1.d_year
        |        ,v1.avg_monthly_sales
        |        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
        | from v1, v1 v1_lag, v1 v1_lead
        | where v1.i_category = v1_lag.i_category and
        |       v1.i_category = v1_lead.i_category and
        |       v1.i_brand = v1_lag.i_brand and
        |       v1.i_brand = v1_lead.i_brand and
        |       v1.s_store_name = v1_lag.s_store_name and
        |       v1.s_store_name = v1_lead.s_store_name and
        |       v1.s_company_name = v1_lag.s_company_name and
        |       v1.s_company_name = v1_lead.s_company_name and
        |       v1.rn = v1_lag.rn + 1 and
        |       v1.rn = v1_lead.rn - 1)
        | select * from ( select  *
        | from v2
        | where  d_year = 2000 and
        |        avg_monthly_sales > 0 and
        |        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
        | order by sum_sales - avg_monthly_sales, d_year
        | limit 100
        | ) as v3
      """.stripMargin)
  }

  def q52(sqlContext:SQLContext) = {
    System.out.println("Running Q52")
    runQuery(sqlContext,
      """
        |select
        |  dt.d_year,
        |  item.i_brand_id brand_id,
        |  item.i_brand brand,
        |  sum(ss_ext_sales_price) ext_price
        |from
        |  date_dim dt,
        |  store_sales,
        |  item
        |where
        |  dt.d_date_sk = store_sales.ss_sold_date_sk
        |  and store_sales.ss_item_sk = item.i_item_sk
        |  and item.i_manager_id = 1
        |  and dt.d_moy = 12
        |  and dt.d_year = 1998
        |  and ss_sold_date_sk between 2451149 and 2451179 -- added for partition pruning
        |group by
        |  dt.d_year,
        |  item.i_brand,
        |  item.i_brand_id
        |order by
        |  dt.d_year,
        |  ext_price desc,
        |  brand_id
        |limit 100
      """.stripMargin)
  }

  def q53(sqlContext:SQLContext) = {
    System.out.println("Running Q53")
    runQuery(sqlContext,
      """
        |select
        |  *
        |from
        |  (select
        |    i_manufact_id,
        |    sum(ss_sales_price) sum_sales
        |    -- avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
        |  from
        |    item,
        |    store_sales,
        |    date_dim,
        |    store
        |  where
        |    ss_item_sk = i_item_sk
        |    and ss_sold_date_sk = d_date_sk
        |    and ss_store_sk = s_store_sk
        |    and d_month_seq in (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
        |    and ((i_category in ('Books', 'Children', 'Electronics')
        |      and i_class in ('personal', 'portable', 'reference', 'self-help')
        |      and i_brand in ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))
        |    or (i_category in ('Women', 'Music', 'Men')
        |      and i_class in ('accessories', 'classical', 'fragrances', 'pants')
        |      and i_brand in ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')))
        |    and ss_sold_date_sk between 2451911 and 2452275 -- partition key filter
        |  group by
        |    i_manufact_id,
        |    d_qoy
        |  ) tmp1
        |-- where
        |--   case when avg_quarterly_sales > 0 then abs (sum_sales - avg_quarterly_sales) / avg_quarterly_sales else null end > 0.1
        |order by
        |  -- avg_quarterly_sales,
        |  sum_sales,
        |  i_manufact_id
        |limit 100
      """.stripMargin)
  }

  def q55(sqlContext:SQLContext) = {
    System.out.println("Running Q55")
    runQuery(sqlContext,
      """
        |select
        |  i_brand_id brand_id,
        |  i_brand brand,
        |  sum(ss_ext_sales_price) ext_price
        |from
        |  date_dim,
        |  store_sales,
        |  item
        |where
        |  d_date_sk = ss_sold_date_sk
        |  and ss_item_sk = i_item_sk
        |  and i_manager_id = 36
        |  and d_moy = 12
        |  and d_year = 2001
        |  and ss_sold_date_sk between 2452245 and 2452275 -- partition key filter
        |group by
        |  i_brand,
        |  i_brand_id
        |order by
        |  ext_price desc,
        |  i_brand_id
        |limit 100
      """.stripMargin)
  }

  def q59(sqlContext:SQLContext) = {
    System.out.println("Running Q59")
    runQuery(sqlContext,
      """
        |with
        |  wss as
        |  (select
        |    d_week_seq,
        |    ss_store_sk,
        |    sum(case when (d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
        |    sum(case when (d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
        |    sum(case when (d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
        |    sum(case when (d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
        |    sum(case when (d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
        |    sum(case when (d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
        |    sum(case when (d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
        |  from
        |    store_sales,
        |    date_dim
        |  where
        |    d_date_sk = ss_sold_date_sk
        |  group by
        |    d_week_seq,
        |    ss_store_sk
        |  )
        |select
        |  s_store_name1,
        |  s_store_id1,
        |  d_week_seq1,
        |  sun_sales1 / sun_sales2,
        |  mon_sales1 / mon_sales2,
        |  tue_sales1 / tue_sales2,
        |  wed_sales1 / wed_sales2,
        |  thu_sales1 / thu_sales2,
        |  fri_sales1 / fri_sales2,
        |  sat_sales1 / sat_sales2
        |from
        |  (select
        |    s_store_name s_store_name1,
        |    wss.d_week_seq d_week_seq1,
        |    s_store_id s_store_id1,
        |    sun_sales sun_sales1,
        |    mon_sales mon_sales1,
        |    tue_sales tue_sales1,
        |    wed_sales wed_sales1,
        |    thu_sales thu_sales1,
        |    fri_sales fri_sales1,
        |    sat_sales sat_sales1
        |  from
        |    wss,
        |    store,
        |    date_dim d
        |  where
        |    d.d_week_seq = wss.d_week_seq
        |    and ss_store_sk = s_store_sk
        |    and d_month_seq between 1185 and 1185 + 11
        |  ) y,
        |  (select
        |    s_store_name s_store_name2,
        |    wss.d_week_seq d_week_seq2,
        |    s_store_id s_store_id2,
        |    sun_sales sun_sales2,
        |    mon_sales mon_sales2,
        |    tue_sales tue_sales2,
        |    wed_sales wed_sales2,
        |    thu_sales thu_sales2,
        |    fri_sales fri_sales2,
        |    sat_sales sat_sales2
        |  from
        |    wss,
        |    store,
        |    date_dim d
        |  where
        |    d.d_week_seq = wss.d_week_seq
        |    and ss_store_sk = s_store_sk
        |    and d_month_seq between 1185 + 12 and 1185 + 23
        |  ) x
        |where
        |  s_store_id1 = s_store_id2
        |  and d_week_seq1 = d_week_seq2 - 52
        |order by
        |  s_store_name1,
        |  s_store_id1,
        |  d_week_seq1
        |limit 100
      """.stripMargin)
  }

  def q61(sqlContext:SQLContext) = {
    System.out.println("Running Q61")
    runQuery(sqlContext,
      """
        |select promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
        |from
        |  (select sum(ss_ext_sales_price) promotions
        |   from  store_sales
        |        ,store
        |        ,promotion
        |        ,date_dim
        |        ,customer
        |        ,customer_address
        |        ,item
        |   where ss_sold_date_sk = d_date_sk
        |   and   ss_store_sk = s_store_sk
        |   and   ss_promo_sk = p_promo_sk
        |   and   ss_customer_sk= c_customer_sk
        |   and   ca_address_sk = c_current_addr_sk
        |   and   ss_item_sk = i_item_sk
        |   and   ca_gmt_offset = -5
        |   and   i_category = 'Books'
        |   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
        |   and   s_gmt_offset = -5
        |   and   d_year = 2000
        |   and   d_moy  = 11) promotional_sales,
        |  (select sum(ss_ext_sales_price) total
        |   from  store_sales
        |        ,store
        |        ,date_dim
        |        ,customer
        |        ,customer_address
        |        ,item
        |   where ss_sold_date_sk = d_date_sk
        |   and   ss_store_sk = s_store_sk
        |   and   ss_customer_sk= c_customer_sk
        |   and   ca_address_sk = c_current_addr_sk
        |   and   ss_item_sk = i_item_sk
        |   and   ca_gmt_offset = -5
        |   and   i_category = 'Books'
        |   and   s_gmt_offset = -5
        |   and   d_year = 2000
        |   and   d_moy  = 11) all_sales
        |order by promotions, total
      """.stripMargin)
  }

  def q63(sqlContext:SQLContext) = {
    System.out.println("Running Q63")
    runQuery(sqlContext,
      """
        |select
        |  *
        |from
        |  (select
        |    i_manager_id,
        |    sum(ss_sales_price) sum_sales,
        |    avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
        |  from
        |    item,
        |    store_sales,
        |    date_dim,
        |    store
        |  where
        |    ss_item_sk = i_item_sk
        |    and ss_sold_date_sk = d_date_sk
        |    and ss_store_sk = s_store_sk
        |    and d_month_seq in (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
        |    and ((i_category in ('Books', 'Children', 'Electronics')
        |      and i_class in ('personal', 'portable', 'refernece', 'self-help')
        |      and i_brand in ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))
        |    or (i_category in ('Women', 'Music', 'Men')
        |      and i_class in ('accessories', 'classical', 'fragrances', 'pants')
        |      and i_brand in ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')))
        |    and ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
        |  group by
        |    i_manager_id,
        |    d_moy
        |  ) tmp1
        |where
        |case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
        |order by
        |  i_manager_id,
        |  avg_monthly_sales,
        |  sum_sales
        |limit 100
      """.stripMargin)
  }

  def q65(sqlContext:SQLContext) = {
    System.out.println("Running Q65")
    runQuery(sqlContext,
      """
        |select
        |  s_store_name,
        |  i_item_desc,
        |  sc.revenue,
        |  i_current_price,
        |  i_wholesale_cost,
        |  i_brand
        |from
        |  store,
        |  item,
        |  (select
        |    ss_store_sk,
        |    avg(revenue) as ave
        |  from
        |    (select
        |      ss_store_sk,
        |      ss_item_sk,
        |      sum(ss_sales_price) as revenue
        |    from
        |      store_sales,
        |      date_dim
        |    where
        |      ss_sold_date_sk = d_date_sk
        |      and d_month_seq between 1212 and 1212 + 11
        |      and ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
        |    group by
        |      ss_store_sk,
        |      ss_item_sk
        |    ) sa
        |  group by
        |    ss_store_sk
        |  ) sb,
        |  (select
        |    ss_store_sk,
        |    ss_item_sk,
        |    sum(ss_sales_price) as revenue
        |  from
        |    store_sales,
        |    date_dim
        |  where
        |    ss_sold_date_sk = d_date_sk
        |    and d_month_seq between 1212 and 1212 + 11
        |    and ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
        |  group by
        |    ss_store_sk,
        |    ss_item_sk
        |  ) sc
        |where
        |  sb.ss_store_sk = sc.ss_store_sk
        |  and sc.revenue <= 0.1 * sb.ave
        |  and s_store_sk = sc.ss_store_sk
        |  and i_item_sk = sc.ss_item_sk
        |order by
        |  s_store_name,
        |  i_item_desc
        |limit 100
      """.stripMargin)
  }

  def q68(sqlContext:SQLContext) = {
    System.out.println("Running Q68")
    runQuery(sqlContext,
      """
        |select
        |  c_last_name,
        |  c_first_name,
        |  ca_city,
        |  bought_city,
        |  ss_ticket_number,
        |  extended_price,
        |  extended_tax,
        |  list_price
        |from
        |  (select
        |    ss_ticket_number,
        |    ss_customer_sk,
        |    ca_city bought_city,
        |    sum(ss_ext_sales_price) extended_price,
        |    sum(ss_ext_list_price) list_price,
        |    sum(ss_ext_tax) extended_tax
        |  from
        |    store_sales,
        |    date_dim,
        |    store,
        |    household_demographics,
        |    customer_address
        |  where
        |    store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |    and store_sales.ss_store_sk = store.s_store_sk
        |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        |    and store_sales.ss_addr_sk = customer_address.ca_address_sk
        |    -- and date_dim.d_dom between 1 and 2
        |    and (household_demographics.hd_dep_count = 5
        |      or household_demographics.hd_vehicle_count = 3)
        |    -- and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)
        |    and store.s_city in ('Midway', 'Fairview')
        |    -- partition key filter
        |    -- and ss_sold_date_sk in (2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301, 2451331,
        |    --                         2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484, 2451485,
        |    --                         2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637, 2451666,
        |    --                         2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819, 2451820,
        |    --                         2451850, 2451851, 2451880, 2451881, 2451911, 2451912, 2451942, 2451943, 2451970, 2451971, 2452001,
        |    --                         2452002, 2452031, 2452032, 2452062, 2452063, 2452092, 2452093, 2452123, 2452124, 2452154, 2452155,
        |    --                         2452184, 2452185, 2452215, 2452216, 2452245, 2452246)
        |    and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
        |    and d_date between '1999-01-01' and '1999-03-31'
        |  group by
        |    ss_ticket_number,
        |    ss_customer_sk,
        |    ss_addr_sk,
        |    ca_city
        |  ) dn,
        |  customer,
        |  customer_address current_addr
        |where
        |  ss_customer_sk = c_customer_sk
        |  and customer.c_current_addr_sk = current_addr.ca_address_sk
        |  and current_addr.ca_city <> bought_city
        |order by
        |  c_last_name,
        |  ss_ticket_number
        |limit 100
      """.stripMargin)
  }

  def q73(sqlContext:SQLContext) = {
    System.out.println("Running Q73")
    runQuery(sqlContext,
      """
        |select
        |  c_last_name,
        |  c_first_name,
        |  c_salutation,
        |  c_preferred_cust_flag,
        |  ss_ticket_number,
        |  cnt
        |from
        |  (select
        |    ss_ticket_number,
        |    ss_customer_sk,
        |    count(*) cnt
        |  from
        |    store_sales,
        |    date_dim,
        |    store,
        |    household_demographics
        |  where
        |    store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |    and store_sales.ss_store_sk = store.s_store_sk
        |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        |    -- and date_dim.d_dom between 1 and 2
        |    and (household_demographics.hd_buy_potential = '>10000'
        |      or household_demographics.hd_buy_potential = 'unknown')
        |    and household_demographics.hd_vehicle_count > 0
        |    and case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end > 1
        |    -- and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
        |    and store.s_county in ('Saginaw County', 'Sumner County', 'Appanoose County', 'Daviess County')
        |    -- partition key filter
        |    -- and ss_sold_date_sk in (2450816, 2450846, 2450847, 2450874, 2450875, 2450905, 2450906, 2450935, 2450936, 2450966, 2450967,
        |    --                         2450996, 2450997, 2451027, 2451028, 2451058, 2451059, 2451088, 2451089, 2451119, 2451120, 2451149,
        |    --                         2451150, 2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301,
        |    --                         2451331, 2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484,
        |    --                         2451485, 2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637,
        |    --                         2451666, 2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819,
        |    --                         2451820, 2451850, 2451851, 2451880, 2451881)
        |    and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
        |  group by
        |    ss_ticket_number,
        |    ss_customer_sk
        |  ) dj,
        |  customer
        |where
        |  ss_customer_sk = c_customer_sk
        |  and cnt between 1 and 5
        |order by
        |  cnt desc
        |limit 1000
      """.stripMargin)
  }

  def q79(sqlContext:SQLContext) = {
    System.out.println("Running Q79")
    runQuery(sqlContext,
      """
        |select
        |  c_last_name,
        |  c_first_name,
        |  substr(s_city, 1, 30),
        |  ss_ticket_number,
        |  amt,
        |  profit
        |from
        |  (select
        |    ss_ticket_number,
        |    ss_customer_sk,
        |    store.s_city,
        |    sum(ss_coupon_amt) amt,
        |    sum(ss_net_profit) profit
        |  from
        |    store_sales,
        |    date_dim,
        |    store,
        |    household_demographics
        |  where
        |    store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |    and store_sales.ss_store_sk = store.s_store_sk
        |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        |    and (household_demographics.hd_dep_count = 8
        |      or household_demographics.hd_vehicle_count > 0)
        |    -- and date_dim.d_dow = 1
        |    -- and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
        |    and store.s_number_employees between 200 and 295
        |    -- partition key filter
        |    -- and ss_sold_date_sk in (2450819, 2450826, 2450833, 2450840, 2450847, 2450854, 2450861, 2450868, 2450875, 2450882, 2450889,
        |    -- 2450896, 2450903, 2450910, 2450917, 2450924, 2450931, 2450938, 2450945, 2450952, 2450959, 2450966, 2450973, 2450980, 2450987,
        |    -- 2450994, 2451001, 2451008, 2451015, 2451022, 2451029, 2451036, 2451043, 2451050, 2451057, 2451064, 2451071, 2451078, 2451085,
        |    -- 2451092, 2451099, 2451106, 2451113, 2451120, 2451127, 2451134, 2451141, 2451148, 2451155, 2451162, 2451169, 2451176, 2451183,
        |    -- 2451190, 2451197, 2451204, 2451211, 2451218, 2451225, 2451232, 2451239, 2451246, 2451253, 2451260, 2451267, 2451274, 2451281,
        |    -- 2451288, 2451295, 2451302, 2451309, 2451316, 2451323, 2451330, 2451337, 2451344, 2451351, 2451358, 2451365, 2451372, 2451379,
        |    -- 2451386, 2451393, 2451400, 2451407, 2451414, 2451421, 2451428, 2451435, 2451442, 2451449, 2451456, 2451463, 2451470, 2451477,
        |    -- 2451484, 2451491, 2451498, 2451505, 2451512, 2451519, 2451526, 2451533, 2451540, 2451547, 2451554, 2451561, 2451568, 2451575,
        |    -- 2451582, 2451589, 2451596, 2451603, 2451610, 2451617, 2451624, 2451631, 2451638, 2451645, 2451652, 2451659, 2451666, 2451673,
        |    -- 2451680, 2451687, 2451694, 2451701, 2451708, 2451715, 2451722, 2451729, 2451736, 2451743, 2451750, 2451757, 2451764, 2451771,
        |    -- 2451778, 2451785, 2451792, 2451799, 2451806, 2451813, 2451820, 2451827, 2451834, 2451841, 2451848, 2451855, 2451862, 2451869,
        |    -- 2451876, 2451883, 2451890, 2451897, 2451904)
        |    and d_date between '1999-01-01' and '1999-03-31'
        |    and ss_sold_date_sk between 2451180 and 2451269  -- partition key filter
        |  group by
        |    ss_ticket_number,
        |    ss_customer_sk,
        |    ss_addr_sk,
        |    store.s_city
        |  ) ms,
        |  customer
        |where
        |  ss_customer_sk = c_customer_sk
        |order by
        |  c_last_name,
        |  c_first_name,
        |  substr(s_city, 1, 30),
        |  profit
        |limit 100
      """.stripMargin)
  }

  def q88(sqlContext:SQLContext) = {
    System.out.println("Running Q88")
    runQuery(sqlContext,
      """
        |select  *
        |from
        | (select count(*) h8_30_to_9
        | from store_sales, household_demographics , time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 8
        |     and time_dim.t_minute >= 30
        |     and ((household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or
        |          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
        |          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2))
        |     and store.s_store_name = 'ese') s1,
        | (select count(*) h9_to_9_30
        | from store_sales, household_demographics , time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 9
        |     and time_dim.t_minute < 30
        |     and ((household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or
        |          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
        |          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2))
        |     and store.s_store_name = 'ese') s2,
        | (select count(*) h9_30_to_10
        | from store_sales, household_demographics , time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 9
        |     and time_dim.t_minute >= 30
        |     and ((household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or
        |          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
        |          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2))
        |     and store.s_store_name = 'ese') s3,
        | (select count(*) h10_to_10_30
        | from store_sales, household_demographics , time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 10
        |     and time_dim.t_minute < 30
        |     and ((household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or
        |          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
        |          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2))
        |     and store.s_store_name = 'ese') s4,
        | (select count(*) h10_30_to_11
        | from store_sales, household_demographics , time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 10
        |     and time_dim.t_minute >= 30
        |     and ((household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or
        |          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
        |          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2))
        |     and store.s_store_name = 'ese') s5,
        | (select count(*) h11_to_11_30
        | from store_sales, household_demographics , time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 11
        |     and time_dim.t_minute < 30
        |     and ((household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or
        |          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
        |          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2))
        |     and store.s_store_name = 'ese') s6,
        | (select count(*) h11_30_to_12
        | from store_sales, household_demographics , time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 11
        |     and time_dim.t_minute >= 30
        |     and ((household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or
        |          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
        |          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2))
        |     and store.s_store_name = 'ese') s7,
        | (select count(*) h12_to_12_30
        | from store_sales, household_demographics , time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 12
        |     and time_dim.t_minute < 30
        |     and ((household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or
        |          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
        |          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2))
        |     and store.s_store_name = 'ese') s8
      """.stripMargin)
  }

  def q89(sqlContext:SQLContext) = {
    System.out.println("Running Q89")
    runQuery(sqlContext,
      """
        |select * from (select  *
        |from (
        |select i_category, i_class, i_brand,
        |       s_store_name, s_company_name,
        |       d_moy,
        |       sum(ss_sales_price) sum_sales,
        |       avg(sum(ss_sales_price)) over
        |         (partition by i_category, i_brand, s_store_name, s_company_name)
        |         avg_monthly_sales
        |from item, store_sales, date_dim, store
        |where ss_item_sk = i_item_sk and
        |      ss_sold_date_sk = d_date_sk and
        |      ss_store_sk = s_store_sk and
        |      d_year in (2000) and
        |        ((i_category in ('Children','Music','Home') and
        |          i_class in ('toddlers','pop','lighting')
        |         )
        |      or (i_category in ('Jewelry','Books','Sports') and
        |          i_class in ('costume','travel','football')
        |        ))
        |      and ss_sold_date_sk between 2451545 and 2451910  -- partition key filter
        |group by i_category, i_class, i_brand,
        |         s_store_name, s_company_name, d_moy) tmp1
        |where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
        |order by sum_sales - avg_monthly_sales, s_store_name
        |limit 100) tmp2
      """.stripMargin)
  }

  def q96(sqlContext:SQLContext) = {
    System.out.println("Running Q96")
    runQuery(sqlContext,
      """
        |SELECT
        |  COUNT(*) AS total
        |FROM store_sales ss
        |JOIN time_dim td
        |  ON (ss.ss_sold_time_sk = td.t_time_sk)
        |JOIN household_demographics hd
        |  ON (ss.ss_hdemo_sk = hd.hd_demo_sk)
        |JOIN store s
        |  ON (ss.ss_store_sk = s.s_store_sk)
        |WHERE
        |  td.t_hour = 8
        |  AND td.t_minute >= 30
        |  AND hd.hd_dep_count = 5
        |  AND s.s_store_name = 'ese'
      """.stripMargin)
  }

  def q98(sqlContext:SQLContext) = {
    System.out.println("Running Q98")
    runQuery(sqlContext,
      """
        |select
        |  i_item_desc,
        |  i_category,
        |  i_class,
        |  i_current_price,
        |  sum(ss_ext_sales_price) as itemrevenue,
        |  sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio
        |from
        |  store_sales,
        |  item,
        |  date_dim
        |where
        |  ss_item_sk = i_item_sk
        |  and i_category in ('Jewelry', 'Sports', 'Books')
        |  and ss_sold_date_sk = d_date_sk
        |  and ss_sold_date_sk between 2451911 and 2451941
        |  and d_date between '2001-01-01' and '2001-01-31' -- original uses interval and the
        |group by
        |  i_item_id,
        |  i_item_desc,
        |  i_category,
        |  i_class,
        |  i_current_price
        |order by
        |  i_category,
        |  i_class,
        |  i_item_id,
        |  i_item_desc,
        |  revenueratio
        |limit 1000
      """.stripMargin)
  }
}
