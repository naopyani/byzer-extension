package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.ml.stat.{ChiSquareTest, Correlation, KolmogorovSmirnovTest}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, countDistinct, lit, mean, variance, when}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, ShortType}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, Doc, MarkDownDoc, ModelType, ProcessType, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.mllib.ets.PluginBaseETAuth


/**
 * 4/1/2023 ff(fflxm@outlook.com)
 */
class SQLColumnsAnalysis (override val uid: String) extends SQLAlg
  with Functions
  with MllibFunctions
  with BaseClassification
  with PluginBaseETAuth {

  def this() = this(BaseParams.randomUID())

  val sifttype = Array(IntegerType.toString, ShortType.toString(), DoubleType.toString(), FloatType.toString(), LongType.toString())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    if (df.isEmpty){
      throw new RuntimeException(s"Input DataFrame is empty : ${df.show()}! ")
      return df.sparkSession.emptyDataFrame
    }

    val _action = params.getOrElse(action.name, $(action).toString)
    val _target = params.getOrElse(target.name, $(target).toString)
    val _positive = params.getOrElse(positive.name, $(positive).toString)
//    val _fields = params.getOrElse(fields.name, $(fields).mkString(",")).split(",")
//    if (_fields.length == 0) return df

    val dfName = params("__dfname__")
    val data = _action match {
      case "KS" => ks(df)
      case "IV" => iv(df, _target, _positive)
      case "P_VALUE" => pvalue(df)
      case "MISS_RATE" => lack(df)
      case "HORIZONTAL_SCORE" => horizontal(df)
      case "PEARSON_IV" => person_iv(df)
      case "PEARSON_DV" => person(df, _target)
      case _ => throw new RuntimeException(s"Undefined ColumnsAnalysis action: ${_action}! ")
    }
    data
  }

  private def ks(df: DataFrame): DataFrame = {
    var seqresult:Map[String , Double] = Map()
    val fields = df.dtypes.toMap

    fields.foreach(colName => {
      if(sifttype.contains(colName._2)) {
        val ff = df.withColumn(colName._1, col(colName._1).isNotNull.cast(DoubleType))

        val Row(pValue: Double, statistic: Double) = KolmogorovSmirnovTest
          .test(ff, colName._1, "norm", 0, 1).head()
        seqresult += (colName._1 -> statistic)
      }
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def iv(df: DataFrame, target:String, positive:String): DataFrame = {
    var seqresult:Map[String , Double] = Map()
    val fields = df.dtypes.toMap
//    if(!fields.contains(target))
//      throw new RuntimeException(s"The target name : ${target} is not exist in column : ${fields} ! ")
//    else
//      if(!sifttype.contains(fields.get(target).head))
//        throw new RuntimeException(s"The target name : ${target} DataType ${fields.get(target).head} not exist in DataType : ${sifttype.toSeq} ! ")

    fields.foreach(colName => {
      if(sifttype.contains(colName._2)) {
        //val ff = df.withColumn(colName, col(colName).cast(DoubleType))
        val ff = df.withColumn(target, when(col(colName._1).isNotNull === positive, 1).otherwise(0))
        val statistic = IV(ff, colName._1, target)
        seqresult += (colName._1 -> statistic)
      }
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def pvalue(df: DataFrame): DataFrame = {
    var seqresult:Map[String , Double] = Map()
    val fields = df.dtypes.toMap
    fields.foreach(colName => {
      if(sifttype.contains(colName._2)) {
        val ff = df.withColumn(colName._1, col(colName._1).isNotNull.cast(DoubleType))

        val Row(pValue: Double, statistic: Double) = KolmogorovSmirnovTest
          .test(ff, colName._1, "norm", 0, 1).head()
        seqresult += (colName._1 -> pValue)
      }
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def lack(df: DataFrame): DataFrame = {
    var seqresult:Map[String , Double] = Map();
    val fields = df.dtypes.toMap
    fields.foreach(colName => {
      if(sifttype.contains(colName._2)) {
        val miss_cnt = df.select(col(colName._1)).where(col(colName._1).isNull).count
        //统计每列的缺失率，并保留4位小数
        val statistic = (miss_cnt / df.count()).toDouble.formatted("%.4f").toDouble

        seqresult += (colName._1 -> statistic)
      }
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def horizontal(df: DataFrame): DataFrame = {
    var seqresult:Map[String , Double] = Map();
    val fields = df.dtypes.toMap
    fields.foreach(colName => {
      if(sifttype.contains(colName._2)) {
        val statistic = df.agg(countDistinct(colName._1)).collect().map(_ (0)).toList(0).toString.toDouble
        seqresult += (colName._1 -> statistic)
      }
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def person(df: DataFrame, target:String): DataFrame = {
    var seqresult:Map[String , Double] = Map()
    val fields = df.dtypes.toMap
    if(!fields.contains(target))
      throw new RuntimeException(s"The target name : ${target} is not exist in column : ${fields} ! ")
    else
      if(!sifttype.contains(fields.get(target).head))
        throw new RuntimeException(s"The target name : ${target} DataType ${fields.get(target).head} not exist in DataType : ${sifttype.toSeq} ! ")

    fields.foreach(colName => {
      if(sifttype.contains(colName._2)) {
        //val Row(coeff2: Matrix) = Correlation.corr(df,colName).head()
        val pearsonValue = PEARSON(df, colName._1, target)
        seqresult += (colName._1 -> pearsonValue)
      }
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def person_iv(df: DataFrame): DataFrame = {
    var seqresult:Map[String , Double] = Map()
    val fields = df.dtypes.toMap

    fields.foreach(colName => {
      if(sifttype.contains(colName._2)) {
        //val Row(coeff2: Matrix) = Correlation.corr(df,colName).head()
        fields.foreach(colName2 => {
          if(sifttype.contains(colName2._2)) {
            //val Row(coeff2: Matrix) = Correlation.corr(df,colName).head()
            val pearsonValue = PEARSON(df, colName2._1, colName._1)
            seqresult += (colName._1+"&"+colName2._1 -> pearsonValue)
          }
        })
      }
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  override def skipOriginalDFName: Boolean = false

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def modelType: ModelType = ProcessType

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |
      |""".stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |
      |set abc='''
      |{"name": "elena", "age": 57, "phone": 15552231521, "income": 433000, "label": 0}
      |{"name": "candy", "age": 67, "phone": 15552231521, "income": 1200, "label": 0}
      |{"name": "bob", "age": 57, "phone": 15252211521, "income": 89000, "label": 0}
      |{"name": "candy", "age": 25, "phone": 15552211522, "income": 36000, "label": 1}
      |{"name": "candy", "age": 31, "phone": 15552211521, "income": 300000, "label": 1}
      | {"name": "finn", "age": 23, "phone": 15552211521, "income": 238000, "label": 1}
      |''';
      |
      |load jsonStr.`abc` as table1;
      |run table1 as ColumnsAnalysis.`` where action='iv' and target='name' and positive='candy' as outputTable;
      |select colName from outputTable where statistic < 0.2 as t2;
      |""".stripMargin)


  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def etName: String = "__fe_columns_analysis_operator__"

  final val action: Param[String] =
    new Param[String](this, name = "action", doc = "")
  setDefault(action, "KS")

  final val target: Param[String] =
    new Param[String](this, name = "target", doc = "")
  setDefault(target, "null")

  final val positive: Param[String] =
    new Param[String](this, name = "positive", doc = "")
  setDefault(positive, "0.0")

  final val fields: StringArrayParam =
    new StringArrayParam(this, name = "fields", doc = "")
  setDefault(fields, Array[String]())

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_columns_analysis_operator__"),
      OperateType.SELECT,
      Option("select"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None =>
        List(TableAuthResult(granted = true, ""))
    }
  }

  private def IV(srcDf: DataFrame, featureCol: String, target: String): Double = {
    var ivvalue: Double = 0.0

    val discretizer = new QuantileDiscretizer()
      .setInputCol(featureCol)
      .setOutputCol("result")
      .setNumBuckets(10)//默认值取10，最好可以外部输入

    val result = discretizer.fit(srcDf).transform(srcDf)
    result.show()
    val pos_all_num = if(result.filter(col(target) ===1).count() <=0) 1 else result.filter(col(target) ===1).count()
    val neg_all_num = if(result.filter(col(target) ===0).count() <=0) 1 else result.filter(col(target) ===0).count()
    val bluk_num = result.selectExpr("result").collect().distinct.length

    var i = 0
    while( i < bluk_num){
      val bluk_group = result.filter(col("result") === i.toDouble)

      val good_group_num = bluk_group.filter(col(target) ===1).count()
      val bad_group_num = bluk_group.filter(col(target) ===0).count()

      val good_group = if(good_group_num != 0) good_group_num else 1
      val bad_group = if(bad_group_num != 0) bad_group_num else 1

      val iv = (good_group.toDouble/pos_all_num.toDouble - bad_group.toDouble/neg_all_num.toDouble) * math.log((good_group.toDouble/pos_all_num.toDouble)/(bad_group.toDouble/neg_all_num.toDouble))
      ivvalue = ivvalue + iv
      i += 1
    }

    ivvalue
  }

  private def PEARSON(srcDf: DataFrame, featureCol: String, targetCol:String): Double = {
    val df_real = srcDf.select(targetCol, featureCol).na.fill(0.0)
    val rdd_real = df_real.rdd.map(x=>(x(0).toString.toDouble ,x(1).toString.toDouble ))
    val label = rdd_real.map(x=>x._1 )
    val feature = rdd_real.map(x=>x._2 )

    val cor_pearson = Statistics.corr(feature, label,"pearson")
    cor_pearson
  }

}
