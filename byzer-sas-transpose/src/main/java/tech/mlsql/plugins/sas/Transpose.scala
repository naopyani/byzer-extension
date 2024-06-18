package tech.mlsql.plugins.sas

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import tech.mlsql.common.form._
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.util.{Failure, Success, Try}

/**
 * 9/5/24 naopyani(naopyani@gmail.com)
 */
class Transpose(override val uid: String) extends SQLAlg with Functions with MllibFunctions with BaseClassification with ETAuth {
  def this() = this(BaseParams.randomUID())

  private def colTrans(df: DataFrame, transCols: Array[String], sourceCol: String, extendCol: String, limitNum: String): DataFrame = {
    // 转置列
    val limitNumInt = Try(limitNum.toInt) match {
      case Success(value) => value
      case Failure(exception) => throw new RuntimeException(s"Could not convert '$limitNum' to Int: ${exception.getMessage}")
    }
    // 增加转置限制1000行
    require(limitNumInt <= 1000, "The limitNum parameter is limited to 1000 or less!")
    val transDF = df.limit(limitNumInt)
    val rowCount = transDF.count().toInt
    val transColDF = transDF.select(transCols.head, transCols.tail: _*)
    val transColArray = transColDF.collect()
    val sourceRow = Row(transCols: _*)
    val transColList = sourceRow :: transColArray.toList
    val trans = transColList.map(row =>
      Option(row).map(_.toSeq.map { value =>
        Option(value).map(_.toString).getOrElse("")
      }).getOrElse(Seq.fill(row.length)(""))).transpose
    // 遍历 transposedData 并将其转换为 Row 对象
    val rowList: List[Row] = trans.map(listToRow)
    val columnNames = Seq(sourceCol) ++ (1 to rowCount).map(i => s"$extendCol$i")
    val schema = StructType(columnNames.map(colName => StructField(colName, StringType, nullable = true)))
    val spark = df.sparkSession
    val transResultDF = spark.createDataFrame(spark.sparkContext.parallelize(rowList), schema)
    transResultDF
  }

  // 定义一个函数来将列表转换为 Row 对象
  private def listToRow(data: Seq[Any]): Row = Row.fromSeq(data)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val _method = params.getOrElse(method.name, "")
    require(_method.nonEmpty, "The param for the method is required!")
    _method match {
      case Transpose.COL_TRANS =>
        val transColsStr = params.getOrElse(transCols.name, "")
        val _transCols = transColsStr.split(",")
        require(_transCols.nonEmpty, "The param for the transCols is required!")
        val _sourceCol = params.getOrElse(sourceCOL.name, "")
        require(_sourceCol.nonEmpty, "The param for the sourceCOL is required!")
        val _extendCol = params.getOrElse(extendCol.name, "")
        require(_extendCol.nonEmpty, "The param for the extendCol is required!")
        val _limitNum = params.getOrElse(limitNum.name, "")
        require(_limitNum.nonEmpty, "The param for the limitNum is required!")
        val res = colTrans(df, _transCols, _sourceCol, _extendCol, _limitNum)
        res
    }

  }

  private val method: Param[String] = new Param[String](this, "method", FormParams.toJson(
    Select(
      name = "method",
      values = List(),
      extra = Extra(
        doc = "",
        label = "",
        options = Map(
        )), valueProvider = Option(() => {
        List(
          KV(Some("method"), Some(Transpose.COL_TRANS)),
        )
      })
    )
  ))

  private final val transCols: Param[String] = new Param[String](parent = this
    , name = Transpose.TRANS_COLS
    , doc = FormParams.toJson(Text(
      name = Transpose.TRANS_COLS
      , value = ""
      , extra = Extra(
        doc = "The transpose columns for transposing data"
        , label = Transpose.TRANS_COLS
        , options = Map(
        )
      )
    )
    )
  )

  private final val sourceCOL: Param[String] = new Param[String](parent = this
    , name = Transpose.SOURCE_COL
    , doc = FormParams.toJson(Text(
      name = Transpose.SOURCE_COL
      , value = ""
      , extra = Extra(
        doc = "The source columns for transposing data"
        , label = Transpose.SOURCE_COL
        , options = Map(
        )
      )
    )
    )
  )

  private final val extendCol: Param[String] = new Param[String](parent = this
    , name = Transpose.EXTEND_COL
    , doc = FormParams.toJson(Text(
      name = Transpose.EXTEND_COL
      , value = ""
      , extra = Extra(
        doc = "The extend columns for transposing data"
        , label = Transpose.EXTEND_COL
        , options = Map(
        )
      )
    )
    )
  )

  private final val limitNum: Param[String] = new Param[String](parent = this
    , name = Transpose.LIMIT_NUM
    , doc = FormParams.toJson(Text(
      name = Transpose.LIMIT_NUM
      , value = ""
      , extra = Extra(
        doc = "The extend columns for transposing data"
        , label = Transpose.LIMIT_NUM
        , options = Map(
        )
      )
    )
    )
  )

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is not supported in SAS Transpose (ColTrans) ET")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is not supported in SAS Transpose (ColTrans) ET")
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = ???

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |
      |set jsonStr='''
      |{"subject":"数学","name":"张三","score":88},
      |{"subject":"语文","name":"张三","score":92}
      |{"subject":"英语","name":"张三","score":77}
      |{"subject":"数学","name":"王五","score":65}
      |{"subject":"语文","name":"王五","score":87}
      |{"subject":"英语","name":"王五","score":90}
      |{"subject":"数学","name":"李雷","score":67}
      |{"subject":"语文","name":"李雷","score":33}
      |{"subject":"英语","name":"李雷","score":24}
      |{"subject":"数学","name":"宫九","score":77}
      |{"subject":"语文","name":"宫九","score":87}
      |{"subject":"英语","name":"宫九","score":90}
      |''';
      |load jsonStr.`jsonStr` as data;
      |run data as SASTranspose.`` where method='colTrans'
      |and transCols='name,subject'
      |and sourceCol='源'
      |and extendCol='列'
      |and limitNum='5'
      |as data1;
      |
      |;
      """.stripMargin)

}

object Transpose {
  //  val METHOD = "method" // colTrans 列转行,
  private val COL_TRANS = "colTrans" // 列转行
  private val TRANS_COLS = "transCols" // 需要转换的列是哪些
  private val SOURCE_COL = "sourceCol" // 转换后的第一列名称，列转行时此列数据为原列名数据。
  private val EXTEND_COL = "extendCol" // 转换后第二列及以后列名统称，列转行时需要多少列自增加1的方式展开。
  private val LIMIT_NUM = "limitNum" // 选取多少行数据做转换
}
