import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.category.DefaultCategoryDataset

import javax.swing.JFrame


object Main {
  case class Transaction(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)

  case class CustomerTransactionFrequency(customer_id: Int, transaction_frequency: Long)

  case class TransactionPattern(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate
    val sc = spark.sparkContext
    val transactionsRDD = loadDataFromCSV(sc, "input.csv")
    val dataFrame = exploreDataset(spark, transactionsRDD)
    calculateBasicStatistics(transactionsRDD)
    customerTransactionFrequency(spark, transactionsRDD)
    groupByTransactionDate(dataFrame, "daily")
    val depositsAndDrawlsByDateDataFrame = getDepositsAndWithdrawalsByDate(dataFrame)
    plotTransactionTrends(depositsAndDrawlsByDateDataFrame)
    val customerSegments: DataFrame = customerSegmentation(dataFrame)
    customerSegments.show()
    val avgTransactionAmountBySegment: DataFrame = calculateAvgTransactionAmount(dataFrame, customerSegments)
    avgTransactionAmountBySegment.show()
    val transactionPatterns: DataFrame = identifyTransactionPatterns(dataFrame)
    transactionPatterns.show()
    visualizeTransactionPatterns(dataFrame, transactionPatterns)
  }

  def loadDataFromCSV(sc: SparkContext, filename: String): RDD[Transaction] = {
    val dataRDD = sc.textFile(filename)
    val filteredRDD = dataRDD.zipWithIndex().filter { case (_, index) => index > 0 }.keys

    filteredRDD.flatMap { line =>
      val fields = line.split(",")
      try {
        val customerId = fields(0).toInt
        val transactionDate = fields(1)
        val transactionType = fields(2)
        val amount = fields(3).toDouble

        Some(Transaction(customerId, transactionDate, transactionType, amount))
      } catch {
        case _: NumberFormatException => None
        case _: ArrayIndexOutOfBoundsException => None
      }
    }
  }

  private def exploreDataset(sparkSession: SparkSession, data: RDD[Transaction]): DataFrame = {
    val dataDF = sparkSession.createDataFrame(data)
    dataDF.printSchema()
    dataDF.show()
    dataDF
  }

  def calculateBasicStatistics(data: RDD[Transaction]): Unit = {
    val totalDeposits = data.filter(_.transaction_type == "deposit").map(_.amount).sum()
    val totalWithdrawals = data.filter(_.transaction_type == "withdrawal").map(_.amount).sum()
    val averageTransactionAmount = data.map(_.amount).mean()

    println(s"Total Deposits: $totalDeposits")
    println(s"Total Withdrawals: $totalWithdrawals")
    println(s"Average Transaction Amount: $averageTransactionAmount")
  }

  def customerTransactionFrequency(sparkSession: SparkSession, data: RDD[Transaction]): DataFrame = {
    val transactionCounts = data.map(transaction => (transaction.customer_id, 1))
      .reduceByKey(_ + _)

    transactionCounts.map { case (customer_id, frequency) =>
      CustomerTransactionFrequency(customer_id, frequency)
    }
    val dataDF = sparkSession.createDataFrame(transactionCounts)
    dataDF.show()
    dataDF
  }

  def groupByTransactionDate(data: DataFrame, timeUnit: String): DataFrame = {
    val dateFormat = if (timeUnit == "daily") "yyyy-MM-dd" else "yyyy-MM"
    val aggregatedData = data.groupBy(date_format(col("transaction_date"), dateFormat).alias("transaction_date"))
      .agg(functions.sum("amount").alias("total_amount"))
    aggregatedData.show()
    aggregatedData
  }

  def getDepositsAndWithdrawalsByDate(data: DataFrame): DataFrame = {
    val depositsAndWithdrawals = data
      .groupBy("transaction_date")
      .agg(
        functions.sum(when(col("transaction_type") === "deposit", col("amount")).otherwise(0.0)).alias("total_deposit"),
        functions.sum(when(col("transaction_type") === "withdrawal", col("amount")).otherwise(0.0)).alias("total_withdrawal")
      )
      .orderBy("transaction_date")

    depositsAndWithdrawals.show()
    depositsAndWithdrawals
  }

  def plotTransactionTrends(data: DataFrame): Unit = {
    val dataset = new DefaultCategoryDataset()
    data.collect().foreach { row =>
      val date = row.getAs[String]("transaction_date")
      val deposit = row.getAs[Double]("total_deposit")
      val withdrawal = row.getAs[Double]("total_withdrawal")
      dataset.addValue(deposit, "Deposits", date)
      dataset.addValue(withdrawal, "Withdrawals", date)
    }
    val chart = ChartFactory.createLineChart(
      "Trends of Deposits and Withdrawals Over Time",
      "Date",
      "Amount",
      dataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )
    val frame = new JFrame("Transaction Trends")
    frame.add(new ChartPanel(chart))
    frame.pack()
    frame.setVisible(true)
  }

  def customerSegmentation(data: DataFrame): DataFrame = {
    val highValueThreshold = 600.0
    val frequentTransactorThreshold = 2

    val customerSummary = data.groupBy("customer_id")
      .agg(
        functions.sum("amount").alias("total_amount"),
        count("transaction_type").alias("transaction_count")
      )

    val customerSegments = customerSummary.select("customer_id", "total_amount", "transaction_count")
      .withColumn("segment",
        when(col("total_amount") > highValueThreshold, "High-Value")
          .when(col("transaction_count") > frequentTransactorThreshold, "Frequent Transactor")
          .otherwise("Inactive")
      )
      .select("customer_id", "segment")

    customerSegments
  }

  def calculateAvgTransactionAmount(data: DataFrame, segments: DataFrame): DataFrame = {
    val joinedData = data.join(segments, Seq("customer_id"))

    val avgTransactionAmountBySegment = joinedData.groupBy("segment")
      .agg(
        avg("amount").alias("avg_transaction_amount")
      )

    avgTransactionAmountBySegment
  }

  def identifyTransactionPatterns(data: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
    val transactionPatterns = data.withColumn("previous_amount", lag("amount", 1).over(windowSpec))
      .withColumn("next_amount", lead("amount", 1).over(windowSpec))
    val identifiedPatterns = transactionPatterns.filter(
      (col("transaction_type") === "deposit" && col("amount") >= 500) &&
        (col("next_amount").isNotNull && col("next_amount") > 500) &&
        (col("next_amount") > col("amount"))
    )
    val finalResult = identifiedPatterns.select("customer_id", "transaction_date", "transaction_type", "amount")
    finalResult
  }

  def visualizeTransactionPatterns(data: DataFrame, finalResult: DataFrame): Unit = {

    val customerIDs = finalResult.select("customer_id").collect().map(_.getInt(0))
    val transactionDates = finalResult.select("transaction_date").collect().map(_.getString(0))
    val transactionAmounts = finalResult.select("amount").collect().map(_.getDouble(0))

    val dataset = new DefaultCategoryDataset()
    for (i <- customerIDs.indices) {
      dataset.addValue(transactionAmounts(i), s"Customer ${customerIDs(i)}", transactionDates(i))
    }

    val chart = ChartFactory.createBarChart(
      "Transaction Amounts over Time for Identified Patterns",
      "Transaction Date",
      "Transaction Amount",
      dataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )

    val chartPanel = new ChartPanel(chart)
    chartPanel.setPreferredSize(new java.awt.Dimension(800, 600))

    val frame = new javax.swing.JFrame("Transaction Patterns")
    frame.setContentPane(chartPanel)
    frame.pack()
    frame.setVisible(true)
  }


}