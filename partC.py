import pyspark

from Bitcoin_Analysis_Helper import Helper_Methods


class BitcoinAnalysis():
    # Change this based on HDFS URI

    def __init__(self):
        pass

    HDFS = "hdfs://localhost::9000/Andria"

    context = pyspark.SparkContext().appName("Bitcoin_Analysis").setMaster("local")

    # Initialise

    # input URI for HDFS
    myTransactions = context.textFile(HDFS + "/Input/transactions.csv")
    vout = context.textFile(HDFS + "/Input/vout.csv")
    vin = context.textFile(HDFS + "/Input/vin.csv")

    # output URI for HDFS
    outputResults = (HDFS + "/Output/")

    # Cleaning
    cleaned_transactions = myTransactions.filter(Helper_Methods.clean_transactions).map(lambda x: x.split(','))
    cleaned_vout = vout.filter(Helper_Methods.clean_vout).map(lambda x: x.split(','))
    cleaned_vin = vin.filter(Helper_Methods.clean_vin).map(lambda x: x.split(','))

    # We are joining based on matching values of the vout field and the n field as this establishes whether the coins
    # have been respent or not
    in_join_uno = cleaned_vin.map(lambda x: (x[0], (x[2], "")))
    out_join_duo = cleaned_vout.map(lambda x: (x[0], (x[2], x[1])))
    inFtout = out_join_duo.join(in_join_uno)

    # here we are establishing if the coins have been respent or not and seperating based on the result

    smaller_transactions = inFtout.filter(Helper_Methods.filter_small)
    larger_transactions = inFtout.filter(Helper_Methods.filter_large)

    # s joining first join and transactions.csv based on hashes

    transactions_hash_join = cleaned_transactions.map(lambda ct: (ct[0], (ct[2], "")))
    smallerFttransactions = smaller_transactions.map(lambda st: (st[0], (st[1][0][0], st[1][0][1], st[1][1][0])))
    largerFttransactions = larger_transactions.map(lambda lt: (lt[0], (lt[1][0][0], lt[1][0][1], lt[1][1][0])))

    # joining

    smaller_joined_transactions = transactions_hash_join.join(smallerFttransactions)
    larger_joined_transactions = transactions_hash_join.join(largerFttransactions)

    # Grouping amount of BTC by year
    smallAmountsbtc = smaller_joined_transactions.map(
        lambda x: (Helper_Methods.group_transactions(x[1][0][0]), float(x[1][1][1]))).reduceByKey(lambda x, y: x + y)
    largeAmountsbtc = larger_joined_transactions.map(
        lambda x: (Helper_Methods.group_transactions(x[1][0][0]), float(x[1][1][1]))).reduceByKey(lambda x, y: x + y)

    # Grouping number of transactions by year
    smallAmountstra = smaller_joined_transactions.map(
        lambda x: (Helper_Methods.group_transactions(x[1][0][0]), 1)).reduceByKey(lambda x, y: x + y)
    largeAmountstra = larger_joined_transactions.map(
        lambda x: (Helper_Methods.group_transactions(x[1][0][0]), 1)).reduceByKey(lambda x, y: x + y)
    # total number of small and large transactions
    total = ["small:", smaller_joined_transactions.count(), "large:", larger_joined_transactions.count()]
    # writing in files
    smallAmountsbtc.saveAsTextFile(outputResults + "smallbtc")
    smallAmountstra.saveAsTextFile(outputResults + "smalltra")
    largeAmountsbtc.saveAsTextFile(outputResults + "largebtc")
    largeAmountstra.saveAsTextFile(outputResults + "largetra")

    # makes available to all nodes , so they have info on output
    context.parallelize(total).saveAsTextFile("totalAnalysis")


if __name__ == '__main__':
    BitcoinAnalysis.run()
