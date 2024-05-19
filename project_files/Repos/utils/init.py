from databricks.sdk.runtime import *

source_path = "abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/adv-dse"
# working_path = "abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_B/zhastay_yeltay"
working_path = "dbfs:/mnt/adls_custom/data/Team_B/zhastay_yeltay"

catalog_name = "hive_metastore"

schema_bronze_name = "zhastay_yeltay_01_bronze"
schema_silver_name = "zhastay_yeltay_02_silver"
schema_gold_name = "zhastay_yeltay_03_gold"

bronze = f"{working_path}/{schema_bronze_name}"
silver = f"{working_path}/{schema_silver_name}"
gold = f"{working_path}/{schema_gold_name}"


def init_spark():
    sa_name = "dextestwesteurope"
    sas_token = dbutils.secrets.get("zhastay_yeltay", "sas_token")

    spark.conf.set(f"fs.azure.account.auth.type.{sa_name}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{sa_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{sa_name}.dfs.core.windows.net", sas_token)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")