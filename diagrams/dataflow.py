from diagrams import Diagram, Cluster, Edge

from diagrams.programming.flowchart import Decision, Action

from diagrams.azure.database import DataLake
from diagrams.azure.storage import DataLakeStorage

from diagrams.azure.analytics import Databricks as ADatabricks
from diagrams.azure.integration import EventGridTopics # LINKS CLEAN
from diagrams.azure.general import Templates # JSON
from diagrams.azure.general import Developertools # ScrewDriver
from diagrams.azure.database import SQL # SQL

from diagrams.onprem.analytics import Databricks, Spark # SPARK

from diagrams.alibabacloud.compute import ROS

from diagrams.custom import Custom

graph_attrs = {
        "pad": "2.0",
        "splines": "ortho",
        "nodesep": "0.20",
        "ranksep": "0.40",
        "fontname": "Sans-Serif",
        "fontsize": "15",
        "fontcolor": "#2D3436",
    }

with Diagram("Epam Big Data Capstone Project", curvestyle='ortho', show=False, graph_attr=graph_attrs):
    with Cluster("Azure Data Lake Storage Gen2"):
        a = Custom("addresses", "./custom/parquet.png")
        c = Custom("customers", "./custom/parquet.png")
        i = Custom("items", "./custom/parquet.png")
        od = Custom("orderDetails", "./custom/parquet.png")
        o = Custom("orders", "./custom/parquet.png")

    with Cluster("Databricks"):
        with Cluster("Bronze"):
            b_a = Custom("bronze_addresses", "./custom/delta_table.png")
            b_c = Custom("bronze_customers", "./custom/delta_table.png")
            b_i = Custom("bronze_items", "./custom/delta_table.png")
            b_od = Custom("bronze_orderDetails", "./custom/delta_table.png")
            b_o = Custom("bronze_orders", "./custom/delta_table.png")

        with Cluster("Silver"):
            s_a = Custom("silver_addresses", "./custom/delta_table.png")
            s_v = Decision("validation")
            s_a_dlq = Custom("silver_addresses_DLQ", "./custom/delta_table.png")

            s_c = Custom("silver_customers", "./custom/delta_table.png")
            s_i = Custom("silver_items", "./custom/delta_table.png")
            s_od = Custom("silver_order_details", "./custom/delta_table.png")
            s_o = Custom("silver_orders", "./custom/delta_table.png")

            s_mc = Custom("silver_metropolitan_cities", "./custom/delta_table.png")
            s_scc = Custom("silver_state_capital_cities", "./custom/delta_table.png")
            s_ae = Custom("silver_addressline_enriched", "./custom/delta_table.png")

            # DLQ
            s_v >> Edge(label="upsert", color="green") >> s_a
            s_v >> Edge(label="upsert", color="red") >> s_a_dlq
            s_a_dlq >> ROS(label="Cleansing") >> Edge(label="upsert") >> s_a
            s_a >> ROS(label="Enriching") >> Edge(label="append") >> s_ae

        with Cluster("Gold"):
            with Cluster('g1') as g1:
                q02 = Custom("02_total_orders_by_address", "./custom/delta_table.png")

                q07 = Custom("07_average_time_and_order_count_by_city", "./custom/delta_table.png")
                q08 = Custom("08_order_delays_by_order_type", "./custom/delta_table.png")
                q10 = Custom("10_complete_order_customer_by_id", "./custom/delta_table.png")
                q11 = Custom("11_monthly_orders_by_state", "./custom/delta_table.png")

                q02 - q07 - q08 - q10 - q11
                g1 = [q02, q07, q08, q10, q11]

            with Cluster('g2') as g2:
                q01 = Custom("01_cities_by_vip_customer_count", "./custom/delta_table.png")
                q05 = Custom("05_affiliate_by_weekly_orders", "./custom/delta_table.png")
                q06 = Custom("06_total_vip_customers_coupon", "./custom/delta_table.png")
                q09 = Custom("09_total_orders_by_state_capital", "./custom/delta_table.png")

                q01 - q05 - q06 - q09
                g2 = [q01, q05, q06, q09]

            with Cluster('g3') as g3:
                q03 = Custom("03_total_customers_by_type_status", "./custom/delta_table.png")
                q04 = Custom("04_customer_kinds_types_list", "./custom/delta_table.png")

                q12 = Custom("12_affiliate_customer_orders", "./custom/delta_table.png")
                q13 = Custom("13_customer_total_order_delivered", "./custom/delta_table.png")
                q14 = Custom("14_daily_orders_by_city", "./custom/delta_table.png")

                q03 - q04 - q12 - q13 - q14

                g3 = [q03, q04, q12, q13, q14]
            # q03 - q04 - q12 - q13
            # q09 - q10
            # q11 - q14
            # q01 - q05 - q06 - q07 - q08
            # q02

    a \
        >> Edge(label="upsert", color="black") \
        >> b_a \
        >> s_v


    c \
        >> Edge(label="upsert", color="black") \
        >> b_c \
        >> Edge(label="upsert", color="black") \
        >> s_c


    i \
        >> Edge(label="upsert", color="black") \
        >> b_i \
        >> Edge(label="upsert", color="black") \
        >> s_i


    od \
        >> Edge(label="upsert", color="black") \
        >> b_od \
        >> Edge(label="upsert", color="black") \
        >> s_od


    o \
        >> Edge(label="upsert", color="black") \
        >> b_o \
        >> Edge(label="upsert", color="black") \
        >> s_o

    # GOLD
    s_a >> [q01, q02, q05, q06, q07, q08, q09, q10, q11, q14]
    s_o >> [q01, q02, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14]
    s_c >> [q01, q03, q04, q05, q06, q10, q12, q13]
    s_mc >> [q06, q08]
    s_scc >> [q09]
    s_i >> [q10]
    s_od >> [q10]


    s_a >> q01
    s_o >> q01
    s_c >> q01

    s_a >> q02
    s_o >> q02

    s_c >> q03

    s_c >> q04

    s_a >> q05
    s_o >> q05
    s_c >> q05

    s_a >> q06
    s_o >> q06
    s_c >> q06
    s_mc >> q06

    s_a >> q07
    s_o >> q07

    s_a >> q08
    s_o >> q08
    s_mc >> q08

    s_a >> q09
    s_o >> q09
    s_scc >> q09

    s_a >> q10
    s_c >> q10
    s_i >> q10
    s_od >> q10
    s_o >> q10

    s_a >> q11
    s_o >> q11

    s_c >> q12
    s_o >> q12

    s_c >> q13
    s_o >> q13

    s_o >> q14
    s_a >> q14
