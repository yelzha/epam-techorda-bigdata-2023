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
        "pad": "1.0",
        "splines": "ortho",
        "nodesep": "0.60",
        "ranksep": "0.75",
        "fontname": "Sans-Serif",
        "fontcolor": "#2D3436",
    }

node_attrs = {
        "shape": "box",
        "style": "rounded",
        "fixedsize": "true",
        "width": "1.4",
        "height": "1.4",
        "labelloc": "b",
        "imagescale": "true",
        "fontname": "Sans-Serif",
        "fontcolor": "#2D3436",
    }

with Diagram("Validation Workflow", show=False, graph_attr=graph_attrs, node_attr=node_attrs):
    with Cluster("Azure Data Lake Storage Gen2"):
        a = Custom("addresses", "./custom/parquet.png")

    with Cluster("Databricks"):
        with Cluster("Bronze"):
            b_a = Custom("bronze_addresses", "./custom/delta_table.png")

        with Cluster("Silver"):
            s_a = Custom("silver_addresses", "./custom/delta_table.png")
            s_a_dlq = Custom("silver_addresses_DLQ", "./custom/delta_table.png")

            cleansing_1 = ROS(label="Transformation")
            cleansing_2 = ROS(label="Cleansing \n& Transformation")

            s_v1 = Developertools("Validation")
            s_v2 = Developertools("Validation")

            # DLQ
            s_v1 >> Edge(label="upsert", color="green") >> s_a
            s_v1 >> Edge(label="upsert", color="red") >> s_a_dlq
            s_a_dlq >> cleansing_2 >> s_v2
            s_v2 >> Edge(label="valid upsert", color='green') >> s_a
            s_v2 >> Edge(label="InValid merge \n& define processed=True\nDelete cleaned & validated", color='red') >> s_a_dlq

        with Cluster("Gold"):
            g_a = Custom("Gold Tables", "./custom/delta_table.png")

    a >> Edge(label="upsert", color="black") >> b_a >> cleansing_1 >> s_v1
    s_a >> Edge(label="overwrite", color="black") >> g_a

