import re
from loguru import logger

# Master list of Data Engineering skills to detect
DE_SKILLS = [
    # Languages
    "python", "sql", "scala", "java", "bash", "r",
    # Big Data
    "apache spark", "spark", "hadoop", "hive", "kafka",
    "flink", "storm", "pig",
    # Databases
    "postgresql", "mysql", "mongodb", "cassandra", "redis",
    "elasticsearch", "dynamodb", "hbase",
    # Cloud
    "aws", "gcp", "azure", "s3", "ec2", "glue",
    "bigquery", "redshift", "snowflake", "databricks",
    "azure data factory", "dataflow",
    # Orchestration
    "airflow", "apache airflow", "prefect", "dagster", "luigi",
    # Transformation
    "dbt", "data build tool",
    # Streaming
    "kafka", "kinesis", "pub/sub", "rabbitmq",
    # DevOps
    "docker", "kubernetes", "terraform", "git", "github",
    "jenkins", "ci/cd",
    # Visualization
    "tableau", "power bi", "looker", "superset", "metabase",
    # Concepts
    "etl", "elt", "data warehouse", "data lake", "data lakehouse",
    "data pipeline", "data modeling", "data quality",
    "rest api", "graphql", "microservices",
]

def extract_skills(text: str) -> list:
    if not text:
        return []

    text_lower = text.lower()
    found_skills = []

    for skill in DE_SKILLS:
        # Use word boundary matching for accuracy
        pattern = r"\b" + re.escape(skill) + r"\b"
        if re.search(pattern, text_lower):
            # Store in clean title case
            found_skills.append(skill.title())

    # Remove duplicates while preserving order
    seen = set()
    unique_skills = []
    for skill in found_skills:
        if skill.lower() not in seen:
            seen.add(skill.lower())
            unique_skills.append(skill)

    logger.debug(f"Extracted {len(unique_skills)} skills")
    return unique_skills