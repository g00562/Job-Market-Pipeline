import re
from loguru import logger

DE_SKILLS = [
    "python", "sql", "scala", "java", "bash",
    "apache spark", "spark", "hadoop", "hive", "kafka",
    "flink", "pig",
    "postgresql", "mysql", "mongodb", "cassandra", "redis",
    "elasticsearch", "dynamodb",
    "aws", "gcp", "azure", "s3", "ec2", "glue",
    "bigquery", "redshift", "snowflake", "databricks",
    "azure data factory", "dataflow",
    "airflow", "apache airflow", "prefect", "dagster",
    "dbt", "data build tool",
    "kafka", "kinesis", "rabbitmq",
    "docker", "kubernetes", "terraform", "git", "github",
    "jenkins", "ci/cd",
    "tableau", "power bi", "looker", "superset",
    "etl", "elt", "data warehouse", "data lake",
    "data pipeline", "data modeling", "data quality",
    "rest api", "microservices",
]

def extract_skills(text: str) -> list:
    if not text:
        return []
    text_lower   = text.lower()
    found_skills = []
    for skill in DE_SKILLS:
        pattern = r"\b" + re.escape(skill) + r"\b"
        if re.search(pattern, text_lower):
            found_skills.append(skill.title())
    seen   = set()
    unique = []
    for skill in found_skills:
        if skill.lower() not in seen:
            seen.add(skill.lower())
            unique.append(skill)
    return unique