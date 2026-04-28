import re
from loguru import logger

DE_SKILLS = [
    "python", "sql", "scala", "java", "bash",
    "apache spark", "spark", "hadoop", "hive",
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
    """
    Extract data engineering skills from text.
    Returns deduplicated list of skills with consistent capitalization.
    """
    if not text:
        return []
    
    text_lower = text.lower()
    found_skills = []
    
    # Use a set to track found skills (case-insensitive)
    seen_skills = set()
    
    for skill in DE_SKILLS:
        if skill.lower() not in seen_skills:
            pattern = r"\b" + re.escape(skill) + r"\b"
            if re.search(pattern, text_lower):
                found_skills.append(skill)
                seen_skills.add(skill.lower())
    
    # Return with consistent title casing (first word title case)
    return [skill.title() for skill in found_skills]