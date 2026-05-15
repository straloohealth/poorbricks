class DatabricksSecrets:
    scope: str
    key: str

    def __init__(self, scope: str, key: str):
        self.scope = scope
        self.key = key


MONGO_SECRET = DatabricksSecrets(scope="MONGO", key="mongodb")

# Postgres writer URI for the nightly export job that mirrors silver/gold
# tables to the team's `analytics` Postgres database in GKE. Set once via:
#   databricks secrets put-secret POSTGRES poorbricks-writer-uri --string-value \
#       "postgresql://poorbricks_writer:***@34.45.23.27:5432/analytics?sslmode=require"
# Rotated by the storage repo's bootstrap SQL (see analytics-database.md).
POSTGRES_WRITER_SECRET = DatabricksSecrets(scope="POSTGRES", key="poorbricks-writer-uri")
