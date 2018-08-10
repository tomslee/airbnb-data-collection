echo "initializing database..."

psql -U airbnb airbnb < /sql/schema_current.sql

echo "initializing database succeeded."
