# be in same directory as .env, schema.sql and this connect.sh
# run: bash reset.sh

source .env
export PGPASSWORD=$DATABASE_PASSWORD
psql -h $DATABASE_IP -d $DATABASE_NAME -U $DATABASE_USERNAME -f schema.sql