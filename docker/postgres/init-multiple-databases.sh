#!/bin/bash
set -e
set -u

function create_user_and_database() {
    local database=$1
    local username=$2
    local password=$3
    echo "Creating user '$username' and database '$database'"

    # 先检查用户是否存在
    USER_EXISTS=$(psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$username'")
    if [ "$USER_EXISTS" != "1" ]; then
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
            CREATE USER $username WITH PASSWORD '$password';
EOSQL
        echo "  User '$username' created successfully"
    else
        echo "  User '$username' already exists, skipping"
    fi

    # 再检查数据库是否存在
    DB_EXISTS=$(psql -tAc "SELECT 1 FROM pg_database WHERE datname='$database'")
    if [ "$DB_EXISTS" != "1" ]; then
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
            CREATE DATABASE $database;
            GRANT ALL PRIVILEGES ON DATABASE $database TO $username;
EOSQL
        echo "  Database '$database' created successfully"
    else
        echo "  Database '$database' already exists, skipping"
    fi
}

# 然后调用你的函数
create_user_and_database $METADATA_DATABASE_NAME $METADATA_DATABASE_USERNAME $METADATA_DATABASE_PASSWORD
create_user_and_database $CELERY_BACKEND_NAME $CELERY_BACKEND_USERNAME $CELERY_BACKEND_PASSWORD
create_user_and_database $ELT_DATABASE_NAME $ELT_DATABASE_USERNAME $ELT_DATABASE_PASSWORD

echo "All databases and users created successfully"