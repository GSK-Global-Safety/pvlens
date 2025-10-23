#!/bin/bash
#
# Create empty PVLens database
#

# Update with your local MySQL username and password
# Insure that you have created the pvlens database 
# prior to running this script:
#
# Example:
# --------
# $ mysql
# mysql> drop database pvlens; create database pvlens;
#
export DB_USER=dbuser
export DB_PASS=dbpass

echo "Creating empty PVLens database table structure"
mysql -u ${DB_USER} -p${DB_PASS} pvlens < create_pvlens_db_structure.sql
