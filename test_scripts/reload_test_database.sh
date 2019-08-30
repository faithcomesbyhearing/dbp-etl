#!/bin/sh -v

mysql -uroot -p$MYSQL_PASSWD test_dbp < test_dbp.sql

