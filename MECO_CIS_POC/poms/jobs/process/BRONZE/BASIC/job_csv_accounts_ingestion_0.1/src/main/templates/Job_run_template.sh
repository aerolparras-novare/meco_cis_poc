#!/bin/sh
cd `dirname $0`
ROOT_PATH=`pwd`
java -Dtalend.component.manager.m2.repository=$ROOT_PATH/../lib -Xms256M -Xmx1024M -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 --add-opens=java.base/sun.security.util=ALL-UNNAMED --add-opens=java.base/sun.security.ssl=ALL-UNNAMED -cp ${talend.job.sh.classpath} meco_cis_poc.job_csv_accounts_ingestion_0_1.job_csv_accounts_ingestion "$@"
