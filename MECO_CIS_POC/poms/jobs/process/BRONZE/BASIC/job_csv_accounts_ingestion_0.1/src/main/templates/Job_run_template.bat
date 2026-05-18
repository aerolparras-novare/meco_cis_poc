%~d0
cd %~dp0
java -Dtalend.component.manager.m2.repository=../lib -Xms256M -Xmx1024M -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 --add-opens=java.base/sun.security.util=ALL-UNNAMED --add-opens=java.base/sun.security.ssl=ALL-UNNAMED -cp ${talend.job.bat.classpath} meco_cis_poc.job_csv_accounts_ingestion_0_1.job_csv_accounts_ingestion %*
