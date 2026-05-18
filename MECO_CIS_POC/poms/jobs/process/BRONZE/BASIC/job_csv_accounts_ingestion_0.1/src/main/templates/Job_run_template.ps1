$fileDir = Split-Path -Parent $MyInvocation.MyCommand.Path
cd $fileDir
java '-Dtalend.component.manager.m2.repository=%cd%/../lib' '-Xms256M' '-Xmx1024M' '-Dfile.encoding=UTF-8' '-Dstdout.encoding=UTF-8' '--add-opens=java.base/sun.security.util=ALL-UNNAMED' '--add-opens=java.base/sun.security.ssl=ALL-UNNAMED' -cp 'null' meco_cis_poc.job_csv_accounts_ingestion_0_1.job_csv_accounts_ingestion $args
