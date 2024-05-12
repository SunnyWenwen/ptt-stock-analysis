set path=C:\Users\User\iCloudDrive\share_data\log
set log_path=%path%\find_high_perf_auth_log.txt
set ymd=%date:~0,4%%date:~5,2%%date:~8,2%
set python_log_path=%path%\find_high_perf_auth_log

if not exist "%path%" (
	mkdir "%path%"
    echo path does not exist, creating...>> %log_path%
    
)

if not exist "%python_log_path%" (
	mkdir "%python_log_path%"
    echo python_log_path does not exist, creating...>> %log_path%
    
)



echo %ymd% %time%----Start %ymd% batch run >> %log_path%
echo %ymd% %time%----Start Activate Venv  >> %log_path%
cd D:\project\ptt_stock_analysis 
call D:\software\python\virtualenvs\ptt_stock_analysis\Scripts\activate.bat
D:
echo %ymd% %time%----Run backtest_all_target_article.py >> %log_path%
python .\backtest_all_target_article.py >> %python_log_path%\%ymd%.txt
echo %ymd% %time%----Run find_high_perf_auth.py >> %log_path%
python .\find_high_perf_auth.py >> %python_log_path%\%ymd%.txt

echo %ymd% %time%----Complete run Python  >> %log_path%
