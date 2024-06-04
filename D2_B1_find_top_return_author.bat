set my_path=C:\Users\User\iCloudDrive\share_data\log
set log_path=%my_path%\find_top_return_author_log.txt
set ymd=%date:~0,4%%date:~5,2%%date:~8,2%
set python_log_path=%my_path%\find_top_return_author_python_daily_log

if not exist "%my_path%" (
	mkdir "%my_path%"
    echo my_path does not exist, creating...>> %log_path%
    
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
echo %ymd% %time%----Run D2_P1_summary_target_article_return.py >> %log_path%
python -u .\D2_P1_summary_target_article_return.py | tee -a %python_log_path%\%ymd%.txt
echo %ymd% %time%----Run D2_P2_find_top_return_author.py >> %log_path%
python -u .\D2_P2_find_top_return_author.py | tee -a %python_log_path%\%ymd%.txt
echo %ymd% %time%----Run D2_P5_upload_date_to_google_document.py >> %log_path%
python -u .\D2_P5_upload_date_to_google_document.py | tee -a %python_log_path%\%ymd%.txt

echo %ymd% %time%----Complete run Python  >> %log_path%
