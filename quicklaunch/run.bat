@echo off
setlocal enabledelayedexpansion

:: =============================================================
::  Digital Asset Protection — Crawler Pipeline Launcher (Win)
::  Place: quicklaunch\run.bat
::
::  Resolves project root (one level up from quicklaunch\),
::  activates the virtual environment, and runs the pipeline.
::
::  Standalone mode 2 — Social (Reddit + Twitter)   → suspicious\
::  Standalone mode 3 — Stock  (Unsplash/Pexels/Pixabay) → assets\
:: =============================================================

:: ── Resolve project root ──────────────────────────────────────────────────────
pushd "%~dp0.."
set "PROJECT_ROOT=%CD%"

:: ── Activate virtual environment ──────────────────────────────────────────────
call :activate_venv
goto menu

:activate_venv
if exist "%PROJECT_ROOT%\venv\Scripts\activate.bat" (
    call "%PROJECT_ROOT%\venv\Scripts\activate.bat"
    echo [INFO] Virtual environment activated: venv\
    exit /b
)
if exist "%PROJECT_ROOT%\.venv\Scripts\activate.bat" (
    call "%PROJECT_ROOT%\.venv\Scripts\activate.bat"
    echo [INFO] Virtual environment activated: .venv\
    exit /b
)
echo [WARN] No virtual environment found at venv\ or .venv\
echo [WARN] Using system Python. Run option 5 to set up dependencies.
exit /b

:: ── Main menu ─────────────────────────────────────────────────────────────────
:menu
cls
echo ============================================================
echo  Digital Asset Protection - Crawler Pipeline Launcher
echo  Project: %PROJECT_ROOT%
echo ============================================================
echo.
echo  1) Run Full Pipeline (Regular mode)
echo  2) Run Standalone Scraper (Social or Stock)
echo  3) Run Connectivity Test
echo  4) Install / Update Dependencies (Setup)
echo  5) Clean up Output Folders (assets ^& suspicious)
echo  6) Exit
echo.
set /p choice="Select an option (1-6): "

if "%choice%"=="1" goto run_full
if "%choice%"=="2" goto run_standalone_menu
if "%choice%"=="3" goto run_test
if "%choice%"=="4" goto setup
if "%choice%"=="5" goto cleanup
if "%choice%"=="6" goto exit_script
goto menu

:: ── Standalone Menu ───────────────────────────────────────────────────────────
:run_standalone_menu
echo.
echo  Select standalone source:
echo    1) Social (Reddit + Twitter -^> suspicious\)
echo    2) Stock  (Unsplash / Pexels / Pixabay -^> assets\)
echo    3) Back to main menu
echo.
set /p schoice="  Selection (1-3): "

if "%schoice%"=="1" goto run_standalone_social
if "%schoice%"=="2" goto run_standalone_stock
if "%schoice%"=="3" goto menu
echo Invalid choice.
pause
goto run_standalone_menu

:: ── Full pipeline ─────────────────────────────────────────────────────────────
:run_full
echo.
echo  +-----------------------------------------------------------+
echo  ^|  FULL PIPELINE -- Crawl -^> Preprocess -^> API             ^|
echo  +-----------------------------------------------------------+
echo.
set src=social
set /p src="Select source (social/stock) [default: social]: "
set /p kws="Enter keywords/topics     (leave blank for .env defaults): "
set /p lim="Max images per source     (leave blank for .env default): "

call :make_logfile pipeline

set cmd=python main.py --logfile "!logfile!" --source "!src!"
if not "!kws!"=="" set cmd=!cmd! --keywords "!kws!"
if not "!lim!"=="" set cmd=!cmd! --limit "!lim!"

echo.
echo [INFO] Running: !cmd!
echo [INFO] Log file: !logfile!
!cmd!
pause
goto menu

:: ── Standalone: Social (Reddit + Twitter) ─────────────────────────────────────
:run_standalone_social
echo.
echo  +-----------------------------------------------------------+
echo  ^|  SOCIAL SCRAPER -- Reddit + Twitter                      ^|
echo  ^|  Output: suspicious\                                     ^|
echo  ^|  Files:  ^<sha256^>_reddit.jpg / ^<sha256^>_twitter.jpg   ^|
echo  +-----------------------------------------------------------+
echo.
echo  Select scrape mode:
echo    A) home -- keyword-based search (default)
echo    B) top  -- top subreddits + X accounts from targets.json
echo.
set /p smode="  Mode (A/B): "

if /i "!smode!"=="b" goto _social_top
if /i "!smode!"=="top" goto _social_top
goto _social_home

:_social_home
echo.
echo [HOME MODE] Keyword-based search across Reddit and Twitter/X.
echo.
set /p kws="Enter keywords         (leave blank for .env defaults): "
set /p out="Enter output folder    (leave blank for .\suspicious): "
set /p lim="Max images per source  (leave blank for .env default): "

call :make_logfile social_home

set cmd=python main.py --standalone --source social --mode home --logfile "!logfile!"
if not "!kws!"=="" set cmd=!cmd! --keywords "!kws!"
if not "!out!"=="" set cmd=!cmd! --output "!out!"
if not "!lim!"=="" set cmd=!cmd! --limit "!lim!"

echo.
echo [INFO] Running: !cmd!
echo [INFO] Log file: !logfile!
!cmd!
echo.
echo [DONE] Social home-mode scrape finished. Images saved to: suspicious\
pause
goto menu

:_social_top
echo.
echo [TOP MODE] Browse top subreddits and X accounts from targets.json.
echo.
set /p targets="Path to targets.json   (leave blank for .\targets.json): "
set /p out="Enter output folder    (leave blank for .\suspicious): "
set /p lim="Max images per source  (leave blank for .env default): "

call :make_logfile social_top

set cmd=python main.py --standalone --source social --mode top --logfile "!logfile!"
if not "!targets!"=="" set cmd=!cmd! --targets "!targets!"
if not "!out!"==""     set cmd=!cmd! --output "!out!"
if not "!lim!"==""     set cmd=!cmd! --limit "!lim!"

echo.
echo [INFO] Running: !cmd!
echo [INFO] Log file: !logfile!
!cmd!
echo.
echo [DONE] Social top-mode scrape finished. Images saved to: suspicious\
pause
goto menu

:: ── Standalone: Stock (Unsplash / Pexels / Pixabay) ──────────────────────────
:run_standalone_stock
echo.
echo  +-----------------------------------------------------------+
echo  ^|  STOCK SCRAPER -- Unsplash / Pexels / Pixabay            ^|
echo  ^|  Output: assets\                                         ^|
echo  ^|  Files:  ^<sha256^>_unsplash.jpg / _pexels / _pixabay     ^|
echo  ^|                                                           ^|
echo  ^|  Requires API keys in .env:                               ^|
echo  ^|    UNSPLASH_ACCESS_KEY, PEXELS_API_KEY, PIXABAY_API_KEY   ^|
echo  ^|  Sites without a key are skipped automatically.           ^|
echo  +-----------------------------------------------------------+
echo.
set /p kws="Enter keywords         (leave blank for .env defaults): "
set /p out="Enter output folder    (leave blank for .\assets): "
set /p lim="Max images per source  (leave blank for .env default): "

call :make_logfile stock

set cmd=python main.py --standalone --source stock --logfile "!logfile!"
if not "!kws!"=="" set cmd=!cmd! --keywords "!kws!"
if not "!out!"=="" set cmd=!cmd! --output "!out!"
if not "!lim!"=="" set cmd=!cmd! --limit "!lim!"

echo.
echo [INFO] Running: !cmd!
echo [INFO] Log file: !logfile!
!cmd!
echo.
echo [DONE] Stock scrape finished. Images saved to: assets\
pause
goto menu

:: ── Connectivity test ─────────────────────────────────────────────────────────
:run_test
echo.
call :make_logfile test
echo [INFO] Running connectivity tests...
echo [INFO] Log file: !logfile!
python main.py --test --logfile "!logfile!"
pause
goto menu

:: ── Setup ─────────────────────────────────────────────────────────────────────
:setup
echo.
if not exist "%PROJECT_ROOT%\venv\" (
    echo [INFO] Creating virtual environment at venv\ ...
    python -m venv "%PROJECT_ROOT%\venv"
    call "%PROJECT_ROOT%\venv\Scripts\activate.bat"
    echo [INFO] venv created and activated.
)
echo [INFO] Installing requirements...
python -m pip install --upgrade pip -q
python -m pip install -r requirements.txt
echo [INFO] Installing Playwright browsers...
python -m playwright install chromium
echo.
echo [DONE] Setup complete. You can now run the pipeline.
pause
goto menu

:cleanup
echo.
echo  +-----------------------------------------------------------+
echo  ^|  CLEANUP -- Digital Asset Protection                     ^|
echo  ^|  Target: assets\, suspicious\, and logs\                 ^|
echo  +-----------------------------------------------------------+
echo.
echo [WARN] This will delete ALL files and subfolders in:
echo        - %PROJECT_ROOT%\assets\
echo        - %PROJECT_ROOT%\suspicious\
echo        - %PROJECT_ROOT%\logs\
echo.
set /p confirm="Are you sure you want to proceed? (y/N): "
if /i "!confirm!"=="y" (
    echo [INFO] Cleaning assets\...
    if exist "%PROJECT_ROOT%\assets\" (
        del /s /q "%PROJECT_ROOT%\assets\*" >nul 2>&1
        for /d %%x in ("%PROJECT_ROOT%\assets\*") do rd /s /q "%%x" >nul 2>&1
    )
    echo [INFO] Cleaning suspicious\...
    if exist "%PROJECT_ROOT%\suspicious\" (
        del /s /q "%PROJECT_ROOT%\suspicious\*" >nul 2>&1
        for /d %%x in ("%PROJECT_ROOT%\suspicious\*") do rd /s /q "%%x" >nul 2>&1
    )
    echo [INFO] Cleaning logs\...
    if exist "%PROJECT_ROOT%\logs\" (
        del /s /q "%PROJECT_ROOT%\logs\*" >nul 2>&1
        for /d %%x in ("%PROJECT_ROOT%\logs\*") do rd /s /q "%%x" >nul 2>&1
    )
    echo [DONE] Cleanup complete.
) else (
    echo [INFO] Cleanup cancelled.
)
pause
goto menu

:exit_script
exit

:: ── Helper: timestamped log file path ─────────────────────────────────────────
:make_logfile
if not exist "%PROJECT_ROOT%\logs\" mkdir "%PROJECT_ROOT%\logs"
for /f "tokens=1-3 delims=/" %%a in ("%date%") do set ts=%%c%%a%%b
for /f "tokens=1-3 delims=:." %%a in ("%time: =0%") do set ts=!ts!_%%a%%b%%c
set logfile=%PROJECT_ROOT%\logs\!ts!_%1.log
exit /b
