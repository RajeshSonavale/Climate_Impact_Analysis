@echo off
REM Change to the script folder (makes relative paths work)
cd/d "C:\data_science\Projects\Climat_Impact_Dashboard\scripts"

REM Run the pipeline using python Launcher
py -3 "Full_Pipeline_Climate_Data_For_Automation.py"

REM Optional log
echo %DATE% %TIME% : Pipeline ran >>"pipeline_log.txt"