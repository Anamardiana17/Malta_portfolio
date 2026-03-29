# GUI Runbook

## Run
streamlit run apps/gui_control_panel/app.py

## Purpose
Provide a safe operating/control interface above validated Malta_portfolio artifacts.

## Source policy
Use active pipeline outputs only:
- data_processed/management/
- data_processed/dashboard_export/

Do not use backups or snapshots as default GUI sources.
