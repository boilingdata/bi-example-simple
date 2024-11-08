-- ----------------------------------------------------------------------
-- Highlight query and press Ctrl/Cmd + Enter to run query.
-- NOTE: You need to have monitoring database selected:
--       Data --> Cloud on left side menu
-- ----------------------------------------------------------------------
-- List boilingdata-demo bucket (dummy)
SELECT * FROM list('s3://boilingdata-demo/');
-- Get BoilingData settings
SELECT * FROM boilingdata;
-- Get all shared data sets from/to you
SELECT * FROM boilingshares;
