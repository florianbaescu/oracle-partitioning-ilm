-- =============================================================================
-- Grant EXECUTE on DBMS_REDEFINITION to CMR schema
-- This allows the analyzer to check if tables support online redefinition
--
-- RUN AS: SYSDBA or user with GRANT privilege
-- =============================================================================

PROMPT Granting EXECUTE on DBMS_REDEFINITION to CMR...

GRANT EXECUTE ON SYS.DBMS_REDEFINITION TO CMR;

PROMPT
PROMPT ✓ Grant successful!
PROMPT
PROMPT This allows pck_dwh_table_migration_analyzer to:
PROMPT   - Check if tables support online redefinition
PROMPT   - Recommend ONLINE migration method for large tables
PROMPT   - Detect whether to use CONS_USE_PK or CONS_USE_ROWID
PROMPT
PROMPT If grant fails, the analyzer will still work but will always
PROMPT recommend CTAS method (which requires downtime).
PROMPT
