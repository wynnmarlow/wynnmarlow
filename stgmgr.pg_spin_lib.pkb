DROP PACKAGE BODY STGMGR.PG_SPIN_LIB;

CREATE OR REPLACE PACKAGE BODY STGMGR.PG_SPIN_LIB
 /*#############################################################################################
 #   Script Name : STGMGR.PG_SPIN_LIB.sql
 #
 #   Description : Contains common objects utilized by various processes.
 #
 #   Author       : Andy Fritz
 #   Date created : 10/14/2019
 #   Restartable  : Yes
 #
 #   Modified on     Modified by         Description
 #   05/21/2021      Eugene Levitan      Modified f_get_param -  set the size of lvc_return_value to 1000
 #
 #############################################################################################*/
 AS
    FUNCTION f_time RETURN VARCHAR2
    /*#############################################################################################
    # Function Name  : f_time
    #
    # Description    : Returns '*  INFO: ( date/time ): ' string
    #
    # Input          : NA
    # Output         : log line
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by         Description
    #
    #############################################################################################*/
    IS
    BEGIN
        RETURN '*  INFO: ('|| TO_CHAR(SYSDATE,'MM/DD/RR-HH24:MI:SS')|| '): ';
        EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (SQLERRM(SQLCODE));
            RAISE_APPLICATION_ERROR (-20001,SQLERRM(SQLCODE));
    END;

    FUNCTION f_get_batch_date RETURN DATE PARALLEL_ENABLE
    /*#############################################################################################
    # Function Name  : f_get_batch_date
    # Description    : This function will return the batch date from the UDT_PARAMETER table.
    #
    # Input          : NA
    # Output         : UDT_PARAMETER Batch Date
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by         Description
    #
    #############################################################################################*/
    AS
        ldt_batchdate        DATE;
    BEGIN
        SELECT TO_DATE(PARAM_VALUE,'MM/DD/YYYY')
          INTO ldt_batchdate
          FROM scpomgr.udt_parameter
         WHERE param_name = 'BATCHDATE';

        RETURN ldt_batchdate;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE ('There is no record for PARAM_NAME=BATCHDATE within SCPOMGR.UDT_PARAMETER table, please add this record before proceeding... Processs Aborted');
            RAISE_APPLICATION_ERROR (-20002,'Within SCPOMGR.UDT_PARAMETER table, there is no record for PARAM_NAME=BATCHDATE.  Please add this record before proceeding... Processs Aborted');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (SQLERRM(SQLCODE));
            RAISE_APPLICATION_ERROR (-20002,SQLERRM(SQLCODE)||' Process Aborted');
    END f_get_batch_date;

    FUNCTION f_get_dpd (pvc_ignore_null VARCHAR2 DEFAULT 'N') RETURN DATE PARALLEL_ENABLE
    /*#############################################################################################
    # Function Name  : f_get_dpd
    # Description    : This function will get the DPD date from the DFU table
    #
    # Input          : pvc_ignore_null - 'Y' don't fail if no data in DFU
    # Output         : Demand Post Date
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by         Description
    #
    #############################################################################################*/
    AS
        ldt_dpddate        DATE;
    BEGIN
        BEGIN
          SELECT dmdpostdate
            INTO ldt_dpddate
            FROM scpomgr.dfu
           WHERE ROWNUM = 1;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            ldt_dpddate := TRUNC(pg_spin_lib.f_get_batch_date,'MM');
        END;

        RETURN ldt_dpddate;
        EXCEPTION
        WHEN OTHERS THEN
          IF pvc_ignore_null = 'N' THEN
            DBMS_OUTPUT.PUT_LINE (SQLERRM(SQLCODE));
            RAISE_APPLICATION_ERROR (-20003,SQLERRM(SQLCODE)||' f_get_dpd process aborted');
          ELSE
            RETURN NULL;
          END IF;

    END f_get_dpd;


    FUNCTION f_get_param (pvc_param_name_in IN VARCHAR2,
                          pvc_jobname_in    IN VARCHAR2 DEFAULT NULL,
                          pn_seqnum_in      IN NUMBER DEFAULT NULL) RETURN VARCHAR2 PARALLEL_ENABLE
    /*#############################################################################################
    # Function Name  : f_get_param
    # Description    : This function will accept param_name and return param_value from UDT_PARAMETER table.
    #                  This function returns data type of VARCHAR2.
    #
    # Input          : pvc_param_name_in (example SRE_JOB_RETENTION, LOG_RETENTION),
    #                  pvc_jobname_in - optional, job name for logging
    #                  pn_seqnum_in - optional, job sequence number for logging
    #
    # Output         : UDT_PARAMETER.PARAM_VALUE
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on       Modified by         Description
    # 05/21/2021        Eugene Levitan      Increased the size of lvc_return_value to 1000
    #############################################################################################*/
    IS
        lvc_jobname         VARCHAR2(100) := 'STGMGR.pg_spin_lib.f_get_param';
        ln_seqnum           NUMBER;
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(300);
        lvc_return_value    VARCHAR2(1000);
        le_exception        EXCEPTION;
    BEGIN
        IF (pvc_param_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started getting param_value for param_name='||pvc_param_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        SELECT PARAM_VALUE INTO lvc_return_value
          FROM SCPOMGR.UDT_PARAMETER
         WHERE param_name = pvc_param_name_in;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed getting param_value for param_name='||pvc_param_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        RETURN lvc_return_value;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('f_get_param requires a parameter name. Aborting....');
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'f_get_param requires a parameter name.  Aborting..',1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR( -20004, 'f_get_param requires a parameter name.  Aborting...');
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE ('f_get_param: Parameter '||pvc_param_name_in|| ' may not be available in UDT_PARAMETER table, please check.  Aborting...');
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'Parameter '||pvc_param_name_in||' may not be available in UDT_PARAMETER table, please check.  Aborting...',1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR( -20004, 'f_get_param: Parameter '||pvc_param_name_in|| ' may not be available in UDT_PARAMETER table, please check.  Aborting...');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_step||' : '|| SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'f_get_param failed with '||SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR( -20004, lvc_step||' : '|| SQLERRM(SQLCODE));
    END f_get_param;


    PROCEDURE p_set_param (pvc_param_name_in IN VARCHAR2,
                           pvc_param_group_in IN VARCHAR2,
                           pvc_param_value_in IN VARCHAR2,
                           pvc_param_descr_in IN VARCHAR2,
                           pvc_jobname_in    IN VARCHAR2 DEFAULT NULL,
                           pn_seqnum_in      IN NUMBER DEFAULT NULL)
    /*#############################################################################################
    # Procedure Name : p_set_param
    # Description    : Updates or inserts parameter values into UDT_PARAMETER table.
    #
    # Input          : pvc_param_name_in - parameter name ie. SRE_JOB_RETENTION
    #                  pvc_param_group_in - parameter group ie. SRE_JOBS
    #                  pvc_param_value_in - parameter value ie. 90
    #                  pvc_param_descr_in - optional, parameter description
    #                  pn_seqnum_in - optional, job sequence number for logging
    #                  pvc_jobname_in - optional, job name for logging
    #
    # Output         : NA
    # Author         : Andy Fritz
    # Date created   : 04/01/2019
    # Restartable    : Yes
    #
    # Modified on       Modified by         Description
    #
    #############################################################################################*/
    IS
        lvc_jobname         VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_set_param';
        ln_seqnum           NUMBER;
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(300);
        lvc_return_value    VARCHAR2(100);
        ln_record_count     NUMBER;
        le_exception        EXCEPTION;
    BEGIN
        IF (TRIM(pvc_param_name_in) IS NULL OR
            TRIM(pvc_param_group_in) IS NULL OR
            TRIM(pvc_param_value_in) IS NULL OR
            TRIM(pvc_param_descr_in) IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started getting param_value for param_name='||pvc_param_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        MERGE INTO scpomgr.udt_parameter p
        USING (SELECT pvc_param_name_in param_name,
                      pvc_param_group_in param_group,
                      pvc_param_value_in param_value,
                      pvc_param_descr_in param_descr
               FROM dual) x
        ON (p.param_name = x.param_name)
        WHEN MATCHED THEN UPDATE SET p.param_value = x.param_value
        WHEN NOT MATCHED THEN INSERT (param_name, param_group, param_value, param_descr)
        VALUES (x.param_name, x.param_group, x.param_value, x.param_descr);
        ln_record_count := SQL%ROWCOUNT;
        COMMIT;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed setting param_value for param_name='||pvc_param_name_in,0,ln_record_count,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('p_set_param requires a parameter name, group, and value. Aborting....');
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'p_set_param requires a parameter name, group, and value.  Aborting..',1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR( -20005, 'p_set_param requires a parameter name, group, and value.  Aborting...');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_step||' : '|| SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'p_set_param failed with '||SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR( -20005, lvc_step||' : '|| SQLERRM(SQLCODE));
    END p_set_param;

    FUNCTION f_log_step_start (pn_job_seq_no_in IN NUMBER,
                               pvc_job_name_in  IN VARCHAR2,
                               pvc_job_step_in  IN VARCHAR2,
                               pvc_log_msg_in   IN VARCHAR2 DEFAULT NULL,
                               pvc_table_name_in       IN VARCHAR2 DEFAULT ' ',
                               pvc_operation_type_in   IN VARCHAR2 DEFAULT ' ',
                               pn_line_no_in    IN NUMBER DEFAULT 0) RETURN VARCHAR2
    /*#############################################################################################
    # Function Name  : f_log_step_start
    #
    # Description    : This procedure is called within a script before a step is performed,
    #                  it will create a log record in the UDT_SCRIPT_LOG table.
    #
    # Input          : pvc_job_seq_no_in - job sequence number
    #                  pvc_job_name_in - name of job
    #                  pvc_job_step_in - step name of job
    #                  pvc_log_msg_in  - optional, log message
    #                  pn_line_no_in   - optional, Line No in Proc (can use $$PLSQL_LINE)
    #
    # Output         : step name
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by             Description
    #
    ################################################################################################*/
    IS
        ln_step_seqnum  NUMBER;
        le_exception    EXCEPTION;
        PRAGMA          AUTONOMOUS_TRANSACTION;
    BEGIN
        IF (pn_job_seq_no_in IS NULL OR pvc_job_name_in IS NULL OR pvc_job_step_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        SELECT SCPOMGR.UDT_SCRIPT_LOG_SEQ.NEXTVAL
          INTO ln_step_seqnum
          FROM DUAL;

        -- Insert starting record into UDT_SCRIPT_LOG
        INSERT
          INTO SCPOMGR.UDT_SCRIPT_LOG (job_seq_no,
                                       job_step_seq_no,
                                       job_name,
                                       batch_date,
                                       job_step,
                                       job_step_start,
                                       log_msg,
                                       table_name,
                                       operation_type,
                                       line_no)
                               VALUES (pn_job_seq_no_in,
                                       ln_step_seqnum,
                                       UPPER(pvc_job_name_in),
                                       NVL(STGMGR.PG_SPIN_LIB.f_get_batch_date(),TRUNC(SYSDATE+1)),
                                       TRIM(UPPER(pvc_job_step_in)),
                                       SYSDATE,
                                       pvc_log_msg_in,
                                       pvc_table_name_in,
                                       pvc_operation_type_in,
                                       pn_line_no_in);

        COMMIT;

        RETURN pvc_job_step_in;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('f_log_step_start : Need to pass the value for job sequence, job name and step name; process aborting.');
            RAISE_APPLICATION_ERROR (-20006,'f_log_step_start : Need to pass the value for job sequence, job name and step name; process aborting.');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE ('f_log_step_start : '||SQLERRM(SQLCODE));
            RAISE_APPLICATION_ERROR (-20006,'f_log_step_start : '||SQLERRM(SQLCODE));
    END f_log_step_start;


    PROCEDURE p_log_step_end (pn_job_seq_no_in        IN NUMBER,
                              pvc_job_step_in         IN VARCHAR2,
                              pvc_log_msg_in          IN VARCHAR2 DEFAULT NULL,
                              pn_return_code_in       IN NUMBER DEFAULT 0,
                              pn_records_inserted_in  IN NUMBER DEFAULT 0,
                              pn_records_updated_in   IN NUMBER DEFAULT 0,
                              pn_records_deleted_in   IN NUMBER DEFAULT 0,
                              pn_records_rejected_in  IN NUMBER DEFAULT 0,
                              pn_records_processed_in IN NUMBER DEFAULT 0,
                              pn_error_code_in        IN NUMBER DEFAULT 0)
    /*#############################################################################################
    # Function Name  : p_log_step_end
    #
    # Description    : This procedure is called within a script after a step is performed,
    #                  it will update the matching log record in the UDT_SCRIPT_LOG table.
    #
    # Input          : pvc_job_seq_no_in - job sequence number
    #                  pvc_job_step_in - name of job
    #                  pvc_log_msg_in - optional, log message
    #                  pvc_return_code_in - optional, return code of script
    #                  pvc_records_inserted_in - optional, number of records inserted
    #                  pvc_records_updated_in - optional, number of records updated
    #                  pvc_records_deleted_in - optional, number of records deleted
    #                  pvc_records_rejected_in - optional, number of records rejected
    #                  pvc_records_processed_in - optional, number of records processed
    #                  pn_error_code_in         - sql error code from SQLCODE
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by             Description
    #   7/6/2021      wm                      Added aadditional parameters to capture additional error info
    #  11/05/2021     Andy Fritz              Changed contents of lv_error_message to include entire stack
    #
    ################################################################################################*/
    IS
        ldt_endtime     DATE:=SYSDATE;
        le_exception    EXCEPTION;
        lvc_error_message        VARCHAR2(512) := ' ';
        lvc_call_stack           VARCHAR2(2000) := ' ';
        lvc_back_trace           VARCHAR2(2000) := ' ';
        PRAGMA          AUTONOMOUS_TRANSACTION;

    BEGIN
        IF (pn_job_seq_no_in IS NULL OR pvc_job_step_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF pn_error_code_in <> 0 then
           --lvc_error_message := SQLERRM(pn_error_code_in);
           lvc_error_message := SYS.DBMS_UTILITY.FORMAT_ERROR_STACK;
           lvc_back_trace := SYS.DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
           lvc_call_stack := SYS.DBMS_UTILITY.FORMAT_CALL_STACK;
        END IF;
----


        UPDATE scpomgr.udt_script_log us
           SET job_step_end         = ldt_endtime,
               duration             = ldt_endtime-us.job_step_start,
               log_msg              = pvc_log_msg_in,
               return_code          = pn_return_code_in,
               records_inserted     = pn_records_inserted_in,
               records_updated      = pn_records_updated_in,
               records_deleted      = pn_records_deleted_in,
               records_rejected     = pn_records_rejected_in,
               records_processed    = pn_records_processed_in,
               spot_rate            = DECODE(NVL(pn_records_processed_in,0),0,NULL,
                                      DECODE(ldt_endtime-us.job_step_start,0,pn_records_processed_in,
                                      ROUND(pn_records_processed_in/((ldt_endtime-us.job_step_start)*1440*60),10))),
               total_execution_time = FLOOR((ldt_endtime-us.job_step_start)*24)              ||' HOURS '   ||
                                      MOD(FLOOR((ldt_endtime-us.job_step_start)*24*60),60)   ||' MINUTES ' ||
                                      MOD(FLOOR((ldt_endtime-us.job_step_start)*24*60*60),60)||' SECS',
               error_code           = pn_error_code_in,
               error_message        = lvc_error_message,
               call_stack           = lvc_call_stack,
               back_trace           = lvc_back_trace
         WHERE job_seq_no = pn_job_seq_no_in
           AND job_step = UPPER(TRIM(pvc_job_step_in))
           AND job_step_seq_no = (SELECT MAX(job_step_seq_no)
                                    FROM scpomgr.udt_script_log
                                   WHERE job_seq_no = pn_job_seq_no_in
                                     AND job_step = UPPER(TRIM(pvc_job_step_in)));
        COMMIT;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_step_end : pn_job_seq_no_in and pvc_job_step_in must be populated; process aborting.');
            RAISE_APPLICATION_ERROR (-20007,'p_log_step_end : pn_job_seq_no_in and pvc_job_step_in must be populated; process aborting.');
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_step_end : Couldn''t Update SCPOMGR.UDT_SCRIPT_LOG for job_seq_no=' || pn_job_seq_no_in || ', job_step=' || pvc_job_step_in);
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_step_end : Couldn''t Update SCPOMGR.UDT_SCRIPT_LOG for job_seq_no=' || pn_job_seq_no_in || ', job_step=' || pvc_job_step_in);
            DBMS_OUTPUT.PUT_LINE ('p_log_step_end : '||SQLERRM(SQLCODE));
    END p_log_step_end;



    FUNCTION f_log_script_start (pvc_job_name_in IN VARCHAR2,
                                 pn_line_no_in IN NUMBER DEFAULT 0) RETURN NUMBER
    /*#############################################################################################
    # Function Name  : f_log_script_start
    #
    # Description    : This function is called at the start of a script
    #                  to create log records in the UDT_SCRIPT_RUNTIMES and UDT_SCRIPT_LOG tables.
    #
    # Input          : pvc_job_name_in
    #                  pn_line_no_in          - optional, Line No in Proc (can use $$PLSQL_LINE)
    # Output         : Number - job sequence
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by             Description
    #
    ################################################################################################*/
    IS
        ln_seqnum       NUMBER;
        lvc_step        VARCHAR2(100);
        ln_dup_seqnum   NUMBER;
        le_exception    EXCEPTION;
        PRAGMA          AUTONOMOUS_TRANSACTION;
    BEGIN
        SELECT SCPOMGR.UDT_SCRIPT_SEQ.NEXTVAL
          INTO ln_seqnum
          FROM DUAL;

        IF (pvc_job_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        -- Insert starting record into UDT_SCRIPT_RUNTIMES
        INSERT
          INTO SCPOMGR.UDT_SCRIPT_RUNTIMES (
                                       job_seq_no,
                                       job_name,
                                       batch_date,
                                       job_start)
                               VALUES (ln_seqnum,
                                       UPPER(pvc_job_name_in),
                                       NVL(STGMGR.PG_SPIN_LIB.f_get_batch_date(),TRUNC(SYSDATE+1)),
                                       SYSDATE);

        COMMIT;

        -- Insert starting record into UDT_SCRIPT_LOG
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,UPPER(pvc_job_name_in),UPPER(pvc_job_name_in),
                                            UPPER(pvc_job_name_in)||' Started',' ',' ',pn_line_no_in);

        RETURN ln_seqnum;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('f_log_script_start : Need to pass the value for job name; process aborting.');
            RAISE_APPLICATION_ERROR (-20008,'f_log_script_start : Need to pass the value for job name; process aborting.');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE ('f_log_script_start : '||SQLERRM(SQLCODE));
            RAISE_APPLICATION_ERROR (-20008,'f_log_script_start : '||SQLERRM(SQLCODE));
    END f_log_script_start;


    PROCEDURE p_log_script_end (pn_job_seq_no_in        IN NUMBER,
                                pn_return_code_in       IN NUMBER DEFAULT 0,
                                pn_records_inserted_in  IN NUMBER DEFAULT 0,
                                pn_records_updated_in   IN NUMBER DEFAULT 0,
                                pn_records_deleted_in   IN NUMBER DEFAULT 0,
                                pn_records_rejected_in  IN NUMBER DEFAULT 0,
                                pn_records_processed_in IN NUMBER DEFAULT 0,
                                pn_error_code_in        IN NUMBER DEFAULT 0)
    /*#############################################################################################
    # Procedure Name : p_log_script_end
    #
    # Description    : This procedure updates the corresponding log record in the
    #                  UDT_SCRIPT_RUNTIMES and UDT_SCRIPT_LOG tables.
    #
    # Input          : pn_job_seq_no_in - job sequence number
    #                  pn_return_code_in - return code of script
    #                  pn_records_inserted_in - number of records inserted
    #                  pn_records_updated_in - number of records updated
    #                  pn_records_deleted_in - number of records deleted
    #                  pn_records_rejected_in - number of records rejected
    #                  pn_records_processed_in - number of records processed
    #                  pvc_table_name_in        - table name
    #                  pvc_operation_type_in    - operation type
    #                  pn_line_no_in           - line number
    #                  pn_error_code_in        - sql error code from SQLCODE
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by             Description
    #
    ################################################################################################*/
    IS
        ldt_endtime     DATE:=SYSDATE;
        ldt_starttime   DATE;
        le_exception    EXCEPTION;
        lvc_job_name    scpomgr.udt_script_log.job_name%TYPE;
        lvc_msg         VARCHAR2(20);
        PRAGMA          AUTONOMOUS_TRANSACTION;
    BEGIN
        IF pn_job_seq_no_in IS NULL THEN
            RAISE le_exception;
        END IF;

        UPDATE scpomgr.udt_script_runtimes us
           SET job_end              = ldt_endtime,
               duration             = ldt_endtime-us.job_start,
               return_code          = pn_return_code_in,
               records_inserted     = pn_records_inserted_in,
               records_updated      = pn_records_updated_in,
               records_deleted      = pn_records_deleted_in,
               records_rejected     = pn_records_rejected_in,
               records_processed    = pn_records_processed_in,
               spot_rate            = DECODE(NVL(pn_records_processed_in,0),0,NULL,
                                      DECODE(ldt_endtime-us.job_start,0,pn_records_processed_in,
                                      ROUND(pn_records_processed_in/((ldt_endtime-us.job_start)*1440*60),10))),
               total_execution_time = FLOOR((ldt_endtime-us.job_start)*24)              ||' HOURS '   ||
                                      MOD(FLOOR((ldt_endtime-us.job_start)*24*60),60)   ||' MINUTES ' ||
                                      MOD(FLOOR((ldt_endtime-us.job_start)*24*60*60),60)||' SECS'
         WHERE job_seq_no = pn_job_seq_no_in
         RETURNING job_name
          INTO lvc_job_name;

        COMMIT;

        IF pn_return_code_in = 0 THEN
          lvc_msg := ' Completed';
        ELSE
          lvc_msg := ' Failed';
        END IF;

        -- Insert ending record into UDT_SCRIPT_LOG
        STGMGR.pg_spin_lib.p_log_step_end (pn_job_seq_no_in,lvc_job_name,lvc_job_name||lvc_msg,
                                           pn_return_code_in,pn_records_inserted_in,pn_records_updated_in,
                                           pn_records_deleted_in,pn_records_rejected_in,pn_records_processed_in,
                                           pn_error_code_in);

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_script_end : pn_job_seq_no_in must be populated; process aborting.');
            RAISE_APPLICATION_ERROR (-20009,'p_log_script_end : pn_job_seq_no_in must be populated; process aborting.');
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_script_end : Couldn''t Update SCPOMGR.UDT_SCRIPT_RUNTIMES for job_seq_no=' || pn_job_seq_no_in);
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_script_end : Couldn''t Update SCPOMGR.UDT_SCRIPT_RUNTIMES for job_seq_no=' || pn_job_seq_no_in);
            DBMS_OUTPUT.PUT_LINE ('p_log_script_end : '||SQLERRM(SQLCODE));
    END p_log_script_end;


    PROCEDURE p_log_batch_start (pvc_batch_name_in  IN VARCHAR2)
    /*#############################################################################################
    # Function Name  : p_log_batch_start
    #
    # Description    : This procedure is called at the start of a batch schedule,
    #                  it will create a log record in the UDT_BATCH_RUNTIMES table.
    #
    # Input          : pvc_batch_name_in - Name of batch schedulejob sequence number
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by             Description
    #
    ################################################################################################*/
    IS
        le_exception    EXCEPTION;
        PRAGMA          AUTONOMOUS_TRANSACTION;
    BEGIN

        IF (pvc_batch_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        -- Insert record into UDT_BATCH_RUNTIMES
        INSERT
          INTO scpomgr.udt_batch_runtimes
              (batch_name,
               batch_date,
               batch_start)
       VALUES (UPPER(pvc_batch_name_in),
               NVL(STGMGR.PG_SPIN_LIB.f_get_batch_date(),TRUNC(SYSDATE+1)),
               SYSDATE);

        COMMIT;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_batch_start : Need to pass the value for batch name; process aborting.');
            RAISE_APPLICATION_ERROR (-20010,'p_log_batch_start : Need to pass the value for batch name; process aborting.');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_batch_start : '||SQLERRM(SQLCODE));
            RAISE_APPLICATION_ERROR (-20010,'p_log_batch_start : '||SQLERRM(SQLCODE));
    END p_log_batch_start;

    PROCEDURE p_log_batch_end (pvc_batch_name_in  IN VARCHAR2)
    /*#############################################################################################
    # Function Name  : p_log_batch_end
    #
    # Description    : This procedure is called at the start of a batch schedule,
    #                  it will create a log record in the UDT_BATCH_RUNTIMES table.
    #
    # Input          : pvc_batch_name_in - Name of batch schedulejob sequence number
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by             Description
    #
    ################################################################################################*/
    IS
        le_exception    EXCEPTION;
        PRAGMA          AUTONOMOUS_TRANSACTION;
    BEGIN

        IF (pvc_batch_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        -- Update BATCH_END in UDT_BATCH_RUNTIMES.
        -- If there are multiple entries for the same batch in the same
        -- day with BATCH_END not populated, then update the record
        -- with the latest BATCH_START among those.
        UPDATE scpomgr.udt_batch_runtimes
           SET batch_end = SYSDATE
         WHERE batch_name = pvc_batch_name_in
           AND batch_end IS NULL
           AND batch_start = (SELECT MAX(batch_start)
                                FROM scpomgr.udt_batch_runtimes
                               WHERE batch_name = pvc_batch_name_in
                                 AND batch_end IS NULL);

        COMMIT;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_batch_end : Need to pass the value for batch name; process aborting.');
            RAISE_APPLICATION_ERROR (-20020,'p_log_batch_end : Need to pass the value for batch name; process aborting.');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE ('p_log_batch_end : '||SQLERRM(SQLCODE));
            RAISE_APPLICATION_ERROR (-20020,'p_log_batch_end : '||SQLERRM(SQLCODE));
    END p_log_batch_end;


    PROCEDURE p_archive_log_messages
    /*#############################################################################################
    # Procedure Name : p_archive_log_messages
    #
    # Description    : This procedure is used to archive the logs from
    #                  SCPOMGR.UDT_SCRIPT_RUNTIMES and SCPOMGR.UDT_SCRIPT_LOGS tables
    #                  to SCPOMGR.UDT_SCRIPT_RUNTIMES_ARC and SCPOMGR.UDT_SCRIPT_LOG_ARC tables.
    #
    # Input          : NA
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by             Description
    #
    ################################################################################################*/
    AS
        lvc_jobname          VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_archive_log_messages';
        lvc_step             VARCHAR2(100);
        ln_retention_period  NUMBER:=0;
        ldt_date             DATE;
        ln_seqnum            NUMBER;
        le_exception         EXCEPTION;
        ln_rows_ins          NUMBER:=0;
        ln_rows_del          NUMBER:=0;
    BEGIN

        ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start (lvc_jobname);

        lvc_step := STGMGR.pg_spin_lib.f_log_step_start (ln_seqnum,lvc_jobname,'RETENTION_PERIOD','Getting RETENTION_PERIOD');
        ln_retention_period:=f_get_param('LOG_RETENTION',ln_seqnum,lvc_jobname);
        IF (ln_retention_period < 0) THEN
            RAISE le_exception;
        END IF;
        STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_step,'Done getting RETENTION_PERIOD, value='||ln_retention_period,0,0,0,0,0,0);

        lvc_step := STGMGR.pg_spin_lib.f_log_step_start (ln_seqnum,lvc_jobname,'SYSDATE','Getting SYSDATE');
        SELECT TRUNC(SYSDATE)
          INTO ldt_date
          FROM dual;
        STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_step,'Done getting SYSDATE, value='||to_char(ldt_date,'MM/DD/YY'),0,0,0,0,0,0);

        lvc_step := STGMGR.pg_spin_lib.f_log_step_start (ln_seqnum,lvc_jobname,'Archive UDT_SCRIPT_LOG');
        INSERT INTO scpomgr.udt_script_log_arc
                   (job_seq_no,
                    job_step_seq_no,
                    job_name,
                    batch_date,
                    job_step,
                    job_step_start,
                    job_step_end,
                    duration,
                    log_msg,
                    return_code,
                    records_inserted,
                    records_updated,
                    records_deleted,
                    records_rejected,
                    records_processed,
                    spot_rate,
                    total_execution_time,
                    table_name,
                    operation_type,
                    line_no,
                    error_code,
                    error_message,
                    call_stack,
                    back_trace)
             SELECT job_seq_no,
                    job_step_seq_no,
                    job_name,
                    batch_date,
                    job_step,
                    job_step_start,
                    job_step_end,
                    duration,
                    log_msg,
                    return_code,
                    records_inserted,
                    records_updated,
                    records_deleted,
                    records_rejected,
                    records_processed,
                    spot_rate,
                    total_execution_time,
                    table_name,
                    operation_type,
                    line_no,
                    error_code,
                    error_message,
                    call_stack,
                    back_trace
               FROM scpomgr.udt_script_log
              WHERE TRUNC(batch_date) < TRUNC(ldt_date)-ln_retention_period;
        ln_rows_ins := ln_rows_ins + SQL%ROWCOUNT;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Done archiving UDT_SCRIPT_LOG',0,SQL%ROWCOUNT,0,0,0,SQL%ROWCOUNT);

        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,lvc_jobname,'Purge UDT_SCRIPT_LOG');
        DELETE FROM scpomgr.udt_script_log
         WHERE TRUNC(batch_date) < TRUNC(ldt_date)-ln_retention_period;
        ln_rows_del := ln_rows_del + SQL%ROWCOUNT;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Done purging UDT_SCRIPT_LOG',0,0,0,SQL%ROWCOUNT,0,SQL%ROWCOUNT);

        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,lvc_jobname,'Archive UDT_SCRIPT_RUNTIMES');
        INSERT INTO scpomgr.udt_script_runtimes_arc
                   (job_seq_no,
                    job_name,
                    batch_date,
                    job_start,
                    job_end,
                    duration,
                    return_code,
                    records_inserted,
                    records_updated,
                    records_deleted,
                    records_rejected,
                    records_processed,
                    spot_rate,
                    total_execution_time)
             SELECT job_seq_no,
                    job_name,
                    batch_date,
                    job_start,
                    job_end,
                    duration,
                    return_code,
                    records_inserted,
                    records_updated,
                    records_deleted,
                    records_rejected,
                    records_processed,
                    spot_rate,
                    total_execution_time
               FROM scpomgr.udt_script_runtimes
              WHERE TRUNC(batch_date) < TRUNC(ldt_date)-ln_retention_period;
        ln_rows_ins := ln_rows_ins + SQL%ROWCOUNT;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Done archiving UDT_SCRIPT_RUNTIMES',0,SQL%ROWCOUNT,0,0,0,SQL%ROWCOUNT);

        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,lvc_jobname,'Purge UDT_SCRIPT_RUNTIMES');
        DELETE FROM scpomgr.udt_script_runtimes
         WHERE TRUNC(batch_date) < TRUNC(ldt_date)-ln_retention_period;
        ln_rows_del := ln_rows_del + SQL%ROWCOUNT;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Done purging UDT_SCRIPT_RUNTIMES',0,0,0,SQL%ROWCOUNT,0,SQL%ROWCOUNT);

        COMMIT;

        STGMGR.pg_spin_lib.p_log_script_end (ln_seqnum,0,ln_rows_ins,0,ln_rows_del,0,ln_rows_ins+ln_rows_del);

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('p_archive_log_messages : Retention period should be greater than or equal to 0: Aborting...');
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error:' || SQLERRM(SQLCODE),1,0,0,0,0,0);
            STGMGR.pg_spin_lib.p_log_script_end (ln_seqnum,1,0,0,0,0,0);
            RAISE_APPLICATION_ERROR (-20030, 'p_archive_log_messages : Retention period should be greater than or equal to 0: Aborting...');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE ('p_archive_log_messages : '||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error:' || SQLERRM(SQLCODE),1,0,0,0,0,0);
            STGMGR.pg_spin_lib.p_log_script_end (ln_seqnum,1,0,0,0,0,0);
            RAISE_APPLICATION_ERROR (-20030, 'p_archive_log_messages : '||SQLERRM(SQLCODE));
    END p_archive_log_messages;


    FUNCTION f_is_pmi_modified (pvc_table_owner_in     IN VARCHAR2,
                                pvc_table_name_in      IN VARCHAR2,
                                pvc_constraint_name_in IN VARCHAR2,
                                pvc_jobname_in         IN VARCHAR2 DEFAULT NULL,
                                pn_seqnum_in           IN NUMBER DEFAULT NULL) RETURN NUMBER
    /*#############################################################################################
    # Function Name  : f_is_pmi_modified
    # Description    : This function will accept 3 parameters and returns a number.  Based on the
    #                  parameters, this function will check for whether p_maintain_indexes procedure
    #                  altered the constraint or not within STGMGR.batch_index_ddl table.  If the
    #                  constraint is altered, function will return 1 else 0.
    #
    # Input          : pvc_table_owner_in (Mapped to BATCH_INDEX_DDL.TABLE_OWNER),
    #                  pvc_table_name_in (Mapped to BATCH_INDEX_DDL.table_name),
    #                  pvc_constraint_name_in (Mapped to BATCH_INDEX_DDL.INDEX_NAME)
    #                  pvc_jobname_in - optional, job name for logging
    #                  pn_seqnum_in - optional, job sequence number for logging
    #
    # Output         : Number 0 or 1
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on       Modified by         Description
    #
    #############################################################################################*/
    IS
        lvc_jobname         VARCHAR2(100) := 'STGMGR.pg_spin_lib.f_is_pmi_modified';
        ln_seqnum           NUMBER;
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(300);
        ln_is_modified      NUMBER:=0;
    BEGIN
        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started check for constraint='||pvc_constraint_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        SELECT COUNT(1)
          INTO ln_is_modified
          FROM STGMGR.batch_index_ddl
         WHERE index_name  = pvc_constraint_name_in
           AND table_owner = pvc_table_owner_in
           AND table_name  = pvc_table_name_in
           AND batch_disable_flag = 'Y';

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed check for constraint='||pvc_constraint_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        RETURN ln_is_modified;

        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            ln_is_modified:=0;
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'Completed',0,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
            END IF;
            RETURN ln_is_modified;
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE ('Unknown Error in f_is_pmi_modified:'||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR (-20040,SQLERRM(SQLCODE));
    END f_is_pmi_modified;

    PROCEDURE p_maintain_indexes (pvc_action_in    IN VARCHAR2,
                                  pvc_jobname_in   IN VARCHAR2 DEFAULT NULL,
                                  pn_seqnum_in     IN NUMBER DEFAULT NULL)
    /*#############################################################################################
    #  Procedure Name : p_maintain_indexes
    #
    #  Description    : Executes DDL to drop or recreate indexes or FKs
    #
    #  Input          : pvc_action_in, pn_seqnum_in, pvc_jobname_in
    #  Input          : pvc_action_in - BATCHSTART or BATCHEND
    #                   pvc_jobname_in - optional, job name for logging
    #                   pn_seqnum_in - optional, job sequence number for logging
    #
    #  Output         : NA
    #  Author         : Andy Fritz
    #  Date created   : 10/14/2019
    #  Restartable    : Yes
    #
    #  Modified on      Modified by         Description
    #
    #############################################################################################*/
    IS
        lvc_jobname     VARCHAR2(100) := 'STGMGR.PG_SPIN_LIB.p_maintain_indexes';
        lvc_step        VARCHAR2(100);
        lc_exists       CHAR(1);
        lclob_exec_sql  CLOB;
        lvc_stepname    VARCHAR2(1000);
        ln_seqnum       NUMBER;
        le_exception    EXCEPTION;

        CURSOR CUR_INDEX_DDL(pvc_action_in VARCHAR2)  IS
            SELECT process_name, index_name, index_owner
                   ,is_pk_fl,table_name,table_owner,index_ddl
                   ,DECODE(pvc_action_in,'BATCHSTART',BATCH_START,BATCH_END) ddl_action
              FROM STGMGR.batch_index_ddl
             WHERE batch_start IS NOT NULL OR batch_end IS NOT NULL
             ORDER BY DECODE(DECODE(pvc_action_in,'BATCHSTART',BATCH_START,BATCH_END),'CREATE',ABS(IS_PK_FL-2), IS_PK_FL) DESC;
    BEGIN
        IF (pvc_action_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for action='||pvc_action_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        FOR rec IN CUR_INDEX_DDL(UPPER(pvc_action_in))
        LOOP
            lc_exists:=NULL;
            IF rec.is_pk_fl =0 THEN
                SELECT DECODE(COUNT(1),0,'N','Y') INTO lc_exists
                  FROM all_indexes idx
                 WHERE idx.index_name  = rec.index_name
                   AND idx.table_name  = rec.table_name
                   AND idx.owner       = rec.index_owner;
            ELSIF rec.is_pk_fl = 1 THEN
                SELECT DECODE(COUNT(1),0,'N','Y') INTO lc_exists
                  FROM all_constraints pk
                 WHERE pk.constraint_name = rec.index_name
                   AND pk.table_name      = rec.table_name
                   AND pk.constraint_type = 'P'
                   AND pk.owner           = rec.index_owner;
            ELSE
                --dbms_output.put_line ('rec.index_name,rec.table_name,rec.index_owner:'||rec.index_name||','||rec.table_name||','||rec.index_owner);
                SELECT DECODE(COUNT(1),0,'N','Y') INTO lc_exists
                  FROM all_constraints pk
                 WHERE pk.constraint_name = rec.index_name
                   AND pk.table_name      = rec.table_name
                   AND pk.constraint_type = 'R'
                   AND pk.owner           = rec.index_owner;
            END IF;

            IF lc_exists ='Y' THEN
                IF rec.is_pk_fl = 0 THEN
                    lvc_stepname:='ALTER INDEX ' ||rec.index_owner||'.'||rec.index_name ||' PARALLEL 16';
                    lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Starting');

                    EXECUTE IMMEDIATE 'BEGIN '||rec.index_owner||'.P_EXEC_SQL(:P_DDL); END;'
                        USING 'ALTER INDEX ' ||rec.index_owner||'.'||rec.index_name ||' PARALLEL 16';

                    STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Complete',0,0,0,0,0,0);
                    commit;
                END IF ;

                lvc_stepname:='UPDATE STGMGR.batch_index_ddl For '||rec.index_owner||'.'||rec.index_name;
                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Starting');

                EXECUTE IMMEDIATE 'UPDATE STGMGR.BATCH_INDEX_DDL bid
                                      SET index_ddl = regexp_replace('||rec.index_owner||'.f_get_ddl(:p_type, :pvc_owner_in, :p_idx_name),''ALTER INDEX '||'"'||rec.index_owner||'"'||'.'||'"'||rec.index_name||'"'|| '.*UNUSABLE'',NULL)
                                    WHERE bid.index_name  = :p_idx_name
                                      AND bid.table_name  = :p_tbl_name
                                      AND bid.index_owner = :pvc_owner_in
                                      AND bid.is_pk_fl    = :p_is_pk_fl '
                USING CASE rec.is_pk_fl
                        WHEN 1 THEN 'CONSTRAINT'
                        WHEN 2 THEN 'REF_CONSTRAINT'
                        ELSE 'INDEX' END,
                        rec.index_owner,rec.index_name,rec.index_name,rec.table_name,rec.index_owner,rec.is_pk_fl;

                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Complete',0,0,0,0,0,0);
                COMMIT;
            END IF;

            IF (rec.ddl_action ='DROP' AND lc_exists ='Y') OR (rec.ddl_action ='CREATE' AND lc_exists ='N') OR (rec.ddl_action ='CREATE' AND lc_exists ='Y' AND rec.is_pk_fl != 0)THEN

                lvc_stepname:='Processing INDEX/CONSTRAINT: ' || rec.index_owner||'.'||rec.index_name;
                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Starting');

                EXECUTE IMMEDIATE 'BEGIN '||rec.index_owner||'.P_EXEC_SQL(:P_DDL); END;'
                USING CASE rec.ddl_action
                    WHEN 'DROP'   THEN
                        CASE rec.is_pk_fl
                            WHEN 0 THEN 'DROP INDEX '||rec.index_owner||'.'||rec.index_name
                                ELSE 'ALTER TABLE '||rec.table_owner||'.'||rec.table_name||' DISABLE CONSTRAINT '||rec.index_name
                    END
                    WHEN 'CREATE' THEN
                        CASE rec.is_pk_fl
                            WHEN 0 THEN REPLACE(REPLACE(REC.INDEX_DDL, 'COMPUTE STATISTICS', ''), 'NOPARALLEL', 'PARALLEL(DEGREE 16)') --REC.INDEX_DDL
                                ELSE  'ALTER TABLE '||rec.table_owner||'.'||rec.table_name||' ENABLE NOVALIDATE CONSTRAINT '||rec.index_name
                        END
                    ELSE 'NULL'
                END;
                COMMIT;

                -- The below IF condition was added to Track Whether Constraint was either ENABLED OR DISABLED
                IF (rec.ddl_action='DROP' AND  rec.is_pk_fl <> 0) THEN
                    UPDATE STGMGR.batch_index_ddl
                       SET batch_disable_flag = 'Y'
                     WHERE index_name =  rec.index_name
                       AND index_owner = rec.index_owner;
                ELSE
                    IF (rec.ddl_action='CREATE' and  rec.is_pk_fl <> 0) THEN
                        UPDATE STGMGR.batch_index_ddl
                           SET batch_disable_flag = 'N'
                         WHERE index_name =  rec.index_name
                           AND index_owner = rec.index_owner;
                    END IF;
                END IF;
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Complete',0,0,0,0,0,0);

            END IF;

            IF rec.ddl_action = 'CREATE' AND  rec.is_pk_fl = 0 THEN
                lvc_stepname:='ALTER INDEX  ' ||rec.index_owner||'.'||rec.index_name ||' NOPARALLEL';
                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Starting');

                EXECUTE IMMEDIATE 'BEGIN '||rec.index_owner||'.P_EXEC_SQL(:P_DDL); END;'
                       USING 'ALTER INDEX ' ||rec.index_owner||'.'||rec.index_name ||' NOPARALLEL';

                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Complete',0,0,0,0,0,0);
                COMMIT;
            END IF;
        END LOOP;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for action='||pvc_action_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        COMMIT;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : First parameter must be either BATCHSTART or BATCHEND.  Aborting....');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed, first parameter needs to be BATCHSTART or BATCHEND.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20050, lvc_jobname||' : First parameter must be either BATCHSTART or BATCHEND.  Aborting....');
            WHEN OTHERS THEN
                ROLLBACK;
                dbms_output.put_line('----------------------------------------------------------');
                dbms_output.put_line('[' || to_char(sysdate, 'MM/DD/YYYY HH24:MI:SS') || '] Procedure STGMGR.PG_SPIN_LIB.p_maintain_indexes() failed with error: ');
                dbms_output.put_line('----------------------------------------------------------');
                dbms_output.put_line(sqlerrm);
                dbms_output.put_line(dbms_utility.format_error_backtrace);
                dbms_output.put_line('Statement Executed:');
                dbms_output.put_line(lclob_exec_sql);
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,lclob_exec_sql||' failed with error: ' || SQLERRM(SQLCODE),0,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE;
    END p_maintain_indexes;

    PROCEDURE p_maintain_indexes (pvc_action_in     IN VARCHAR2,
                                  pvc_process_nm_in IN VARCHAR2,
                                  pvc_table_nm_in   IN VARCHAR2,
                                  pvc_index_nm_in   IN VARCHAR2 DEFAULT NULL,
                                  pvc_jobname_in    IN VARCHAR2 DEFAULT NULL,
                                  pn_seqnum_in      IN NUMBER DEFAULT NULL)
    /*#############################################################################################
    #  Procedure Name : p_maintain_indexes
    #
    #  Description    : This is an overloaded version of the p_maintain_indexes procedure.
    #                   It will drop/create (based on pvc_action_in) indexes on a table which name
    #                   passed as pvc_table_nm_in parameter.
    #
    #  Input          : pvc_action_in, pvc_process_nm_in, pvc_table_nm_in, pvc_index_nm_in,
    #                   pvc_jobname_in, pn_seqnum_in
    #  Output         : NA
    #  Author         : Andy Fritz
    #  Date created   : 10/14/2019
    #  Restartable    : Yes
    #
    #  Modified on      Modified by         Description
    #
    #############################################################################################*/
    IS
        lvc_jobname       VARCHAR2(100) := 'STGMGR.PG_SPIN_LIB.p_maintain_indexes';
        lvc_step          VARCHAR2(200);
        lc_exists         VARCHAR2(1);
        lvc_clnt_info     VARCHAR2(255);
        lvc_table_owner   VARCHAR2(50);
        lvc_table_name    VARCHAR2(50);
        lvc_stepname      VARCHAR2(1000);
        ln_seqnum         NUMBER;
        le_exception      EXCEPTION;

    BEGIN
        IF (pvc_action_in IS NULL OR pvc_process_nm_in IS NULL OR pvc_table_nm_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF pvc_action_in NOT IN (gcvc_drop,gcvc_create) THEN
            raise_application_error(-20010,'p_maintain_indexes: Unknown action=''' || pvc_action_in || '''. Aborting.');
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||pvc_table_nm_in||' index='||pvc_index_nm_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        dbms_application_info.read_client_info(lvc_clnt_info);
        dbms_application_info.set_client_info('p_maintain_indexes('|| pvc_action_in|| ','|| pvc_table_nm_in|| ')');

        FOR rec IN (SELECT *
                      FROM STGMGR.batch_index_ddl
                     WHERE upper(process_name) = upper(pvc_process_nm_in)
                       AND upper(table_name) = upper(pvc_table_nm_in)
                       AND upper(index_name) = nvl(trim(upper(pvc_index_nm_in) ),upper(index_name))
                    ORDER BY index_owner, index_name,
                       -- need to do a "fancy" sort so that PK gets sorted last when action is CREATE and first when action is DROP
                           DECODE(pvc_action_in,gcvc_create,DECODE(is_pk_fl,1,0,1),is_pk_fl) DESC)
        LOOP
            lvc_stepname := pvc_action_in||' '||pvc_table_nm_in||'.'||pvc_index_nm_in;
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

            IF rec.is_pk_fl = 0 THEN
                SELECT DECODE(COUNT(1),0,'N','Y') INTO lc_exists
                  FROM all_indexes idx
                 WHERE idx.index_name = rec.index_name
                   AND idx.table_name = rec.table_name
                   AND idx.owner = rec.index_owner;

            ELSIF rec.is_pk_fl = 1 THEN
                SELECT DECODE(COUNT(1),0,'N','Y') INTO lc_exists
                  FROM all_constraints pk
                 WHERE pk.constraint_name = rec.index_name
                   AND pk.table_name = rec.table_name
                   AND pk.constraint_type = 'P'
                   AND pk.owner = rec.index_owner;
            ELSE
                SELECT DECODE(COUNT(1),0,'N','Y') INTO lc_exists
                  FROM all_constraints pk
                 WHERE pk.constraint_name = rec.index_name
                   AND pk.table_name = rec.table_name
                   AND pk.constraint_type = 'R'
                   AND pk.owner = rec.index_owner;

                SELECT owner, table_name INTO lvc_table_owner,lvc_table_name
                  FROM all_constraints
                 WHERE ( owner,constraint_name ) IN (SELECT owner, r_constraint_name
                                                       FROM all_constraints
                                                      WHERE owner = rec.index_owner
                                                        AND table_name = rec.table_name
                                                        AND constraint_name = rec.index_name
                                                        AND constraint_type = 'R'
                    );
            END IF;

            IF ( pvc_action_in = gcvc_drop AND lc_exists = 'Y' ) OR ( pvc_action_in = gcvc_create AND lc_exists = 'N' ) OR ( pvc_action_in = gcvc_create AND lc_exists = 'Y' AND rec.is_pk_fl != 0 ) THEN
                IF lc_exists = 'Y' AND pvc_action_in = gcvc_drop THEN
                    IF rec.is_pk_fl = 0 THEN
                        EXECUTE IMMEDIATE 'BEGIN ' || rec.index_owner || '.P_EXEC_SQL(:P_DDL); END;'
                            USING 'ALTER INDEX ' || rec.index_owner|| '.'|| rec.index_name|| ' PARALLEL 16';
                    END IF;

                    EXECUTE IMMEDIATE 'UPDATE STGMGR.BATCH_INDEX_DDL bid SET index_ddl = replace('|| rec.index_owner|| '.f_get_ddl(:p_type,:pvc_owner_in,:p_idx_name),''ALTER INDEX '|| '"'|| rec.index_owner|| '"'|| '.'|| '"'|| rec.index_name|| '"'|| '  UNUSABLE'',NULL)
                                        WHERE bid.index_name  = :p_idx_name
                                          AND bid.table_name  = :p_tbl_name
                                          AND bid.index_owner = :pvc_owner_in
                                          AND bid.is_pk_fl    = :p_is_pk_fl '
                                        USING
                                            CASE rec.is_pk_fl
                                                WHEN 1 THEN 'CONSTRAINT'
                                                WHEN 2 THEN 'REF_CONSTRAINT'
                                                ELSE 'INDEX'
                                            END,
                                        rec.index_owner,rec.index_name,rec.index_name,rec.table_name,rec.index_owner,rec.is_pk_fl;

                END IF;

                EXECUTE IMMEDIATE 'BEGIN ' || rec.index_owner || '.p_exec_sql(:p_ddl); END;'
                    USING
                        CASE pvc_action_in
                            WHEN gcvc_drop THEN
                                CASE rec.is_pk_fl
                                    WHEN 0 THEN 'DROP INDEX '|| rec.index_owner|| '.'|| rec.index_name
                                    ELSE 'ALTER TABLE '|| rec.table_owner|| '.'|| rec.table_name|| ' DISABLE CONSTRAINT '|| rec.index_name
                                END
                            WHEN gcvc_create THEN
                                CASE rec.is_pk_fl
                                    WHEN 0 THEN
                                        replace( replace(rec.index_ddl,'COMPUTE STATISTICS',''), 'NOPARALLEL','PARALLEL(DEGREE 16)') --REC.INDEX_DDL
                                    ELSE
                                        CASE lc_exists
                                            WHEN 'N' THEN
                                                rec.index_ddl
                                            ELSE
                                                CASE rec.is_pk_fl
                                                    WHEN 1 THEN 'ALTER TABLE '|| rec.table_owner|| '.'|| rec.table_name|| ' ENABLE NOVALIDATE CONSTRAINT '|| rec.index_name
                                                    ELSE
                                                       'SELECT ''Target table will be locked and constraints will be enabled in the next step'' FROM DUAL'
                                                END
                                        END
                                END
                            ELSE 'NULL'
                        END;

                -- The below IF condition was added to Track Whether Constraint was either ENABLED OR DISABLED
                IF (pvc_action_in = gcvc_drop AND  rec.is_pk_fl <> 0) THEN
                    UPDATE STGMGR.batch_index_ddl
                       SET batch_disable_flag = 'Y'
                     WHERE index_name   = rec.index_name
                       AND index_owner  = rec.index_owner
                       AND process_name = pvc_process_nm_in;
                ELSE
                    IF (pvc_action_in=gcvc_create and  rec.is_pk_fl <> 0) THEN
                        UPDATE STGMGR.batch_index_ddl
                           SET batch_disable_flag = 'Y'
                         WHERE index_name   = rec.index_name
                           AND index_owner  = rec.index_owner
                           AND process_name = pvc_process_nm_in;
                    END IF;
                END IF;


                IF pvc_action_in = gcvc_create AND rec.is_pk_fl = 0 THEN
                    EXECUTE IMMEDIATE 'BEGIN ' || rec.index_owner || '.p_exec_sql(:P_DDL); END;'
                        USING 'ALTER INDEX ' || rec.index_owner || '.' || rec.index_name || ' NOPARALLEL';
                END IF;

                IF pvc_action_in = gcvc_create AND rec.is_pk_fl = 2 AND lc_exists = 'Y' THEN
                    EXECUTE IMMEDIATE 'LOCK TABLE ' || lvc_table_owner || '.' || lvc_table_name || ' IN SHARE MODE';
                    EXECUTE IMMEDIATE 'BEGIN ' || rec.index_owner || '.p_exec_sql(:P_DDL); END;'
                        USING 'ALTER TABLE ' || rec.table_owner || '.' || rec.table_name || ' ENABLE NOVALIDATE CONSTRAINT ' || rec.index_name;
                END IF;
            END IF;
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
        END LOOP;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||pvc_table_nm_in||' index='||pvc_index_nm_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        dbms_application_info.set_client_info(lvc_clnt_info);
        COMMIT;
        EXCEPTION
        WHEN le_exception THEN
            IF (pvc_action_in IS NULL) THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||': First parameter must be either CREATE or DROP.  Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'First parameter must be either CREATE or DROP.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20060, lvc_jobname||': First parameter must be either CREATE or DROP.  Aborting...');
            ELSE
                IF (pvc_process_nm_in IS NULL) THEN
                    DBMS_OUTPUT.PUT_LINE (lvc_jobname||': Second parameter needs to be Process Name.  Aborting...');
                    STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Second parameter needs to be Process Name.  Aborting...',1,0,0,0,0,0);
                    IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                        STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                    END IF;
                    RAISE_APPLICATION_ERROR( -20060, lvc_jobname||': Second parameter needs to be Process Name.  Aborting...');
                ELSE
                    IF (pvc_table_nm_in IS NULL) THEN
                        DBMS_OUTPUT.PUT_LINE (lvc_jobname||': Third parameter needs to be Table Name.  Aborting...');
                        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Third parameter needs to be Table Name.  Aborting...',1,0,0,0,0,0);
                        IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                        END IF;
                        RAISE_APPLICATION_ERROR( -20060, lvc_jobname||': Third parameter needs to be Table Name.  Aborting...');
                     END IF;
                END IF;
            END IF;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE (SQLERRM(SQLCODE));
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                ROLLBACK;
                RAISE_APPLICATION_ERROR( -20060, SQLERRM(SQLCODE));
    END p_maintain_indexes;

    PROCEDURE p_disable_fk (pvc_owner_in           IN VARCHAR2,
                            pvc_table_name_in      IN VARCHAR2,
                            pi_include_children_in IN PLS_INTEGER DEFAULT 0,
                            pvc_jobname_in         IN VARCHAR2 DEFAULT NULL,
                            pn_seqnum_in           IN NUMBER DEFAULT NULL)
    /*#############################################################################################
    #  Procedure Name : p_disable_FK
    #
    #  Description    : Disables FK constraints on a given table
    #
    #  Input          : pvc_owner_in,pvc_table_name_in, pi_include_children_in,
    #                   pvc_jobname_in, pn_seqnum_in.
    #  Output         : NA
    #
    #  Author         : Andy Fritz
    #  Date created   : 10/14/2019
    #  Restartable    : Yes
    #
    #  Modified on      Modified by         Description
    #
    #############################################################################################*/
    IS
        lvc_jobname  VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_disable_fk';
        lvc_step     VARCHAR2(100);
        lvc_stepname VARCHAR2(1000);
        ln_seqnum    NUMBER;
        le_exception EXCEPTION;
    BEGIN

        IF (pvc_owner_in IS NULL OR pvc_table_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||pvc_table_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        gtbl_owner.DELETE;
        gtbl_tbl.DELETE;
        gtbl_degree.DELETE;
        gtbl_cnstr.DELETE;
        gtbl_r_owner.DELETE;
        gtbl_r_tbl.DELETE;
        gtbl_r_degree.DELETE;

        SELECT owner,table_name,degree,constraint_name,r_t_owner,r_table_name,r_degree
        BULK COLLECT INTO
            gtbl_owner,gtbl_tbl,gtbl_degree,gtbl_cnstr,gtbl_r_owner,gtbl_r_tbl,gtbl_r_degree
        FROM
            (SELECT ac.owner, ac.table_name,
                    to_number(DECODE(TRIM(t.degree),'DEFAULT','16',TRIM(t.degree)) ) degree,
                    ac.constraint_name,r_t.owner r_t_owner,r_t.table_name r_table_name,
                    to_number(DECODE(TRIM(r_t.degree),'DEFAULT','16',TRIM(r_t.degree)) ) r_degree
               FROM all_constraints ac,
                    all_tables t,
                    all_constraints r_ac,
                    all_tables r_t
               WHERE r_ac.owner = pvc_owner_in
                 AND r_ac.table_name = pvc_table_name_in
                 AND r_ac.constraint_type = 'P'
                 AND r_t.owner = r_ac.owner
                 AND r_t.table_name = r_ac.table_name
                 AND ac.r_constraint_name = r_ac.constraint_name
                 AND ac.constraint_type = 'R'
                 AND ac.status = 'ENABLED'
                 AND t.owner = ac.owner
                 AND t.table_name = ac.table_name
                 AND pi_include_children_in = 1
              UNION
             SELECT ac.owner,ac.table_name,
                    to_number(DECODE(TRIM(t.degree),'DEFAULT','16',TRIM(t.degree)) ) degree,
                    ac.constraint_name, r_t.owner r_t_owner, r_t.table_name r_table_name,
                    to_number(DECODE(TRIM(r_t.degree),'DEFAULT','16',TRIM(r_t.degree)) ) r_degree
               FROM all_constraints ac,
                    all_tables t,
                    all_constraints r_ac,
                    all_tables r_t
              WHERE ac.owner = pvc_owner_in
                AND ac.table_name = pvc_table_name_in
                AND ac.constraint_type = 'R'
                AND ac.status = 'ENABLED'
                AND t.owner = ac.owner
                AND t.table_name = ac.table_name
                AND r_ac.constraint_name = ac.r_constraint_name
                AND r_ac.owner = ac.r_owner
                AND r_t.owner = r_ac.owner
                AND r_t.table_name = r_ac.table_name);

        IF gtbl_owner.count > 0 THEN
            FOR i IN 1..gtbl_owner.count LOOP
                lvc_stepname :='ALTER TABLE ' || gtbl_owner(i) || '.' || gtbl_tbl(i) || ' DISABLE CONSTRAINT ' || gtbl_cnstr(i);
                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
                EXECUTE IMMEDIATE 'BEGIN '|| gtbl_owner(i) || '.p_exec_sql(''ALTER TABLE ' || gtbl_owner(i) || '.' ||
                                   gtbl_tbl(i) || ' DISABLE CONSTRAINT ' || gtbl_cnstr(i)|| '''); END;';
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
            END LOOP;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||pvc_table_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        COMMIT;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE ( lvc_jobname||' : First 2 parameters need to be Owner and Table.  Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed : First 2 parameters need to be Owner and Table.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20070, lvc_jobname||' : First 2 parameters need to be Owner and Table.  Aborting...' );
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                ROLLBACK;
                RAISE_APPLICATION_ERROR( -20070, lvc_jobname||' : '||SQLERRM(SQLCODE));
    END p_disable_fk;

    PROCEDURE p_enable_fk (pvc_owner_in            IN VARCHAR2,
                           pvc_table_name_in       IN VARCHAR2,
                           pi_include_children_in IN PLS_INTEGER DEFAULT 0,
                           pi_degree_in            IN PLS_INTEGER DEFAULT 16,
                           pvc_constraint_name_in  IN VARCHAR2 DEFAULT NULL,
                           pi_validate_in          IN PLS_INTEGER DEFAULT 1,
                           pvc_jobname_in          IN VARCHAR2 DEFAULT NULL,
                           pn_seqnum_in            IN NUMBER DEFAULT NULL)
    /*#############################################################################################
    #  Procedure Name : p_enable_FK
    #
    #  Description    : Enables FK constraints on a given table
    #
    #  Input          : pvc_owner_in, pvc_table_name_in, pi_include_children_in, pi_degree_in,
    #                   pvc_constraint_name_in, pi_validate_in,pvc_jobname_in, pn_seqnum_in
    #  Output         : NA
    #
    #  Author         : Andy Fritz
    #  Date created   : 10/14/2019
    #  Restartable    : Yes
    #
    #  Modified on      Modified by         Description
    #  02/14/2021     Andy Fritz       Added support for enabling child table constraints
    #
    #############################################################################################*/
    IS
        lvc_jobname    VARCHAR2(100) := 'STGMGR.PG_SPIN_LIB.p_enable_fk';
        lvc_step       VARCHAR2(100);
        lvc_stepname   VARCHAR2(1000);
        ln_seqnum      NUMBER;
        le_exception   EXCEPTION;
    BEGIN
        IF (pvc_owner_in IS NULL OR pvc_table_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||pvc_table_name_in||' constraint='||pvc_constraint_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DML PARALLEL ' || pi_degree_in;
        EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DDL PARALLEL ' || pi_degree_in;

        IF gtbl_owner.count = 0 THEN
            SELECT owner, table_name, degree, constraint_name, r_t_owner, r_table_name, r_degree
            BULK COLLECT INTO
                   gtbl_owner,gtbl_tbl,gtbl_degree,gtbl_cnstr,gtbl_r_owner,gtbl_r_tbl,gtbl_r_degree
            FROM (SELECT ac.owner, ac.table_name,
                         to_number(DECODE(TRIM(t.degree),'DEFAULT','16',TRIM(t.degree)) ) degree,
                         ac.constraint_name,r_t.owner r_t_owner,r_t.table_name r_table_name,
                         to_number(DECODE(TRIM(r_t.degree),'DEFAULT','16',TRIM(r_t.degree)) ) r_degree
                    FROM all_constraints ac,
                         all_tables t,
                         all_constraints r_ac,
                         all_tables r_t
                    WHERE r_ac.owner = pvc_owner_in
                      AND r_ac.table_name = pvc_table_name_in
                      AND r_ac.constraint_type = 'P'
                      AND r_t.owner = r_ac.owner
                      AND r_t.table_name = r_ac.table_name
                      AND ac.r_constraint_name = r_ac.constraint_name
                      AND ac.constraint_type = 'R'
                      AND ac.status <> 'ENABLED'
                      AND t.owner = ac.owner
                      AND t.table_name = ac.table_name
                      AND pi_include_children_in = 1
                   UNION
                 SELECT ac.owner, ac.table_name, to_number(DECODE(TRIM(t.degree),'DEFAULT','16',TRIM(t.degree)) ) degree,
                        ac.constraint_name,r_t.owner r_t_owner,r_t.table_name r_table_name,
                        to_number(DECODE(TRIM(r_t.degree),'DEFAULT','16',TRIM(r_t.degree)) ) r_degree
                   FROM all_constraints ac,
                        all_tables t,
                        all_constraints r_ac,
                        all_tables r_t
                  WHERE ac.owner = pvc_owner_in
                    AND ac.table_name = pvc_table_name_in
                    AND ac.constraint_type = 'R'
                    AND ac.constraint_name = nvl(pvc_constraint_name_in,ac.constraint_name)
                    AND ac.status <> 'ENABLED'
                    AND t.owner = ac.owner
                    AND t.table_name = ac.table_name
                    AND r_ac.constraint_name = ac.r_constraint_name
                    AND r_ac.owner = ac.r_owner
                    AND r_t.owner = r_ac.owner
                    AND r_t.table_name = r_ac.table_name);
        END IF;

        IF gtbl_owner.count > 0 THEN
            FOR i IN 1..gtbl_owner.count LOOP
                lvc_stepname:='ALTER TABLE ' || gtbl_owner(i) || '.' || gtbl_tbl(i) || ' ENABLE NOVALIDATE CONSTRAINT ' || gtbl_cnstr(i);
                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                EXECUTE IMMEDIATE 'BEGIN '|| gtbl_owner(i)|| '.p_exec_sql(''ALTER TABLE '|| gtbl_owner(i)|| '.'||
                        gtbl_tbl(i)|| ' ENABLE NOVALIDATE CONSTRAINT '|| gtbl_cnstr(i)|| ''');END;';

                IF pi_validate_in <> 0 THEN
                    EXECUTE IMMEDIATE 'BEGIN -- set parallel degree on both,parent and child tables'
                       || gtbl_r_owner(i)|| '.p_exec_sql(''ALTER TABLE '|| gtbl_r_owner(i)|| '.'|| gtbl_r_tbl(i)|| ' PARALLEL(DEGREE '|| pi_degree_in|| ')'');'
                       || gtbl_owner(i)|| '.p_exec_sql(''ALTER TABLE '|| gtbl_owner(i)|| '.'|| gtbl_tbl(i)|| ' PARALLEL(DEGREE '|| pi_degree_in|| ')'');-- enable constraint with validate'
                       || gtbl_owner(i)|| '.p_exec_sql(''ALTER TABLE '|| gtbl_owner(i)|| '.'|| gtbl_tbl(i)|| ' MODIFY CONSTRAINT '|| gtbl_cnstr(i)|| ' VALIDATE'');
                             -- restore parallel degree on both,parent and child tables'
                       || gtbl_owner(i)|| '.p_exec_sql(''ALTER TABLE '|| gtbl_owner(i)|| '.'|| gtbl_tbl(i)|| ' '
                       || CASE
                            WHEN gtbl_degree(i) <= 1 THEN 'NOPARALLEL'
                            ELSE 'PARALLEL(DEGREE ' || gtbl_degree(i) || ')'
                          END
                       || ' '');
                             '
                       || gtbl_r_owner(i)|| '.p_exec_sql(''ALTER TABLE '|| gtbl_r_owner(i)|| '.'|| gtbl_r_tbl(i)|| ' '||
                          CASE WHEN gtbl_r_degree(i) <= 1 THEN 'NOPARALLEL' ELSE 'PARALLEL(DEGREE ' || gtbl_r_degree(i) || ')'END|| ''');END;';
                END IF;

                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
            END LOOP;
        END IF;
        COMMIT;

        gtbl_owner.DELETE;
        gtbl_tbl.DELETE;
        gtbl_degree.DELETE;
        gtbl_cnstr.DELETE;
        gtbl_r_owner.DELETE;
        gtbl_r_tbl.DELETE;
        gtbl_r_degree.DELETE;

        COMMIT;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||pvc_table_name_in||' constraint='||pvc_constraint_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE ( lvc_jobname||' : First 2 parameters need to be Owner and Table.  Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed : First 2 parameters need to be Owner and Table.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20080, lvc_jobname||' : First 2 parameters need to be Owner and Table.  Aborting...' );
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                ROLLBACK;
                RAISE_APPLICATION_ERROR( -20080, lvc_jobname||' : '||SQLERRM(SQLCODE));
    END p_enable_fk;

    PROCEDURE p_cascade_delete (pvc_main_owner_in  IN  VARCHAR2,
                                pvc_main_table_in  IN  VARCHAR2,
                                pvc_main_where_in  IN  VARCHAR2,
                                pi_degree_in       IN  PLS_INTEGER DEFAULT 16,
                                pi_validate_in     IN  PLS_INTEGER DEFAULT 1,
                                pn_del_cnt_out     OUT NUMBER,
                                pvc_jobname_in     IN  VARCHAR2 DEFAULT NULL,
                                pn_seqnum_in       IN  NUMBER DEFAULT NULL)
    /*#############################################################################################
    #  Procedure Name : p_cascade_delete
    #
    #  Description    : Removes rows from a specified table cascading down to all related tables
    #                   Will disable triggers on the main or any related tables and will re-enable
    #                   them unless they were disabled before starting this procedure
    #                   p_main_where has to include "WHERE" token
    #                   Will recalculate stats only in number of records removed is bigger than ALL_TABLES.num_rows * 0.2
    #
    #  Input          : pvc_main_owner_in, pvc_main_table_in, p_main_where, pi_degree_in, pi_validate_in,
    #                   pn_del_cnt_out, pvc_jobname_in, pn_seqnum_in
    #  Output         : NA
    #
    #  Author         : Andy Fritz
    #  Date created   : 10/14/2019
    #  Restartable    : Yes
    #
    #  Modified on      Modified by         Description
    #
    #############################################################################################*/
    IS
        -- c_CONS_TREE returns a list of tables and connections between them
        -- sorted by depth of connection
        CURSOR c_cons_tree (pvc_main_owner_in VARCHAR2, pvc_table_name_in VARCHAR2)
        IS
            WITH all_cons AS ( SELECT /*+ MATERIALIZE */
                                        ac.owner,ac.table_name,ac.constraint_name,
                                        ac.constraint_type,ac.r_owner,ac.r_constraint_name,
                                        LISTAGG(acc.column_name,',')
                                        WITHIN GROUP(ORDER BY acc.position) column_name
                                 FROM all_constraints ac,
                                      all_cons_columns acc
                                WHERE acc.owner = ac.owner
                                  AND acc.table_name = ac.table_name
                                  AND acc.constraint_name = ac.constraint_name
                                  AND ac.constraint_type IN ('R','U','P')
                                GROUP BY ac.owner,ac.table_name,ac.constraint_name,
                                         ac.constraint_type,ac.r_owner,ac.r_constraint_name),
                 all_cons_rows AS (SELECT /*+ MATERIALIZE */
                                            ac.*, t.num_rows num_rows
                                     FROM all_cons ac,
                                          all_tables t
                                     WHERE t.owner = ac.owner
                                       AND t.table_name = ac.table_name),
                cons_tree (lvl, s_owner, s_table_name, s_constraint_name, s_column_name,ctc, t_owner, t_table_name, t_constraint_name, t_column_name,s_num_rows, t_num_rows)
                               AS (SELECT 1 lvl, s_acc.owner s_owner, s_acc.table_name s_table_name,
                                          s_acc.constraint_name s_constraint_name, s_acc.column_name s_column_name,
                                          s_acc.table_name || '.'|| s_acc.column_name ctc,
                                          t_acc.owner t_owner, t_acc.table_name t_table_name,
                                          t_acc.constraint_name t_constraint_name,
                                          t_acc.column_name t_column_name,
                                          s_acc.num_rows s_num_rows,
                                          t_acc.num_rows t_num_rows
                                     FROM all_cons_rows s_acc,
                                          all_cons_rows t_acc
                                    WHERE s_acc.owner = pvc_main_owner_in
                                      AND s_acc.table_name = pvc_main_table_in
                                      AND s_acc.constraint_type IN ('P')
                                      AND t_acc.constraint_type = 'R'
                                      AND t_acc.r_constraint_name = s_acc.constraint_name
                                      AND t_acc.r_owner = s_acc.owner
                                   UNION ALL
                                   SELECT p.lvl + 1 lvl,
                                          s_acc.owner s_owner,
                                          s_acc.table_name s_table_name,
                                          s_acc.constraint_name s_constraint_name,
                                          s_acc.column_name s_column_name,
                                          s_acc.table_name|| '.'|| s_acc.column_name ctc,
                                          t_acc.owner t_owner,
                                          t_acc.table_name t_table_name,
                                          t_acc.constraint_name t_constraint_name,
                                          t_acc.column_name t_column_name,
                                          s_acc.num_rows s_num_rows,
                                          t_acc.num_rows t_num_rows
                                     FROM cons_tree p,
                                          all_cons_rows s_acc,
                                          all_cons_rows t_acc
                                    WHERE s_acc.owner = p.t_owner
                                      AND s_acc.table_name = p.t_table_name
                                      AND s_acc.constraint_type IN ('U','P')
                                      AND t_acc.constraint_type = 'R'
                                      AND t_acc.r_constraint_name = s_acc.constraint_name
                                      AND t_acc.r_owner = s_acc.owner)
                        SEARCH DEPTH FIRST BY lvl SET id CYCLE ctc SET is_cycle TO '1' DEFAULT '0'

                        SELECT ct.*, ROW_NUMBER() OVER (PARTITION BY ct.t_owner, ct.t_table_name ORDER BY ct.lvl, ct.id ASC) row_nbr
                          FROM cons_tree ct
                         WHERE is_cycle = 0
                        ORDER BY lvl, id ASC;

        TYPE ltyp_cons_tree_tbl IS TABLE OF c_cons_tree%rowtype;
        ltbl_cons_tree  ltyp_cons_tree_tbl := ltyp_cons_tree_tbl ();
        lcn_calc_stats_trshld   CONSTANT NUMBER :=0.2;
        lvc_ret                 VARCHAR2(255);
        lvc_trg                 VARCHAR2(255);
        t_row_cnt               gtbl_trg_tbl;
        lvc_jobname             VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_cascade_delete';
        lvc_stepname            VARCHAR2(4000);
        lvc_step                VARCHAR2(4000);
        lvc_sqltext             VARCHAR2(4000);
        lvc_prev_table          VARCHAR2(30);
        li_cnt_del              PLS_INTEGER;
        li_tot_cnt_del          PLS_INTEGER;
        li_tot_cnt              PLS_INTEGER;
        ln_seqnum               NUMBER;
    BEGIN
        pn_del_cnt_out := 0;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||pvc_main_table_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DML PARALLEL ' || pi_degree_in;
        EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DDL PARALLEL ' || pi_degree_in;

        -- Wipe out any trigger names from the gtbl_trg collection.
        -- The p_alter_trigger will add to it if a trigger was disabled
        -- so that it can be re-enabled later.
        gtbl_trg.DELETE;

        -- Get all related tables and connections in a collection
        OPEN c_cons_tree(pvc_main_owner_in,pvc_main_table_in);
        FETCH c_cons_tree BULK COLLECT INTO ltbl_cons_tree;
        CLOSE c_cons_tree;

        -- Disable constraints and triggers
        lvc_prev_table:='';

        FOR i IN 1..ltbl_cons_tree.count LOOP
            lvc_stepname:='ALTER TABLE '||ltbl_cons_tree(i).t_table_name||'-'||ltbl_cons_tree(i).row_nbr|| ' DISABLE CONSTRAINT '|| ltbl_cons_tree(i).t_constraint_name||' AND DISABLE TRIGGERS';
            --lvc_stepname := 'Disable constraints for '||ltbl_cons_tree(i).t_table_name;
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

            p_alter_constraint ('DISABLE',ltbl_cons_tree(i).t_table_name, ltbl_cons_tree(i).t_constraint_name,ltbl_cons_tree(i).t_owner,'R',NVL(pvc_jobname_in,lvc_jobname),ln_seqnum);

            IF (ltbl_cons_tree(i).t_table_name <> lvc_prev_table) THEN
                FOR rs_trigger IN (SELECT owner, trigger_name
                                     FROM all_triggers
                                    WHERE owner = ltbl_cons_tree(i).t_owner
                                      AND table_name = ltbl_cons_tree(i).t_table_name)
                LOOP
                    p_disable_trigger (rs_trigger.owner,rs_trigger.trigger_name,lvc_jobname,pn_seqnum_in);
                END LOOP;
            END IF;
            lvc_prev_table:=ltbl_cons_tree(i).t_table_name;

            -- Initialize deleted row counters
            t_row_cnt(ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name) := 0;

            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
        END LOOP;

        -- Disable Triggers For Main Table
        FOR rs_trigger IN (SELECT owner, trigger_name
                             FROM ALL_TRIGGERS
                            WHERE owner      = pvc_main_owner_in
                              AND table_name = pvc_main_table_in)
        LOOP
            p_disable_trigger (rs_trigger.owner,rs_trigger.trigger_name,lvc_jobname,pn_seqnum_in);
        END LOOP;

        -- remove rows from the main table
        dbms_application_info.set_client_info('0 ' || pvc_main_owner_in || '.' || pvc_main_table_in);

        lvc_sqltext :='DELETE /*+ PARALLEL('|| pi_degree_in|| ') */ FROM '|| pvc_main_owner_in|| '.'|| pvc_main_table_in|| ' t '|| pvc_main_where_in;
        --dbms_output.put_line(lvc_sqltext);
        lvc_stepname:=lvc_sqltext;
        --lvc_stepname:='Delete From '||pvc_main_owner_in||'.'||pvc_main_table_in;
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

        EXECUTE IMMEDIATE lvc_sqltext;
        li_cnt_del := SQL%rowcount;
        li_tot_cnt_del := li_cnt_del;
        t_row_cnt(pvc_main_owner_in || '.' || pvc_main_table_in) := li_cnt_del;

        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,li_cnt_del,0,li_cnt_del);

        COMMIT;
        li_tot_cnt := ltbl_cons_tree.count;

        -- Remove data
        FOR i IN 1..ltbl_cons_tree.count LOOP
            -- Only remove rows from "child" tables if there was anything removed from "parent"
            li_cnt_del := 0;
            IF t_row_cnt(ltbl_cons_tree(i).s_owner|| '.'|| ltbl_cons_tree(i).s_table_name) > 0 THEN
                dbms_application_info.set_client_info(i|| '/'|| li_tot_cnt|| ' '|| ltbl_cons_tree(i).lvl|| ' '|| ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name|| '.'|| ltbl_cons_tree(i).t_column_name);
                lvc_stepname:='DELETE /*+ PARALLEL('|| pi_degree_in|| ') */ FROM '|| ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name
                    || ' t WHERE ('|| ltbl_cons_tree(i).t_column_name || ') NOT IN (SELECT /*+ PARALLEL('|| pi_degree_in || ') */ '|| ltbl_cons_tree(i).s_column_name
                    || ' FROM '|| ltbl_cons_tree(i).s_owner|| '.'|| ltbl_cons_tree(i).s_table_name|| ' s)';
                --lvc_stepname:='Delete From '|| ltbl_cons_tree(i).t_table_name;

                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                EXECUTE IMMEDIATE 'DELETE /*+ PARALLEL('|| pi_degree_in|| ') */ FROM '|| ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name
                    || ' t WHERE ('|| ltbl_cons_tree(i).t_column_name || ') NOT IN (SELECT /*+ PARALLEL('|| pi_degree_in || ') */ '|| ltbl_cons_tree(i).s_column_name
                    || ' FROM '|| ltbl_cons_tree(i).s_owner|| '.'|| ltbl_cons_tree(i).s_table_name|| ' s)';

                li_cnt_del := SQL%ROWCOUNT;
                li_tot_cnt_del := li_tot_cnt_del + li_cnt_del;
                t_row_cnt(ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name) := t_row_cnt(ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name) + li_cnt_del;

                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,li_cnt_del,0,li_cnt_del);

                COMMIT;
            END IF;
        END LOOP;

        SELECT num_rows INTO li_cnt_del
          FROM all_tables
         WHERE owner = pvc_main_owner_in
           AND table_name = pvc_main_table_in;

        IF t_row_cnt(pvc_main_owner_in || '.' || pvc_main_table_in) >= nvl(li_cnt_del,0) * lcn_calc_stats_trshld THEN
            lvc_stepname:='Calculate stats and re-enable disabled triggers';
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

            EXECUTE IMMEDIATE 'SELECT '|| pvc_main_owner_in|| '.f_compute_table_stats('''|| pvc_main_owner_in|| ''','''|| pvc_main_table_in|| ''','|| pi_degree_in|| ') FROM DUAL' INTO lvc_ret;

            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
        END IF;

        -- Enable Triggers For Main Table
        FOR rs_trigger IN (SELECT owner, trigger_name
                             FROM ALL_TRIGGERS
                            WHERE owner      = pvc_main_owner_in
                              AND table_name = pvc_main_table_in)
        LOOP
            p_enable_trigger (rs_trigger.owner,rs_trigger.trigger_name,lvc_jobname,pn_seqnum_in);
        END LOOP;

        lvc_prev_table:='';
        FOR i IN 1..ltbl_cons_tree.count LOOP

            -- Calc stats and enable triggers only on a first occurance of table name in the list
            -- Some tables may have more than one FK to the same source table (ie. DFUMAP)
            -- Also, calculate stats only if more than 20% of rows were removed
            -- (as compared with ALL_TABLES.num_rows populated by calc stats).
            IF ltbl_cons_tree(i).row_nbr = 1 THEN

                lvc_stepname:='Calculate stats and re-enabling triggers'||ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name;
                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                dbms_application_info.set_client_info(i|| '/'|| li_tot_cnt|| ' '|| ltbl_cons_tree(i).lvl|| ' '|| ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name|| ' stats');

                IF t_row_cnt(ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name) >= nvl(ltbl_cons_tree(i).t_num_rows,0) * lcn_calc_stats_trshld THEN
                    EXECUTE IMMEDIATE 'SELECT '|| ltbl_cons_tree(i).t_owner|| '.f_compute_table_stats('''|| ltbl_cons_tree(i).t_owner|| ''','''|| ltbl_cons_tree(i).t_table_name|| ''','|| pi_degree_in|| ') FROM DUAL' INTO lvc_ret;
                END IF;

                IF (ltbl_cons_tree(i).t_table_name <> lvc_prev_table) THEN
                    FOR rs_trigger IN (SELECT owner, trigger_name
                                         FROM ALL_TRIGGERS
                                        WHERE owner      = ltbl_cons_tree(i).t_owner
                                          AND table_name = ltbl_cons_tree(i).t_table_name)
                    LOOP
                        p_enable_trigger (rs_trigger.owner,rs_trigger.trigger_name,lvc_jobname,pn_seqnum_in);
                    END LOOP;
                END IF;
                lvc_prev_table:=ltbl_cons_tree(i).t_table_name;

                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
            END IF;
        END LOOP;

        gtbl_trg.DELETE;

        -- re-enable constraints
        FOR i IN 1..ltbl_cons_tree.count LOOP
            lvc_stepname:='Re-enable Constraints'||ltbl_cons_tree(i).t_owner||'.'||ltbl_cons_tree(i).t_table_name;
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

            dbms_application_info.set_client_info(i|| '/'|| li_tot_cnt|| ' '|| ltbl_cons_tree(i).lvl|| ' '|| ltbl_cons_tree(i).t_owner|| '.'|| ltbl_cons_tree(i).t_table_name|| '.'|| ltbl_cons_tree(i).t_column_name|| ' enable FK');
            p_alter_constraint ('ENABLE',ltbl_cons_tree(i).t_table_name,ltbl_cons_tree(i).t_constraint_name,ltbl_cons_tree(i).t_owner,'R',NVL(pvc_jobname_in,lvc_jobname), ln_seqnum);
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
        END LOOP;

        pn_del_cnt_out := li_tot_cnt_del;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||pvc_main_table_in,0,0,0,li_tot_cnt_del,0,li_tot_cnt_del);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,li_tot_cnt_del);
        END IF;

        COMMIT;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            ROLLBACK;
            RAISE_APPLICATION_ERROR (-20090, lvc_jobname||' : '||SQLERRM(SQLCODE));
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            ROLLBACK;
            lvc_trg := gtbl_trg.first;
            WHILE lvc_trg IS NOT NULL
            LOOP
                EXECUTE IMMEDIATE 'BEGIN ' || regexp_replace(lvc_trg,'(\S+)\.\S+','\1') || '.p_exec_sql(''ALTER TRIGGER '|| lvc_trg|| ' ENABLE''); END;';
                lvc_trg := gtbl_trg.next(lvc_trg);
            END LOOP;
            gtbl_trg.DELETE;
            raise_application_error(-20090,dbms_utility.format_error_stack() || dbms_utility.format_error_backtrace);
    END p_cascade_delete;

    PROCEDURE p_register_constraint (pvc_tablename_in         IN VARCHAR2,
                                     pvc_constraint_name_in   IN VARCHAR2,
                                     pvc_owner_in             IN VARCHAR2,
                                     pvc_jobname_in           IN VARCHAR2 DEFAULT NULL,
                                     pn_seqnum_in             IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_register_constraint
    #
    # Description : This procedure is used to enter a constraint
    #               into the BATCH_INDEX_DDL table.
    #
    # Input       : pvc_tablename_in - Name of table to process
    #               pvc_constraint_name_in - Name of constraint to process
    #               pvc_owner_in - Owner of table to process
    #               pvc_jobname_in - Job name to log to
    #               pn_seqnum_in - Sequence number of process to log to
    #
    # Output      : NA
    #
    # Author      : Andy Fritz
    # Date created: 03/11/2019
    # Restartable : Yes
    #
    # Modified on      Modified by         Description
    #
    ###########################################################################################*/
    AS
        lvc_jobname     VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_register_constraint';
        lvc_step        VARCHAR2(300);
        lvc_stepname    VARCHAR2(1000);
        ln_seqnum       NUMBER;
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for constraint='||pvc_constraint_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        -- Insert constraint into BATCH_INDEX_DDL if it doesn't already exist
        lvc_stepname := 'Merge constraint ' || pvc_constraint_name_in || ' into BATCH_INDEX_DDL';
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
        MERGE INTO STGMGR.batch_index_ddl i
        USING (SELECT pvc_jobname_in process_name,
                      pvc_constraint_name_in index_name,
                      pvc_owner_in index_owner,
                      2 is_pk_fl,
                      pvc_tablename_in table_name,
                      pvc_owner_in table_owner,
                      NULL index_ddl,
                      'DROP' batch_start,
                      'CREATE' batch_end,
                      NULL batch_disable_flag
                 FROM dual) x
        ON (i.index_name = x.index_name)
        WHEN NOT MATCHED THEN INSERT
             (process_name, index_name, index_owner, is_pk_fl, table_name,
              table_owner, index_ddl, batch_start, batch_end, batch_disable_flag)
        VALUES
             (x.process_name, x.index_name, x.index_owner, x.is_pk_fl, x.table_name,
              x.table_owner, x.index_ddl, x.batch_start, x.batch_end, x.batch_disable_flag);
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,SQL%ROWCOUNT,0,0,0,0);

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for constraint='||pvc_constraint_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            ROLLBACK;
            RAISE_APPLICATION_ERROR( -20001,lvc_jobname||' : '||SQLERRM(SQLCODE));
    END p_register_constraint;


    PROCEDURE p_alter_constraint (pvc_alter_in             IN VARCHAR2,
                                  pvc_tablename_in         IN VARCHAR2 DEFAULT NULL,
                                  pvc_constraint_name_in   IN VARCHAR2 DEFAULT NULL,
                                  pvc_owner_in             IN VARCHAR2 DEFAULT 'SCPOMGR',
                                  pvc_constraint_type_in   IN VARCHAR2 DEFAULT 'R',
                                  pvc_jobname_in           IN VARCHAR2 DEFAULT NULL,
                                  pn_seqnum_in             IN NUMBER DEFAULT NULL,
                                  pvc_validate_in          IN VARCHAR2 DEFAULT 'NOVALIDATE')
    /*##########################################################################################
    # Procedure Name : p_alter_constraint
    #
    # Description : This Procedure is used to alter a constraint;
    #               Constraint can be either Enabled Or Disabled based on the parameter;
    #               If both pvc_tablename_in and pvc_constraint_name_in are not null then only constraint passed
    #               as variable will be altered.  If pvc_constraint_name_in is not passed, then
    #               all of the constraints on the table will be altered.
    #
    # Input       : pvc_alter_in - 'ENABLE' or 'DISABLE'
    #               pvc_tablename_in - Name of table to process
    #               pvc_constraint_name_in - Name of constraint to process
    #               pvc_owner_in - Owner of table to process
    #               pvc_constraint_type_in - 'R' for referential constraint,
    #               pvc_jobname_in - Job name to log to
    #               pn_seqnum_in - Sequence number of process to log to
    #               pvc_validate_in - 'VALIDATE' or 'NOVALIDATE'
    #
    # Output      : NA
    #
    # Author      : Andy Fritz
    # Date created: 10/14/2019
    # Restartable : Yes
    #
    # Modified on      Modified by         Description
    #
    ###########################################################################################*/
    AS
        lvc_jobname     VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_alter_constraint';
        lvc_tablename   VARCHAR2(30):= NULL;
        lvc_owner       VARCHAR2(30);
        lvc_step        VARCHAR2(300);
        lvc_stepname    VARCHAR2(1000) := 'Start';
        lvc_sql_text    VARCHAR2(3000);
        ln_seqnum       NUMBER;
        le_exception    EXCEPTION;
        ln_pmi_modified NUMBER:=0;
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||pvc_tablename_in||' constraint='||pvc_constraint_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        IF (pvc_owner_in IS NULL) THEN
           lvc_owner := 'SCPOMGR';
        ELSE
           lvc_owner := pvc_owner_in;
        END IF;

        IF (pvc_alter_in IS NULL) THEN
            RAISE le_exception;
        ELSE
            IF (pvc_tablename_in IS NULL AND pvc_constraint_name_in IS NULL) THEN
                RAISE le_exception;
            ELSE
                IF (pvc_tablename_in IS NOT NULL AND pvc_constraint_name_in IS NOT NULL) THEN
                    lvc_sql_text    := 'ALTER TABLE '|| lvc_owner ||'.'|| pvc_tablename_in|| ' '|| pvc_alter_in|| ' ' || pvc_validate_in || ' CONSTRAINT '|| pvc_constraint_name_in;
                    lvc_stepname    := lvc_sql_text;
                    lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                    ln_pmi_modified := f_is_pmi_modified (lvc_owner, pvc_tablename_in,pvc_constraint_name_in,NVL(pvc_jobname_in,lvc_jobname),ln_seqnum);
                    IF (ln_pmi_modified = 0) THEN
                        STGMGR.pg_spin_lib.p_register_constraint(pvc_tablename_in, pvc_constraint_name_in, lvc_owner, NVL(pvc_jobname_in,lvc_jobname),ln_seqnum);
                        EXECUTE IMMEDIATE lvc_sql_text;
                        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
                    ELSE
                        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Constraint modified by p_maintain_indexes, no action taken',0,0,0,0,0,0);
                    END IF;
                ELSE
                    IF (pvc_tablename_in IS NULL AND pvc_constraint_name_in IS NOT NULL) THEN
                        BEGIN
                            SELECT table_name INTO lvc_tablename
                              FROM dba_constraints
                             WHERE owner='SCPOMGR'
                               AND constraint_type = pvc_constraint_type_in
                               AND constraint_name = pvc_constraint_name_in
                               AND status NOT LIKE pvc_alter_in||'%';
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                NULL;
                        END;

                        IF lvc_tablename IS NOT NULL THEN
                            lvc_sql_text := 'ALTER TABLE '|| lvc_owner ||'.'|| lvc_tablename|| ' '|| pvc_alter_in|| ' ' || pvc_validate_in || ' CONSTRAINT '|| pvc_constraint_name_in;
                            lvc_stepname := lvc_sql_text;
                            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                            ln_pmi_modified := f_is_pmi_modified (lvc_owner, lvc_tablename, pvc_constraint_name_in, NVL(pvc_jobname_in,lvc_jobname),ln_seqnum);
                            IF (ln_pmi_modified = 0) THEN
                                STGMGR.pg_spin_lib.p_register_constraint(lvc_tablename, pvc_constraint_name_in, lvc_owner, NVL(pvc_jobname_in,lvc_jobname),ln_seqnum);
                                EXECUTE IMMEDIATE lvc_sql_text;
                                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
                            ELSE
                                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Constraint modified by p_maintain_indexes, no action taken',0,0,0,0,0,0);
                            END IF;
                        END IF;
                    ELSE
                        IF (pvc_tablename_in IS NOT NULL AND pvc_constraint_name_in IS NULL) THEN
                            FOR rec IN (SELECT constraint_name
                                          FROM dba_constraints
                                         WHERE owner = lvc_owner
                                           AND table_name = pvc_tablename_in
                                           AND constraint_type = pvc_constraint_type_in
                                           AND status NOT LIKE pvc_alter_in||'%')
                            LOOP
                                lvc_sql_text := 'ALTER TABLE '|| lvc_owner ||'.'|| pvc_tablename_in|| ' '|| pvc_alter_in|| ' ' || pvc_validate_in || ' CONSTRAINT '|| REC.CONSTRAINT_NAME;
                                lvc_stepname := lvc_sql_text;
                                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                                ln_pmi_modified := f_is_pmi_modified (lvc_owner, pvc_tablename_in, REC.CONSTRAINT_NAME, NVL(pvc_jobname_in,lvc_jobname),ln_seqnum);
                                IF (ln_pmi_modified = 0) THEN
                                    STGMGR.pg_spin_lib.p_register_constraint(pvc_tablename_in, REC.CONSTRAINT_NAME, lvc_owner, NVL(pvc_jobname_in,lvc_jobname),ln_seqnum);
                                    EXECUTE IMMEDIATE lvc_sql_text;
                                    STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
                                ELSE
                                    STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Constraint modified by p_maintain_indexes, no action taken',0,0,0,0,0,0);
                                END IF;
                            END LOOP;
                        END IF;
                    END IF;
                END IF;
            END IF;
        END IF;

        COMMIT;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||pvc_tablename_in||' constraint='||pvc_constraint_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION WHEN le_exception THEN
            IF (pvc_alter_in IS NULL) THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : First parameter must be ENABLE or DISABLE.  Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed: First parameter must be ENABLE or DISABLE.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20100, lvc_jobname||' : First parameter must be ENABLE or DISABLE.  Aborting...' );
            ELSE
                IF (pvc_tablename_in IS NULL AND pvc_constraint_name_in IS NULL) THEN
                    DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : Second or third parameter must be populated with tablename or constraint name.  Aborting...');
                    STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed: Second or third parameter must be populated with tablename or constraint name.  Aborting...',1,0,0,0,0,0);
                    IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                        STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                    END IF;
                    RAISE_APPLICATION_ERROR( -20100, lvc_jobname||' : Second or third parameter must be populated with tablename or constraint name.  Aborting...' );
                END IF;
            END IF;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||':'||lvc_stepname||':'||SQLERRM(SQLCODE));
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                ROLLBACK;
                RAISE_APPLICATION_ERROR( -20100,lvc_jobname||':'||lvc_stepname||':'||SQLERRM(SQLCODE));
    END p_alter_constraint;


    PROCEDURE p_alter_user_account (pvc_switchparam_in  IN VARCHAR2,
                                    pvc_jobname_in      IN VARCHAR2 DEFAULT NULL,
                                    pn_seqnum_in        IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_alter_user_account
    #
    # Description    : This procedure is used either to Suspend or Activate the user at the time
    #                  of batch processing.  If the pvc_switchparam_in is SUSPEND, the procedure
    #                  will change the account status (WWFMGR.CSM_ACCOUNT_STATUS table) to SUSPEND.
    #                  At the time of suspending the users, the list of users who are getting suspended
    #                  will be written to the table gt_usr_acct table.  If the pvc_switchparam_in is ACTIVATE,
    #                  the procedure will activate the users. At the time of activating the user,
    #                  the users will be retrieved from table gt_usr_acct to change the status to ACTIVE.
    #
    # Input          : pvc_switchparam_in (SUSPEND/ACTIVATE), pvc_jobname_in, pn_seqnum_in
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on      Modified by         Description
    # Nov 01 2021      Andy Fritz          Removed batchdate check for re-activition
    #
    ###########################################################################################*/
    AS
        lvc_jobname     VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_alter_user_account';
        lvc_step        VARCHAR2(100);
        lvc_stepname    VARCHAR2(1000);
        lvc_sqltext     VARCHAR2(3000);
        ln_seqnum       NUMBER;
        le_exception    EXCEPTION;
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for parm='||pvc_switchparam_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        IF (pvc_switchparam_in = 'SUSPEND' OR pvc_switchparam_in = 'ACTIVATE') THEN
            IF ( pvc_switchparam_in = 'SUSPEND' ) THEN

                lvc_stepname := 'Suspending active users within JDA before batch run';
                lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                lvc_sqltext := 'insert into STGMGR.batch_userswitch (user_name, batchdate, switch_param) '
                             ||' select distinct cu.user_name, '''||f_get_batch_date()||''', ''' || pvc_switchparam_in || ''''
                             ||' from wwfmgr.csm_user cu, wwfmgr.csm_account_status cas '
                             ||' where cu.user_name = cas.user_name and cas.status_code=''ACTIVE'''
                             ||' and cu.user_name not in ('||f_get_param('RETAIN_USERS_FOR_BATCH',NVL(pvc_jobname_in,lvc_jobname),ln_seqnum)||')';
                EXECUTE IMMEDIATE lvc_sqltext;

                UPDATE WWFMGR.CSM_ACCOUNT_STATUS
                SET STATUS_CODE='SUSPENDED'
                WHERE USER_NAME IN (SELECT DISTINCT USER_NAME
                                      FROM STGMGR.BATCH_USERSWITCH
                                     WHERE SWITCH_PARAM = pvc_switchparam_in
                                       AND TRUNC(batchdate) = TRUNC(f_get_batch_date ()));
                COMMIT;
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,SQL%ROWCOUNT,0,0,SQL%ROWCOUNT);
            ELSE
                IF (pvc_switchparam_in = 'ACTIVATE') THEN
                    lvc_stepname := 'Activating suspended users within JDA after batch run';
                    lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                    UPDATE WWFMGR.CSM_ACCOUNT_STATUS
                       SET STATUS_CODE='ACTIVE'
                     WHERE STATUS_CODE <> 'ACTIVE'
                       AND USER_NAME IN (SELECT DISTINCT USER_NAME
                                           FROM STGMGR.BATCH_USERSWITCH
                                          WHERE SWITCH_PARAM = 'SUSPEND'
                                            --AND TRUNC(batchdate) = TRUNC(f_get_batch_date())  -- AF Nov 1 2021
                                            );

                    STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,SQL%ROWCOUNT,0,0,SQL%ROWCOUNT);

                    lvc_stepname := 'Delete From Temp Table BATCH_USERSWITCH';
                    lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

                    DELETE FROM STGMGR.BATCH_USERSWITCH
                    WHERE SWITCH_PARAM='SUSPEND'
                      --AND TRUNC(batchdate) = TRUNC(f_get_batch_date())  -- AF Nov 1 2021
                      ;

                    STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,SQL%ROWCOUNT,0,SQL%ROWCOUNT);
                    COMMIT;
                END IF;
            END IF;
        ELSE
            RAISE le_exception;
        END IF;

        COMMIT;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for parm='||pvc_switchparam_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : First parameter must be SUSPEND or ACTIVATE.  Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed: First parameter must be SUSPEND or ACTIVATE.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20110, lvc_jobname||' : First parameter must be SUSPEND or ACTIVATE.  Aborting...' );
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                ROLLBACK;
                RAISE_APPLICATION_ERROR( -20110,lvc_jobname||' : '||SQLERRM(SQLCODE));
    END p_alter_user_account;

    PROCEDURE p_sre_cleanup (pvc_jobname_in  IN VARCHAR2 DEFAULT NULL,
                             pn_seqnum_in    IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_sre_cleanup
    #
    # Description    : This procedure is used to clean up the SRE_JOB's related data which was logged by JDA
    #                  within SRE related tables for which SRE processes were executed in the past.  This
    #                  procedure internally calls the p_cascade_delete procedure for deleting the data.
    #
    # Input          : pvc_jobname_in, pn_seqnum_in.
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on      Modified by         Description
    #
    ###########################################################################################*/
    AS
        lvc_jobname         VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_sre_cleanup';
        lvc_sqltext         VARCHAR2(2000);
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(1000);
        ln_seqnum           NUMBER:=NULL;
        ln_retention_days   NUMBER := 0;
        le_exception        EXCEPTION;
        ln_del_cnt          NUMBER:=0;
        ldt_batchdate       DATE := STGMGR.pg_spin_lib.f_get_batch_date();
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started');
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        ln_retention_days:=STGMGR.pg_spin_lib.f_get_param('SRE_JOB_RETENTION',lvc_jobname,ln_seqnum);

        IF (ln_retention_days > 0) THEN
            lvc_stepname:='Delete from SRE_JOB_SUMMARY';
            lvc_sqltext :=' WHERE END_DATE < (TO_DATE('''||
                            TO_CHAR(ldt_batchdate-ln_retention_days,'MM/DD/YYYY')||
                            ''',''MM/DD/YYYY''))';
            --dbms_output.put_line(lvc_sqltext);
            p_cascade_delete('WWFMGR','SRE_JOB_SUMMARY',lvc_sqltext,16,1,ln_del_cnt,lvc_jobname,ln_seqnum);
        ELSE
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed',0,0,0,ln_del_cnt,0,ln_del_cnt);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,ln_del_cnt,0,ln_del_cnt);
        END IF;

        COMMIT;

--        EXCEPTION
--        WHEN le_exception THEN
--            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : Value of SRE_JOB_RETENTION Within SCPOMGR.UDT_PARAMETER should be greater than 0.  Aborting...');
--            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed: Value of SRE_JOB_RETENTION Within SCPOMGR.UDT_PARAMETER should be greater than 0.  Aborting...',1,0,0,0,0,0);
--            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
--                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
--            END IF;
--            RAISE_APPLICATION_ERROR( -20120, lvc_jobname||' : Value of SRE_JOB_RETENTION Within SCPOMGR.UDT_PARAMETER should be greater than 0.  Aborting...' );
--        WHEN OTHERS THEN
--            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
--            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
--            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
--                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
--            END IF;
--            ROLLBACK;
--            RAISE_APPLICATION_ERROR( -20120,lvc_jobname||' : '||SQLERRM(SQLCODE));
    END p_sre_cleanup;

    PROCEDURE p_fet_cleanup (pvc_jobname_in IN VARCHAR2 DEFAULT NULL,
                             pn_seqnum_in IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_fet_cleanup
    #
    # Description    : This procedure is used to remove all temporary FE tables (FETs).
    #
    # Input          : pvc_jobname_in, pn_seqnum_in.
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on      Modified by         Description
    #
    ###########################################################################################*/
    AS
        lvc_jobname         VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_fet_cleanup';
        lvc_sqltext         VARCHAR2(2000);
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(1000);
        ln_seqnum           NUMBER:=NULL;
        le_exception        EXCEPTION;
        ln_del_cnt          NUMBER:=0;
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started');
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        -- Find WWFMGR.FET tables
        FOR rs_tab IN (SELECT table_name
                         FROM dba_tables
                        WHERE owner='WWFMGR'
                          AND table_name LIKE 'FET\_%' ESCAPE '\')
        LOOP
            -- Call the p_exec_sql to drop the table
            lvc_stepname:='Dropping table WWFMGR.'||rs_tab.table_name;
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Starting');
            lvc_sqltext := 'DROP TABLE wwfmgr.'||rs_tab.table_name;
            wwfmgr.p_exec_sql(lvc_sqltext);
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Complete',0,0,0,0,0,0);
        END LOOP;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed',0,0,0,ln_del_cnt,0,ln_del_cnt);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,ln_del_cnt,0,ln_del_cnt);
        END IF;

        COMMIT;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : Value of SRE_JOB_RETENTION Within SCPOMGR.UDT_PARAMETER should be greater than 0.  Aborting...');
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed: Value of SRE_JOB_RETENTION Within SCPOMGR.UDT_PARAMETER should be greater than 0.  Aborting...',1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR( -20121, lvc_jobname||' : Value of SRE_JOB_RETENTION Within SCPOMGR.UDT_PARAMETER should be greater than 0.  Aborting...' );
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            ROLLBACK;
            RAISE_APPLICATION_ERROR( -20121,lvc_jobname||' : '||SQLERRM(SQLCODE));
    END p_fet_cleanup;

    PROCEDURE p_update_batchdate (pvc_jobname_in IN VARCHAR2 DEFAULT NULL,
                                  pn_seqnum_in   IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : P_UPDATE_BATCHDATE
    #
    # Description    : This Procedure is used to update the batch_date within UDT_PARAMETER table.
    #                  The updated batch_date will be used to update SKU.OHPOST and DFU.DMDPOSTDATE.
    #
    # Input          : NA
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    AS
        lvc_jobname       VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_update_batchdate';
        lvc_step          VARCHAR2(100);
        lvc_stepname      VARCHAR2(1000);
        ln_seqnum         NUMBER:=0;
        lvc_freezeparam   VARCHAR2(25) := 'BATCHDATE_FREEZE';
        lvc_freezeval     VARCHAR2(25);
        ld_batchdate_val  DATE;
        ld_batchdate_parm VARCHAR2(25) := 'BATCHDATE';
    BEGIN
        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started');
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        lvc_stepname:='Retrieve BATCHDATE from UDT_PARAMETER';
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
        BEGIN
            SELECT to_date(param_value, 'mm/dd/yyyy')
               INTO ld_batchdate_val
              FROM scpomgr.udt_parameter
            WHERE param_name = ld_batchdate_parm;
        EXCEPTION
           WHEN NO_DATA_FOUND THEN
               ld_batchdate_val := TRUNC(SYSDATE);
        END;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed, existing BATCHDATE='||TO_CHAR(ld_batchdate_val,'MM/DD/YYYY'),0,0,0,0,0,0);

        lvc_stepname:='Update BATCHDATE value within SCPOMGR.UDT_PARAMETER';
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
        BEGIN
            SELECT UPPER(param_value)
               INTO lvc_freezeval
              FROM scpomgr.udt_parameter
            WHERE param_name = lvc_freezeparam;
        EXCEPTION
           WHEN NO_DATA_FOUND THEN
               lvc_freezeval := 'FALSE';
        END;

        IF lvc_freezeval IN ('TRUE','T','1') THEN
            dbms_output.put_line('*** BATCHDATE NOT UPDATED - UDT_PARAMETER BATCH_FREEZE PARAMETER SET ***');
        ELSE
            EXECUTE IMMEDIATE 'MERGE INTO scpomgr.udt_parameter p ' ||
                                            ' USING (SELECT ''BATCHDATE'' param_name, TO_CHAR(TRUNC(SYSDATE+(6/24)), ''MM/DD/YYYY'') dt FROM DUAL) x ' ||
                                            '  ON (p.param_name = x.param_name) ' ||
                                            'WHEN MATCHED THEN UPDATE SET p.param_value = x.dt ' ||
                                            'WHEN NOT MATCHED THEN INSERT (param_name, param_value) VALUES (x.param_name, x.dt)';
            dbms_output.put_line('*** BATCHDATE UPDATED TO ' || f_get_batch_date() || ' ***');
        END IF;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,SQL%ROWCOUNT,0,0,SQL%ROWCOUNT);

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed',0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        COMMIT;

        EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            ROLLBACK;
            RAISE_APPLICATION_ERROR( -20130,lvc_jobname||' : '||SQLERRM(SQLCODE));
    END p_update_batchdate;


    PROCEDURE p_disable_batch_triggers (pvc_jobname_in IN VARCHAR2 DEFAULT NULL,
                                        pn_seqnum_in   IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_disable_batch_triggers
    #
    # Description    : This Procedure is used to disable all triggers in SCPOMGR and
    #                       enter them into the STGMGR.BATCH_TRIGGERS table.
    #
    # Input          : pvc_jobname_in, pn_seqnum_in.
    #                  The parameters pvc_jobname_in, pn_seqnum_in are used for logging purpose.
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        lvc_jobname     VARCHAR2(100):= 'STGMGR.pg_spin_lib.p_disable_batch_triggers';
        lvc_step        VARCHAR2(100);
        lvc_stepname    VARCHAR2(1000);
        ln_seqnum       NUMBER:=0;
        le_exception    EXCEPTION;
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started');
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        -- Clear out any data from prior run
        DELETE FROM STGMGR.batch_triggers;

        FOR REC IN (SELECT trigger_name FROM all_triggers WHERE owner='SCPOMGR')
        LOOP
            EXECUTE IMMEDIATE 'BEGIN scpomgr.p_exec_sql(''ALTER TRIGGER SCPOMGR.'|| REC.trigger_name|| ' DISABLE''); END;';
            INSERT INTO STGMGR.batch_triggers (trigger_name, batch_disable_flag) VALUES (rec.trigger_name, 'Y');
        END LOOP;

        COMMIT;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed',0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            ROLLBACK;
            RAISE_APPLICATION_ERROR( -20140,lvc_jobname||' : '||SQLERRM(SQLCODE));
    END;

    PROCEDURE p_disable_trigger (pvc_owner_name_in   IN VARCHAR2,
                                 pvc_trigger_name_in IN VARCHAR2,
                                 pvc_jobname_in      IN VARCHAR2 DEFAULT NULL,
                                 pn_seqnum_in        IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_disable_trigger
    #
    # Description    : This Procedure is used to disable a trigger.
    #
    # Input          : pvc_owner_name_in, pvc_trigger_name_in, pvc_jobname_in, pn_seqnum_in
    #                  Parameter pvc_owner_name_in is the trigger owner,
    #                   pvc_trigger_name_in is the trigger which needs to be disabled.
    #                  The parameters pvc_jobname_in, pn_seqnum_in are used for logging purpose.
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        lvc_jobname         VARCHAR2(100):= 'STGMGR.pg_spin_lib.p_disable_trigger';
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(1000);
        ln_seqnum           NUMBER:=0;
        ln_trig_count       NUMBER:=0;
        le_exception        EXCEPTION;
        le_ndf_exception    EXCEPTION;
    BEGIN
        IF (pvc_owner_name_in IS NULL OR pvc_trigger_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for trigger='||pvc_trigger_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        SELECT COUNT(1)
          INTO ln_trig_count
          FROM all_triggers
        WHERE owner=pvc_owner_name_in
          AND trigger_name=pvc_trigger_name_in;

        IF (ln_trig_count = 0) THEN
            RAISE le_ndf_exception;
        END IF;

        EXECUTE IMMEDIATE 'BEGIN '||pvc_owner_name_in||'.p_exec_sql(''ALTER TRIGGER '||pvc_owner_name_in||'.'|| pvc_trigger_name_in|| ' DISABLE''); END;';

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for trigger='||pvc_trigger_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE ('Trigger owner and name can''t be NULL.   Process Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20150,'Trigger owner and name can''t be NULL.  Process Aborting...');
            WHEN le_ndf_exception THEN
                DBMS_OUTPUT.PUT_LINE ('Trigger '||pvc_trigger_name_in||' doesn''t exists in '||pvc_owner_name_in||' schema');
                ROLLBACK;
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20150,'Trigger '||pvc_trigger_name_in||' doesn''t exist in '||pvc_owner_name_in||' schema');
            WHEN OTHERS THEN
                ROLLBACK;
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20150,SQLERRM(SQLCODE));
    END;

    PROCEDURE p_disable_trigger (pvc_trigger_name_in IN VARCHAR2,
                                 pvc_jobname_in      IN VARCHAR2 DEFAULT NULL,
                                 pn_seqnum_in        IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_disable_trigger
    #
    # Description    : This Procedure is used to disable a trigger.
    #
    # Input          : pvc_trigger_name_in, pvc_jobname_in, pn_seqnum_in
    #                  Parameter pvc_trigger_name_in is the trigger which needs to be disabled.
    #                  The parameters pvc_jobname_in, pn_seqnum_in are used for logging purpose.
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        lvc_jobname         VARCHAR2(100):= 'STGMGR.pg_spin_lib.p_disable_trigger';
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(1000);
        ln_seqnum           NUMBER:=0;
        ln_trig_count       NUMBER:=0;
        le_exception        EXCEPTION;
        le_ndf_exception    EXCEPTION;
    BEGIN
        IF (pvc_trigger_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        STGMGR.pg_spin_lib.p_disable_trigger('SCPOMGR',pvc_trigger_name_in,pvc_jobname_in,pn_seqnum_in);

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE ('Trigger name can''t be NULL.  Process Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20151,'Trigger name can''t be NULL.  Process Aborting...');
            WHEN OTHERS THEN
                ROLLBACK;
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20151,SQLERRM(SQLCODE));
    END;

    PROCEDURE p_enable_batch_triggers (pvc_jobname_in IN VARCHAR2 DEFAULT NULL,
                                       pn_seqnum_in   IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_enable_batch_triggers
    #
    # Description    : This Procedure is used to enable all triggers in SCPOMGR, and
    #                       clears out the STGMGR.BATCH_TRIGGERS table.
    #
    # Input          : pvc_jobname_in, pn_seqnum_in.
    #                  The parameters pvc_jobname_in, pn_seqnum_in are used for logging purpose.
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        lvc_jobname     VARCHAR2(100):='STGMGR.pg_spin_lib.p_enable_batch_triggers';
        lvc_step        VARCHAR2(100);
        lvc_stepname    VARCHAR2(1000);
        ln_seqnum       NUMBER:=0;
        le_exception    EXCEPTION;
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started');
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        FOR REC IN (SELECT trigger_name FROM all_triggers WHERE owner='SCPOMGR')
        LOOP
            EXECUTE IMMEDIATE 'BEGIN scpomgr.p_exec_sql(''ALTER TRIGGER scpomgr.'|| REC.trigger_name|| ' ENABLE''); END;';
            DELETE FROM STGMGR.batch_triggers WHERE trigger_name = rec.trigger_name;
        END LOOP;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed',0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            ROLLBACK;
            RAISE_APPLICATION_ERROR( -20160,lvc_jobname||' : '||SQLERRM(SQLCODE));

    END;

    PROCEDURE p_enable_trigger (pvc_owner_name_in   IN VARCHAR2,
                                pvc_trigger_name_in IN VARCHAR2,
                                pvc_jobname_in      IN VARCHAR2 DEFAULT NULL,
                                pn_seqnum_in        IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_enable_trigger
    #
    # Description    : This procedure is used to enable a trigger.  It will first check to make sure that
    #                       the trigger wasn't disabled by the p_disable_batch_triggers process (which means
    #                       that batch is running) before enabling the trigger.
    #
    # Input          : pvc_owner_name_in, pvc_trigger_name_in, pvc_jobname_in, pn_seqnum_in.
    #                  The parameters pvc_jobname_in, pn_seqnum_in are used for logging purpose.
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        lvc_jobname         VARCHAR2(100):= 'STGMGR.pg_spin_lib.p_enable_trigger';
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(1000);
        ln_seqnum           NUMBER:=0;
        ln_trig_count       NUMBER:=0;
        le_exception        EXCEPTION;
        le_data_exception   EXCEPTION;
        lvc_batch_trig_flag VARCHAR2(1):='N';
    BEGIN
        IF (pvc_trigger_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for trigger='||pvc_trigger_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        SELECT COUNT(1)
          INTO ln_trig_count
          FROM STGMGR.batch_triggers
         WHERE trigger_name = pvc_trigger_name_in;

        IF (ln_trig_count = 0) THEN
            EXECUTE IMMEDIATE 'BEGIN '||pvc_owner_name_in||'.p_exec_sql(''ALTER TRIGGER '||pvc_owner_name_in||'.'|| pvc_trigger_name_in|| ' ENABLE''); END;';
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for trigger='||pvc_trigger_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE ('pvc_owner_name_in and pvc_trigger_name_in cannot be NULL.  Process Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20170,'pvc_owner_name_in and pvc_trigger_name_in cannot be NULL.');
            WHEN OTHERS THEN
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20170,SQLERRM(SQLCODE));
    END;

    PROCEDURE p_enable_trigger (pvc_trigger_name_in IN VARCHAR2,
                                pvc_jobname_in      IN VARCHAR2 DEFAULT NULL,
                                pn_seqnum_in        IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_enable_trigger
    #
    # Description    : This procedure is used to enable a trigger.  It will first check to make sure that
    #                       the trigger wasn't disabled by the p_disable_batch_triggers process (which means
    #                       that batch is running) before enabling the trigger.
    #
    # Input          : pvc_trigger_name_in, pvc_jobname_in, pn_seqnum_in.
    #                  The parameters pvc_jobname_in, pn_seqnum_in are used for logging purpose.
    #
    # Output         : NA
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        lvc_jobname         VARCHAR2(100):= 'STGMGR.pg_spin_lib.p_enable_trigger';
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(1000);
        ln_seqnum           NUMBER:=0;
        ln_trig_count       NUMBER:=0;
        le_exception        EXCEPTION;
        le_data_exception   EXCEPTION;
        lvc_batch_trig_flag VARCHAR2(1):='N';
    BEGIN
        IF (pvc_trigger_name_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        STGMGR.pg_spin_lib.p_enable_trigger('SCPOMGR',pvc_trigger_name_in,pvc_jobname_in,pn_seqnum_in);

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE ('pvc_trigger_name_in cannot be NULL.  Process Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20170,'pvc_trigger_name_in cannot be NULL.');
            WHEN OTHERS THEN
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_jobname,'Failed, error='||SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR (-20170,SQLERRM(SQLCODE));
    END;

--    PROCEDURE p_get_igp_stats (p_JobID   IN  igpmgr.intjobs.jobid%TYPE := ' ',
--                               p_IGPtab  IN  igpmgr.intjobs.int_tablename%TYPE := ' ',
--                               p_Table   OUT igpmgr.intjobs.tablename%TYPE,
--                               p_RowCnt  OUT igpmgr.intjobs.totalrowsct%TYPE,
--                               p_InsCnt  OUT igpmgr.intjobs.insertct%TYPE,
--                               p_UpdCnt  OUT igpmgr.intjobs.updatect%TYPE)
--    /*##########################################################################################
--    # Procedure Name : p_get_igp_stats
--    #
--    # Description    : Retrieve IGP statistics for the specified job and table.
--    #
--    # Input          : p_JobID -  the name of IGP job for which statistics
--    #                             are to be retrieved
--    #                  p_IGPtab - the name of the IGP table for which
--    #                             statistics are to  be retrieved
--    #
--    # Output         : p_Table - name of the Manu table updated
--    #                  p_RowCnt - total number of rows processed
--    #                  p_InsCnt - total number of rows inserted
--    #                  p_UpdCnt - total number of rows updated
--    #
--    # Author         : Andy Fritz
--    # Date created   : 10/14/2019
--    # Restartable    :
--    #
--    # Modified on     Modified by         Description
--    #
--    ##########################################################################################*/
--    IS
--    BEGIN
--        -- Get IGP statistics from INTJOBS table.
--        SELECT tablename, SUM(totalrowsct), SUM(insertct), SUM(updatect)
--          INTO p_Table,p_RowCnt,p_InsCnt,p_UpdCnt
--          FROM igpmgr.intjobs
--         WHERE jobid LIKE p_JobID || '%'
--           AND int_tablename = p_IGPtab
--      GROUP BY tablename;
--    EXCEPTION
--        WHEN NO_DATA_FOUND THEN
--            p_Table  := ' ';
--            p_RowCnt := 0;
--            p_InsCnt := 0;
--            p_UpdCnt := 0;
--    END p_get_igp_stats;
--
--    PROCEDURE p_log_igp_stats (p_JobID        IN  igpmgr.intjobs.jobid%TYPE := ' ',
--                               p_IGPtab       IN  igpmgr.intjobs.int_tablename%TYPE := ' ',
--                               pvc_jobname_in IN  VARCHAR2 DEFAULT NULL,
--                               pn_seqnum_in   IN NUMBER DEFAULT NULL)
--    /*##########################################################################################
--    # Procedure Name : p_log_igp_stats
--    #
--    # Description    : Retrieve IGP statistics for the specified job and table
--    #                  and write them to the UDT_SCRIPT_LOG table.
--    #
--    # Input          : p_JobID - IGP job name
--    #                  p_IGPtab - IGP table to log the statistics for
--    #                  pvc_jobname_in - calling script
--    #                  pn_seqnum_in - log sequence number
--    #
--    # Output         : writes record to UDT_SCRIPT_LOG with table stats
--    #
--    # Author         : Andy Fritz
--    # Date created   : 10/14/2019
--    # Restartable    :
--    #
--    # Modified on     Modified by         Description
--    #
--    ##########################################################################################*/
--    IS
--        lvc_jobname   VARCHAR2(100) := 'STGMGR.pg_spin_lib.f_log_igp_stats';
--        ln_seqnum     NUMBER;
--        lvc_step      VARCHAR2(100);
--        lvc_stepname  VARCHAR2(300);
--        lvc_Table     igpmgr.intjobs.tablename%TYPE;
--        ln_Total      igpmgr.intjobs.totalrowsct%TYPE;
--        ln_Added      igpmgr.intjobs.insertct%TYPE;
--        ln_Updated    igpmgr.intjobs.updatect%TYPE;
--        lvc_msg       VARCHAR2(200);
--    BEGIN
--        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
--            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||p_IGPTab);
--            ln_seqnum := pn_seqnum_in;
--        ELSE
--            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
--        END IF;
--
--        -- Get IGP statistics from INTJOBS table.
--        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),'Get IGP Stats for ' || p_IGPTab,'Started');
--        STGMGR.pg_spin_lib.p_get_igp_stats(
--          p_JobID,p_IGPtab,lvc_Table,ln_Total,ln_Added,ln_Updated);
--        lvc_msg := lvc_Table||' - SUBMITTED: '||ln_Total||
--                   ' INSERTED: '||ln_Added||
--                   ' UPDATED: '||ln_Updated||
--                   ' REJECTED: '||(ln_Total-(ln_Added+ln_Updated));
--        --dbms_output.put_line(lvc_msg);
--        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,lvc_msg,0,ln_Added,ln_Updated,0,ln_Total-(ln_Added+ln_Updated),ln_Total);
--
--        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
--            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||p_IGPTab,0,0,0,0,0,0);
--        ELSE
--            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
--        END IF;
--
--    END p_log_igp_stats;


    FUNCTION f_is_not_unassigned (pvc_string_in IN VARCHAR2) RETURN BOOLEAN
    /*##########################################################################################
    # Procedure Name : f_is_not_unassigned
    #
    # Description    : To verify that a specified string is NOT NULL
    #
    # Input          : pvc_string_in - string to check for NULL
    #
    # Output         : BOOLEAN - TRUE or FALSE
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
    BEGIN
        IF (NVL(UPPER(TRIM(pvc_string_in)),'UNASSIGNED') = 'UNASSIGNED') THEN
            RETURN FALSE;
        ELSE
            RETURN TRUE;
        END IF;

    END f_is_not_unassigned;

    FUNCTION f_is_numeric (pvc_string_in IN VARCHAR2) RETURN NUMBER
    /*##########################################################################################
    # Procedure Name : f_is_not_unassigned
    #
    # Description    : To verify that a specified string is numeric
    #
    # Input          : pvc_string_in - string to check for numeric value
    #
    # Output         : NUMBER - 1 (True) or 0 (False)
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        ln_number    NUMBER;
    BEGIN
        ln_number := pvc_string_in;
        RETURN 1;
    EXCEPTION
        WHEN OTHERS THEN
        RETURN 0;
    END f_is_numeric;


    FUNCTION f_is_unassigned (pvc_string_in IN VARCHAR2) RETURN BOOLEAN
    /*##########################################################################################
    # Procedure Name : f_is_unassigned
    #
    # Description    : To verify that a specified string is NULL
    #
    # Input          : pvc_string_in - string to check for NULL
    #
    # Output         : BOOLEAN - TRUE or FALSE
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
    BEGIN
        IF (NVL(UPPER(TRIM(pvc_string_in)),'UNASSIGNED') = 'UNASSIGNED') THEN
            RETURN TRUE;
        ELSE
            RETURN FALSE;
        END IF;

    END f_is_unassigned;


    FUNCTION f_is_valid_date_format (pvc_date_in IN VARCHAR2,
                                     pvc_format_in IN VARCHAR2) RETURN NUMBER PARALLEL_ENABLE
    /*##########################################################################################
    # Procedure Name : f_is_valid_date_format
    #
    # Description    : To verify that a specified string is a valid date
    #
    # Input          : pvc_date_in - string to check for date value
    #                  pvc_format_in - format of date string
    #
    # Output         : NUMBER - 1 or 0
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        ldt_date    DATE;
    BEGIN
        ldt_date := TO_DATE(pvc_date_in, pvc_format_in);
        RETURN 1;
    EXCEPTION
        WHEN OTHERS
        THEN
            RETURN 0;
    END f_is_valid_date_format;


    FUNCTION f_split (pvc_string_in IN VARCHAR2,
                      pvc_delim_in  IN VARCHAR2 := ',') RETURN typ_split_nt
    /*##########################################################################################
    # Procedure Name : f_split
    #
    # Description    : To split a delimited string into a nested table.
    #
    # Input          : pvc_string_in - string to parse
    #                  pvc_delim_in - delimite
    #
    # Output         : typ_split_nt
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        li_idx         INTEGER         := 1;
        ltbl_split_nt  typ_split_nt    := typ_split_nt(NULL);
        lvc_string     VARCHAR2(4000)  := pvc_string_in||pvc_delim_in;
    BEGIN
        WHILE (INSTR(lvc_string,pvc_delim_in,1,li_idx) > 0) LOOP
            IF (li_idx = 1) THEN
                ltbl_split_nt(li_idx) := SUBSTR(lvc_string,1,(INSTR(lvc_string,pvc_delim_in,1,li_idx)-1));
            ELSE
                ltbl_split_nt.EXTEND;
                ltbl_split_nt(li_idx) := SUBSTR(lvc_string,(INSTR(lvc_string,pvc_delim_in,1,(li_idx-1))+1),
                  (INSTR(lvc_string,pvc_delim_in,1,li_idx) - (INSTR(lvc_string,pvc_delim_in,1,(li_idx-1))+1)));
            END IF;
                li_idx := li_idx + 1;
        END LOOP;

        RETURN ltbl_split_nt;

    END f_split;


    FUNCTION f_get_part (pvc_string_in IN VARCHAR2,
                         pvc_delim_in  IN VARCHAR2 := ',',
                         pn_part_no_in IN PLS_INTEGER) RETURN VARCHAR2
    /*##########################################################################################
    # Procedure Name : f_get_part
    #
    # Description    : To get a part of a delimited string.
    #
    # Input          : pvc_string_in - string to parse
    #                  pvc_delim_in - delimiter
    #                  pn_part_no_in - part of string to get
    #
    # Output         : varchar2 - part of input string
    #
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    :
    #
    # Modified on     Modified by         Description
    #
    ##########################################################################################*/
    IS
        lvc_part VARCHAR2(4000);
    BEGIN

        BEGIN
          SELECT val
            INTO lvc_part
            FROM (SELECT COLUMN_VALUE val, ROWNUM rn
                    FROM TABLE(STGMGR.pg_spin_lib.f_split(pvc_string_in,pvc_delim_in)))
           WHERE rn = pn_part_no_in;
        EXCEPTION WHEN OTHERS THEN
            lvc_part := '';
        END;
        RETURN lvc_part;

    END f_get_part;


    PROCEDURE p_save_stats (pvc_tablename_in         IN VARCHAR2,
                            pvc_owner_in             IN VARCHAR2 DEFAULT 'SCPOMGR',
                            pvc_jobname_in           IN VARCHAR2 DEFAULT NULL,
                            pn_seqnum_in             IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_save_stats
    #
    # Description : This Procedure is used to save optimizer stats for the table passed in into
    #               STGMGR.STG_STATS table
    #
    # Input       : pvc_tablename_in - Name of table to process
    #               pvc_owner_in - Owner of table to process
    #               pvc_jobname_in - Job name to log to
    #               pn_seqnum_in - Sequence number of process to log to
    #
    # Output      : NA
    #
    # Author      : Ratha J
    # Date created: 10/14/2019
    # Restartable : Yes
    #
    # Modified on      Modified by         Description
    #
    ###########################################################################################*/
    AS
        lvc_jobname     VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_save_stats';
        lvc_tablename   VARCHAR2(30):= NULL;
        lvc_owner       VARCHAR2(30);
        lvc_step        VARCHAR2(300);
        lvc_stepname    VARCHAR2(1000) := 'Start';
        ln_seqnum       NUMBER;
        le_exception    EXCEPTION;
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||pvc_tablename_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        IF (pvc_owner_in IS NULL) THEN
           lvc_owner := 'SCPOMGR';
        ELSE
           lvc_owner := pvc_owner_in;
        END IF;

        IF (pvc_tablename_in IS NULL) THEN
            RAISE le_exception;
        ELSE
            lvc_tablename := pvc_tablename_in;
            lvc_stepname := 'Save stats for ' || lvc_tablename || ' in stg_stats';
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

            DBMS_STATS.export_table_stats (ownname=> lvc_owner,
                               tabname=> lvc_tablename,
                               stattab=> 'STG_STATS',
                               statown=> 'STGMGR');

            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
        END IF;

        COMMIT;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||pvc_tablename_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : First parameter must be populated with tablename.  Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed: First parameter must be populated with tablename.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20200, lvc_jobname||' : First parameter must be populated with tablename.  Aborting...' );
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||':'||lvc_stepname||':'||SQLERRM(SQLCODE));
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                ROLLBACK;
                RAISE_APPLICATION_ERROR( -20200,lvc_jobname||':'||lvc_stepname||':'||SQLERRM(SQLCODE));
    END p_save_stats;


    PROCEDURE p_load_stats (pvc_tablename_in         IN VARCHAR2,
                            pvc_owner_in             IN VARCHAR2 DEFAULT 'SCPOMGR',
                            pvc_jobname_in           IN VARCHAR2 DEFAULT NULL,
                            pn_seqnum_in             IN NUMBER DEFAULT NULL)
    /*##########################################################################################
    # Procedure Name : p_load_stats
    #
    # Description : This Procedure is used to load optimizer stats for the table passed in by
    #               retrieving the saved stats from STGMGR.STG_STATS table
    #
    # Input       : pvc_tablename_in - Name of table to process
    #               pvc_owner_in - Owner of table to process
    #               pvc_jobname_in - Job name to log to
    #               pn_seqnum_in - Sequence number of process to log to
    #
    # Output      : NA
    #
    # Author      : Ratha J
    # Date created: 10/14/2019
    # Restartable : Yes
    #
    # Modified on      Modified by         Description
    #
    ###########################################################################################*/
    AS
        lvc_jobname     VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_load_stats';
        lvc_tablename   VARCHAR2(30):= NULL;
        lvc_owner       VARCHAR2(30);
        lvc_step        VARCHAR2(300);
        lvc_stepname    VARCHAR2(1000) := 'Start';
        ln_seqnum       NUMBER;
        le_exception    EXCEPTION;
    BEGIN

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||pvc_tablename_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        IF (pvc_owner_in IS NULL) THEN
           lvc_owner := 'SCPOMGR';
        ELSE
           lvc_owner := pvc_owner_in;
        END IF;

        IF (pvc_tablename_in IS NULL) THEN
            RAISE le_exception;
        ELSE
            lvc_tablename := pvc_tablename_in;
            lvc_stepname := 'Load stats from stg_stats for ' || lvc_tablename;
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');

            DBMS_STATS.import_table_stats (ownname  => lvc_owner,
                               tabname  => lvc_tablename,
                               stattab  => 'STG_STATS',
                                           CASCADE  => TRUE,
                               statown  => 'STGMGR',
                               force    => TRUE);

            STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);
        END IF;

        COMMIT;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||pvc_tablename_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : First parameter must be populated with tablename.  Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed: First parameter must be populated with tablename.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20210, lvc_jobname||' : First parameter must be populated with tablename.  Aborting...' );
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||':'||lvc_stepname||':'||SQLERRM(SQLCODE));
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed with error ' || SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                ROLLBACK;
                RAISE_APPLICATION_ERROR( -20210,lvc_jobname||':'||lvc_stepname||':'||SQLERRM(SQLCODE));
    END p_load_stats;

    FUNCTION NVL2(origval    VARCHAR2,
                  notnullval VARCHAR2,
                  nullval    VARCHAR2) RETURN VARCHAR2
    /*#############################################################################################
    # Function Name  : NVL2
    # Description    : This function will return second value if first value is not NULL,
    #                  otherwise it returns the second value
    #
    # Input          : origval - input
    #                  notnullval - value returned if origval is not NULL
    #                  nullval - value returned if origval is NULL
    #
    # Output         : nullval or notnullval
    # Author         : Andy Fritz
    # Date created   : 10/14/2019
    # Restartable    : Yes
    #
    # Modified on     Modified by         Description
    #
    #############################################################################################*/
    AS
    BEGIN
        IF TRIM(origval) IS NULL THEN
            RETURN nullval;
        ELSE
            RETURN notnullval;
        END IF;
    END NVL2;

    PROCEDURE p_truncate_replace (pvc_owner_in            IN VARCHAR2,
                                  pvc_table_name_in        IN VARCHAR2,
                                  pvc_where_clause_in     IN VARCHAR2,
                                  pn_delete_count_out    OUT NUMBER,
                                  pvc_jobname_in        IN VARCHAR2 DEFAULT NULL,
                                  pn_seqnum_in          IN NUMBER DEFAULT NULL)
    /*#############################################################################################
    #  Procedure Name : p_truncate_replace
    #
    #  Description    : Performs a conditional delete on a table by taking in a where clause
    #            that specifies which records are to be left in the table after the conditional
    #            delete is completed.  It copies the records to be retained after delete into
    #            a holding table, truncates the main table passed in as parameter and then
    #             copies the records back from the holding table.  Intended to be used on large
    #             volume tables with frequent deletions.  The caller needs to ensure the table
    #            passed in is able to be truncated - i.e. any child constraints that need to be
    #            disabled should already be disabled before this procedure is called.
    #
    #  Input          : pvc_owner_in, pvc_table_name_in, pvc_where_clause_in,
    #                   pvc_jobname_in, pn_seqnum_in.
    #  Output         : pn_delete_count_out
    #
    #  Author         : Spinnaker
    #  Date created   : 02/09/2021
    #  Restartable    : Yes
    #
    #  Modified on      Modified by         Description
    #
    #############################################################################################*/
    IS
        lvc_jobname      VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_truncate_replace';
        lvc_step         VARCHAR2(4000);
        lvc_stepname     VARCHAR2(4000);
        ln_seqnum        NUMBER;
        lvc_sql            VARCHAR2(4000);
        lvc_plsql        VARCHAR2(4000);
        ln_total_cnt    NUMBER;
        ln_retain_cnt    NUMBER;
        ln_delete_cnt    NUMBER;
        le_exception     EXCEPTION;
    BEGIN

        IF (pvc_owner_in IS NULL OR pvc_table_name_in IS NULL OR pvc_where_clause_in IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started for table='||pvc_table_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        -- Create the hold table to retain the data that needs to be copied over after the truncate of main table
        lvc_sql := 'CREATE TABLE STGMGR.HOLD_' || pvc_table_name_in ||
                    ' AS SELECT /*+ parallel */ * FROM ' || pvc_owner_in || '.' || pvc_table_name_in  ||
                    ' WHERE ' || pvc_where_clause_in;
        lvc_stepname := 'Create hold table with data to be retained';
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
        EXECUTE IMMEDIATE lvc_sql;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);

        -- Get the number of records that will be removed by this procedure from the main table
        -- This will be written to the out parameter
        lvc_sql := 'SELECT (SELECT /*+ parallel */ count(1) FROM ' || pvc_owner_in || '.' || pvc_table_name_in  ||
                    ') - (SELECT /*+ parallel */ count(1) FROM STGMGR.HOLD_' || pvc_table_name_in || ') FROM DUAL';
        lvc_stepname := 'Get records removed count';
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
        EXECUTE IMMEDIATE lvc_sql INTO ln_delete_cnt;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);

        -- Truncate the main table
        lvc_plsql := 'BEGIN ' || pvc_owner_in || '.p_exec_sql(''TRUNCATE TABLE ' ||
                      pvc_owner_in || '.' || pvc_table_name_in || '''); END;';
        lvc_stepname := 'Truncate table ' || pvc_owner_in || '.' || pvc_table_name_in;
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
        EXECUTE IMMEDIATE lvc_plsql;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);

        -- Insert data back from the hold table into the main table
        lvc_sql := 'INSERT INTO ' || pvc_owner_in || '.' || pvc_table_name_in  ||
                    ' SELECT /*+ parallel */ * FROM STGMGR.HOLD_' || pvc_table_name_in;
        lvc_stepname := 'Insert data to be retained from hold table';
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
        EXECUTE IMMEDIATE lvc_sql;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);

        -- Drop the hold table
        lvc_sql := 'DROP TABLE STGMGR.HOLD_' || pvc_table_name_in;
        lvc_stepname := 'Drop hold table';
        lvc_step := STGMGR.pg_spin_lib.f_log_step_start(ln_seqnum,NVL(pvc_jobname_in,lvc_jobname),lvc_stepname,'Started');
        EXECUTE IMMEDIATE lvc_sql;
        STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Completed',0,0,0,0,0,0);

        -- Populate the deleted count in the output variable
        pn_delete_count_out := ln_delete_cnt;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed for table='||pvc_table_name_in,0,0,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        COMMIT;

        EXCEPTION
            WHEN le_exception THEN
                DBMS_OUTPUT.PUT_LINE ( lvc_jobname||' : First 3 parameters need to be Owner, Table and Where clause.  Aborting...');
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,'Failed : First 3 parameters need to be Owner, Table and Where clause.  Aborting...',1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                RAISE_APPLICATION_ERROR( -20070, lvc_jobname||' : First 3 parameters need to be Owner, Table and Where clause.  Aborting...' );
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE (lvc_jobname||' : '||SQLERRM(SQLCODE));
                STGMGR.pg_spin_lib.p_log_step_end(ln_seqnum,lvc_step,SQLERRM(SQLCODE),1,0,0,0,0,0);
                IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                    STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
                END IF;
                ROLLBACK;
                RAISE_APPLICATION_ERROR( -20070, lvc_jobname||' : '||SQLERRM(SQLCODE));
    END p_truncate_replace;

    FUNCTION f_stg_pct_complete (p_int_owner  IN VARCHAR2,
                                        p_int_tbl    IN VARCHAR2,
                                        p_live_owner IN VARCHAR2,
                                        p_live_tbl   IN VARCHAR2,
                                        pn_check_pct IN NUMBER DEFAULT NULL,
                                        p_stg_where  IN VARCHAR2 DEFAULT NULL,
                                        p_live_where IN VARCHAR2 DEFAULT NULL)                                  
    RETURN NUMBER
    /*##############################################################################
     #   Script Name   : STGMGR.F_STG_PCT_COMPLETE.sql
     #
     #   Description   : Check if # of record from full reload interface
     #                   file is within tolerance or total records in live table.
     #                   An error is returned if # of records loaded from interface file
     #                   is xx percent less than total number of records in live table
     #
     #   Input          : p_int_owner     - Schema of interfac table
     #                    p_int_tbl       - Name of interface table
     #                    p_live_owner    - Schema of live table
     #                    p_live_tbl      - Name of interface table
     #                    pn_check_pct    - Percentage for comparison
     #                    p_where_stmt    - where clause to be used when selecting count from live table
     #
     #   Output          : 0 - Record count in interface table within tolerance of live
     #                     1 - Record count in interface table outside tolerance of live
     #   Author        : SpinnakerSCA
     #   Date created  : 8/19/21
     #   Restartable   : Yes
     #
     #   Modified on *    Modified by *              Description *
     #
     ################################################################################*/
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        ln_stg_tolerance   NUMBER;
        ln_stg_cnt         NUMBER;
        ln_live_cnt        NUMBER;
        lvc_sql_cmd        VARCHAR2(1000);
        lvc_tab_nm         VARCHAR2(100);
        ln_tol_rtn         NUMBER := 0;
        ln_pct_diff        NUMBER := 0;
        lcn_dflt_pct       NUMBER := 20;

    BEGIN

      IF pn_check_pct IS NOT NULL THEN
        -- Use percent that was passed in
        ln_stg_tolerance := pn_check_pct;
      ELSE
        -- Get tolerance allowed between Interface record count and Live record count
        BEGIN
          ln_stg_tolerance := stgmgr.pg_spin_lib.f_get_param ('STG_TOLERENCE');
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
          ln_stg_tolerance := lcn_dflt_pct;
        END;
      END IF;

      lvc_sql_cmd := 'SELECT count(*) from ';

      lvc_tab_nm := p_int_owner||'.'||p_int_tbl;
      EXECUTE IMMEDIATE lvc_sql_cmd||lvc_tab_nm||' '||p_stg_where INTO ln_stg_cnt;

      lvc_tab_nm := p_live_owner||'.'||p_live_tbl;
      EXECUTE IMMEDIATE lvc_sql_cmd||lvc_tab_nm||' '||p_live_where INTO ln_live_cnt;

      IF ln_stg_cnt < ln_live_cnt THEN
        --ln_pct_diff :=  ((ln_live_cnt - ln_stg_cnt) / 100) * 100;
        ln_pct_diff := (1-(ln_stg_cnt/ln_live_cnt))*100;
        IF ln_pct_diff > ln_stg_tolerance THEN
          ln_tol_rtn := 1;
        END IF;
      END IF;

    RETURN ln_tol_rtn;

    END;

    PROCEDURE p_set_param (pvc_param_value_in IN VARCHAR2,
                           pvc_param_name_in IN VARCHAR2,
                           pvc_jobname_in    IN VARCHAR2 DEFAULT NULL,
                           pn_seqnum_in      IN NUMBER DEFAULT NULL)
    /*#############################################################################################
    # Procedure Name : p_set_param
    # Description    : Updates or inserts parameter values into UDT_PARAMETER table.
    #
    # Input          : pvc_param_value_in - parameter value ie. 10/31/2022
    #                  pvc_param_name_in - parameter name ie. BATCHDATE
    #
    # Output         : NA
    # Author         : Andy Fritz
    # Date created   : 12/24/2021
    # Restartable    : Yes
    #
    # Modified on       Modified by         Description
    #
    #############################################################################################*/
    IS
        lvc_jobname         VARCHAR2(100) := 'STGMGR.pg_spin_lib.p_set_param';
        ln_seqnum           NUMBER;
        lvc_step            VARCHAR2(100);
        lvc_stepname        VARCHAR2(300);
        lvc_return_value    VARCHAR2(100);
        ln_record_count     NUMBER;
        le_exception        EXCEPTION;
    BEGIN
        IF (TRIM(pvc_param_value_in) IS NULL OR
            TRIM(pvc_param_name_in) IS NULL) THEN
            RAISE le_exception;
        END IF;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            lvc_step := STGMGR.pg_spin_lib.f_log_step_start (pn_seqnum_in,pvc_jobname_in,lvc_jobname,'Started getting param_value for param_name='||pvc_param_name_in);
            ln_seqnum := pn_seqnum_in;
        ELSE
            ln_seqnum := STGMGR.pg_spin_lib.f_log_script_start(lvc_jobname);
        END IF;

        UPDATE scpomgr.udt_parameter
           SET param_value = pvc_param_value_in
         WHERE param_name = pvc_param_name_in;
        ln_record_count := SQL%ROWCOUNT;
        COMMIT;

        IF (pn_seqnum_in IS NOT NULL AND pvc_jobname_in IS NOT NULL) THEN
            STGMGR.pg_spin_lib.p_log_step_end (ln_seqnum,lvc_jobname,'Completed setting param_value for param_name='||pvc_param_name_in,0,ln_record_count,0,0,0,0);
        ELSE
            STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,0,0,0,0,0,0);
        END IF;

        EXCEPTION
        WHEN le_exception THEN
            DBMS_OUTPUT.PUT_LINE ('p_set_param requires a parameter value and name. Aborting....');
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'p_set_param requires a parameter value and name.  Aborting..',1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR( -20005, 'p_set_param requires a parameter value and name.  Aborting...');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE (lvc_step||' : '|| SQLERRM(SQLCODE));
            STGMGR.pg_spin_lib.p_log_step_end (pn_seqnum_in,lvc_step,'p_set_param failed with '||SQLERRM(SQLCODE),1,0,0,0,0,0);
            IF (pn_seqnum_in IS NULL OR pvc_jobname_in IS NULL) THEN
                STGMGR.pg_spin_lib.p_log_script_end(ln_seqnum,1,0,0,0,0,0);
            END IF;
            RAISE_APPLICATION_ERROR( -20005, lvc_step||' : '|| SQLERRM(SQLCODE));
    END p_set_param;


END PG_SPIN_LIB;
/


GRANT EXECUTE, DEBUG ON STGMGR.PG_SPIN_LIB TO ABPPMGR;

GRANT EXECUTE, DEBUG ON STGMGR.PG_SPIN_LIB TO JDA_SYSTEM;

GRANT EXECUTE, DEBUG ON STGMGR.PG_SPIN_LIB TO SCPOMGR;

GRANT EXECUTE, DEBUG ON STGMGR.PG_SPIN_LIB TO WWFMGR;
