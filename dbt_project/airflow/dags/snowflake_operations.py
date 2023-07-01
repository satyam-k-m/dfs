""""Contains DDL queres for Audit tables"""

create_dim_pipeline = """
    CREATE TRANSIENT TABLE IF NOT EXISTS dfs_audit.audit_control.{{ params.table_name }}
        (
            pipeline_id VARCHAR(25),
            pipeline_name VARCHAR(25),
            active_status_ind VARCHAR(25)
        );
"""

create_dim_task = """
    CREATE TRANSIENT TABLE IF NOT EXISTS dfs_audit.audit_control.{{ params.table_name }}
        (
            task_id VARCHAR(100),
            pipeline_id VARCHAR(25),
            task_name VARCHAR(100),
            landing_directory VARCHAR(100),
            file_naming_pattern VARCHAR(25),
            active_status_ind VARCHAR(25)
            
            
        );
"""

create_fct_pipeline = """
    CREATE TRANSIENT TABLE IF NOT EXISTS dfs_audit.audit_control.{{ params.table_name }}
        (
            pipeline_id VARCHAR(25),
            run_id VARCHAR(100),
            run_ts TIMESTAMP,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            duration INT,
            status_cd VARCHAR(20),
            error_message VARCHAR(100),
            error_cd VARCHAR(25),
            created_ts TIMESTAMP,
            created_by VARCHAR(50),
            modified_ts TIMESTAMP,
            modified_by VARCHAR(50)

        );
"""

create_fct_task = """
    CREATE TRANSIENT TABLE IF NOT EXISTS  dfs_audit.audit_control.{{ params.table_name }}
        (
            pipeline_id VARCHAR(25),
            run_id VARCHAR(100),
            task_id VARCHAR(100),
            run_ts TIMESTAMP,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            duration INT,
            status_cd VARCHAR(20),
            error_message VARCHAR(100),
            error_cd VARCHAR(25)

        );
"""

insert_dim_task = """

    INSERT INTO  dfs_audit.audit_control.{{ params.table_name }}
        (pipeline_id,
            task_id,
            task_name,
            landing_directory,
            file_naming_pattern,
            active_status_ind )
        VALUES ( '{{params.pipeline_id}}', '{{params.task_id}}', '{{params.task_name}}', 'input/', '*csv', 'Y'
        );
"""

insert_dim_pipeline = """

    INSERT INTO  dfs_audit.audit_control.{{ params.table_name }}
        (pipeline_id,
            pipeline_name,
            active_status_ind)
        VALUES ( '{{params.pipeline_id}}',  '{{params.pipeline_name}}', 'Y'
        );
"""


read_dim_table = """
        SELECT * FROM dfs_audit.audit_control.{{params.table_name}}
"""

insert_pipeline_status = """
        INSERT INTO dfs_audit.audit_control.{{params.table_name}}
        (
         pipeline_id,
            run_id,
            run_ts,
            end_ts,
            duration,
            status_cd,
            error_message,
            error_cd,
            created_ts,
            created_by,
            modified_ts,
            modified_by

        )
        VALUES (
        '{{params.pipeline_id}}', '{{params.run_id}}','{{params.run_ts}}', '{{params.end_ts}}',
        '{{params.duration}}', '{{params.status_cd}}', '{{params.error_message}}', '{{params.error_cd}}',
        '{{params.created_ts}}', '{{params.created_by}}', '{{params.modified_ts}}', '{{params.modified_by}}'
        )
"""

insert_task_status = """
    INSERT INTO dfs_audit.audit_control.{{params.fact_table_name}}
        (pipeline_id, task_id, run_id, run_ts,  start_ts, end_ts, duration, status_cd, error_message, error_cd)
        select pipeline_id, task_id,null as run_id, '{{params.run_ts}}' as run_ts, null as start_ts,null as end_ts,null as duration, 'Pending' as status, 
        null as error_message,null as error_cd
        from dfs_audit.audit_control.{{params.dim_table_name}}
        where pipeline_id='{{params.pipeline_id}}'
        
"""


insert_pipeline_status = """
    INSERT INTO dfs_audit.audit_control.{{params.fact_table_name}}
        select pipeline_id,null as run_id, '{{params.run_ts}}' as run_ts, null as start_ts,null as end_ts,null as duration, 'Pending' as status, null as error_message,null as error_cd,
        null as created_ts, null as created_by, null as modified_ts, null as modified_by
        from dfs_audit.audit_control.{{params.dim_table_name}}
        where pipeline_id='{{params.pipeline_id}}'
        
"""

update_task_status_pre = """
        UPDATE dfs_audit.audit_control.{{params.task_table_name}} tsk
        FROM dfs_audit.audit_control.{{params.pipeline_table_name}} pipeline
        SET 
            tsk.run_id = pipeline.run_id,
            tsk.status_cd = '{{params.status_cd}}',
            tsk.start_ts = CURRENT_TIMESTAMP

        WHERE 
            tsk.pipeline_id =  '{{params.pipeline_id}}' AND 
            tsk.task_id = '{{params.task_id}}' AND
            tsk.run_ts = '{{params.run_ts}}'

"""

update_task_status_post = """
        UPDATE dfs_audit.audit_control.{{params.task_table_name}}
        SET 
            status_cd = '{{params.status_cd}}',
            end_ts = CURRENT_TIMESTAMP

        WHERE 
            pipeline_id =  '{{params.pipeline_id}}' AND 
            task_id = '{{params.task_id}}' AND
            run_ts = '{{params.run_ts}}';
        
        UPDATE dfs_audit.audit_control.{{params.task_table_name}}
        SET 
            duration = TIMESTAMPDIFF(SECONDS, start_ts, end_ts)

        WHERE 
            pipeline_id =  '{{params.pipeline_id}}' AND 
            task_id = '{{params.task_id}}' AND
            run_ts = '{{params.run_ts}}'

"""

update_pipeline_status_pre = """
        UPDATE dfs_audit.audit_control.{{params.table_name}}
        SET 
            run_id = DFS_AUDIT.AUDIT_CONTROL.run_id_seq.nextval,
            status_cd = '{{params.status_cd}}',
            start_ts = CURRENT_TIMESTAMP
        WHERE 
            pipeline_id =  '{{params.pipeline_id}}' AND
            run_ts = '{{params.run_ts}}'

"""

update_pipeline_status_post = """
        UPDATE dfs_audit.audit_control.{{params.table_name}}
        SET status_cd = '{{params.status_cd}}',
            end_ts = CURRENT_TIMESTAMP
        WHERE 
            pipeline_id =  '{{params.pipeline_id}}' AND
            run_ts = '{{params.run_ts}}';

        UPDATE dfs_audit.audit_control.{{params.table_name}}
        SET 
            duration = TIMESTAMPDIFF(SECONDS, start_ts, end_ts)
        WHERE 
            pipeline_id =  '{{params.pipeline_id}}' AND
            run_ts = '{{params.run_ts}}';

"""
# update_task_status = """
#         UPDATE dfs_audit.audit_control.{{params.table_name}}
#         SET status_cd = '{{params.status_cd}}',
#             end_ts = {{params.end_ts}},

#         WHERE 
#             pipeline_id =  '{{params.pipeline_id}}',
#             task_id = '{{params.task_id}},
#             run_id = '{{params.run_id}}'

# """

refresh_stage = """
    ALTER STAGE EXTERNAL_DB.STAGE.AZURE_STAGE refresh
"""


test_snowflake_query = """
    INSERT INTO dfs_audit.audit_control.test
        values(
        %(pipeline_id)s,
        %(task_id)s,
        %(run_id)s
       )
"""