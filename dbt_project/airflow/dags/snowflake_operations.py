""""Contains DDL queres for Audit tables"""

create_dim_pipeline = """
    CREATE TRANSIENT TABLE IF NOT EXISTS {{params.db}}.{{params.schema}}.{{ params.table_name }}
        (
            pipeline_id VARCHAR(25),
            pipeline_name VARCHAR(25),
            active_status_ind VARCHAR(25)
        );
"""

create_dim_task = """
    CREATE TRANSIENT TABLE IF NOT EXISTS {{params.db}}.{{params.schema}}.{{ params.table_name }}
        (
            task_id VARCHAR(100),
            pipeline_id VARCHAR(25),
            pipeline_name VARCHAR(100),
            task_name VARCHAR(100),
            landing_directory VARCHAR(100),
            file_naming_pattern VARCHAR(25),
            active_status_ind VARCHAR(25)
            
            
        );
"""

create_fct_pipeline = """
    CREATE TRANSIENT TABLE IF NOT EXISTS {{params.db}}.{{params.schema}}{{ params.table_name }}
        (
            pipeline_id VARCHAR(25),
            pipeline_name VARCHAR(100),
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
    CREATE TRANSIENT TABLE IF NOT EXISTS  {{params.db}}.{{params.schema}}.{{ params.table_name }}
        (
            pipeline_id VARCHAR(25),
            pipeline_name VARCHAR(100),
            run_id VARCHAR(100),
            task_id VARCHAR(100),
            task_name VARCHAR(100),
            run_ts TIMESTAMP,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            duration INT,
            status_cd VARCHAR(20),
            error_message VARCHAR(100),
            error_cd VARCHAR(25)

        );
"""

# insert_dim_task = """

#     INSERT INTO  {{params.db}}.{{params.schema}}.{{ params.table_name }}
#         (pipeline_id,
#             task_id,
#             task_name,
#             landing_directory,
#             file_naming_pattern,
#             active_status_ind )
#         VALUES ('{{params.pipeline_id}}', {{params.db}}.{{params.schema}}.TASK_ID.nextval , '{{params.task_name}}', 'external/', '*csv', 'Y'
#         );
# """

insert_dim_task = """
    INSERT INTO {{params.db}}.{{params.schema}}.{{params.table_name}}
    (PIPELINE_ID, PIPELINE_NAME, TASK_ID, TASK_NAME, landing_directory, file_naming_pattern, active_status_ind)
    SELECT pipeline_id, pipeline_name, task_id_seq.nextval, '{{params.task_name}}', '{{params.landing_dir}}', '*csv', 'Y'
    FROM dim_pipeline
    WHERE pipeline_name='{{params.pipeline_name}}' and active_status_ind='Y'
"""



insert_dim_pipeline = """

    INSERT INTO  {{params.db}}.{{params.schema}}.{{ params.table_name }}
        (pipeline_id,
            pipeline_name,
            active_status_ind)
        VALUES ( {{params.db}}.{{params.schema}}.PIPELINE_ID_SEQ.nextval,  '{{params.pipeline_name}}', 'Y'
        );
"""


read_dim_table = """
        SELECT * FROM {{params.db}}.{{params.schema}}.{{params.table_name}}
"""


insert_task_status = """
    INSERT INTO {{params.db}}.{{params.schema}}.{{params.fact_table_name}}
        select max(pipeline_id) as pipeline_id, pipeline_name as pipeline_name,  task_id as task_id, task_name as task_name, null as run_id, '{{params.run_ts}}' as run_ts, null as start_ts,null as end_ts,null as duration, 'Pending' as status, 
        null as error_message,null as error_cd
        from {{params.db}}.{{params.schema}}.{{params.dim_table_name}}
        where pipeline_name='{{params.pipeline_name}}'

"""

insert_pipeline_status = """
    INSERT INTO {{params.db}}.{{params.schema}}.{{params.fact_table_name}}
        select max(pipeline_id) as pipeline_id, '{{params.pipeline_name}}' as pipeline_name, null as run_id, '{{params.run_ts}}' as run_ts, null as start_ts,null as end_ts,null as duration, 'Pending' as status, null as error_message,null as error_cd,
        null as created_ts, null as created_by, null as modified_ts, null as modified_by
        from {{params.db}}.{{params.schema}}.{{params.dim_table_name}}
        where pipeline_name='{{params.pipeline_name}}'

"""

update_task_status_pre = """
        UPDATE {{params.db}}.{{params.schema}}.{{params.task_table_name}} tsk
        FROM {{params.db}}.{{params.schema}}.{{params.pipeline_table_name}} pipeline
        SET 
            tsk.run_id = pipeline.run_id,
            tsk.status_cd = '{{params.status_cd}}',
            tsk.start_ts = CURRENT_TIMESTAMP

        WHERE 
            tsk.task_name= '{{params.task_name}}' AND
            tsk.run_ts = '{{params.run_ts}}'

"""

update_task_status_post = """
        UPDATE {{params.db}}.{{params.schema}}.{{params.task_table_name}}
        SET 
            status_cd = '{{params.status_cd}}',
            end_ts = CURRENT_TIMESTAMP

        WHERE 
            task_name = '{{params.task_name}}' AND
            run_ts = '{{params.run_ts}}';
        
        UPDATE {{params.db}}.{{params.schema}}.{{params.task_table_name}}
        SET 
            duration = TIMESTAMPDIFF(SECONDS, start_ts, end_ts)

        WHERE 

            task_name = '{{params.task_id}}' AND
            run_ts = '{{params.run_ts}}'

"""

update_pipeline_status_pre = """
        UPDATE {{params.db}}.{{params.schema}}.{{params.table_name}}
        SET 
            run_id = {{params.db}}.{{params.schema}}.run_id_seq.nextval,
            status_cd = '{{params.status_cd}}',
            start_ts = CURRENT_TIMESTAMP
        WHERE 

            run_ts = '{{params.run_ts}}'

"""

update_pipeline_status_post = """
        UPDATE {{params.db}}.{{params.schema}}.{{params.table_name}}
        SET status_cd = '{{params.status_cd}}',
            end_ts = CURRENT_TIMESTAMP
        WHERE 

            run_ts = '{{params.run_ts}}';

        UPDATE {{params.db}}.{{params.schema}}.{{params.table_name}}
        SET 
            duration = TIMESTAMPDIFF(SECONDS, start_ts, end_ts)
        WHERE 

            run_ts = '{{params.run_ts}}';

"""
# update_task_status = """
#         UPDATE {{params.db}}.{{params.schema}}.{{params.table_name}}
#         SET status_cd = '{{params.status_cd}}',
#             end_ts = {{params.end_ts}},

#         WHERE 
#             pipeline_id =  '{{params.pipeline_id}}',
#             task_id = '{{params.task_id}},
#             run_id = '{{params.run_id}}'

# """

refresh_stage = """
    ALTER STAGE {{params.db}}.{{params.schema}}.{{params.stage_name}} refresh;
"""


test_snowflake_query = """
    INSERT INTO {{params.db}}.{{params.schema}}.test
        values(
        %(pipeline_id)s,
        %(task_id)s,
        %(run_id)s
       )
"""
