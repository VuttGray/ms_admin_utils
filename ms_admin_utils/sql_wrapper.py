from enum import Enum
from logging import getLogger
from re import match

from pyodbc import connect as odbc_connect, DataError, ProgrammingError, OperationalError

logger = getLogger('logger')
CONNECTION_STRING = "Driver={0};Server={1};Database={2};Trusted_Connection=yes;"


class SqlConfig:
    def __init__(self, **kwargs):
        self.driver = kwargs.pop('driver', '{ODBC Driver 17 for SQL Server}')
        self.master_db = kwargs.pop('master_db', 'master')
        self.ms_db = kwargs.pop('ms_db', 'msdb')


conf = SqlConfig()


def configure(**kwargs):
    global conf
    conf = SqlConfig(**kwargs)


class JobAction(Enum):
    """
    SQL Job step: the action to perform if the step succeeds or fail
    """
    QUIT_WITH_SUCCESS = 1
    QUIT_WITH_FAILURE = 2
    TO_NEXT_STEP = 3
    TO_STEP = 4


class JobFlag(Enum):
    """
    SQL job step: Is an option that controls behavior
    """
    OUTPUT_FILE_OVERWRITE = 0  # Overwrite output file (default)
    OUTPUT_FILE_APPEND = 2  # Append to output file
    STEP_HISTORY = 4  # Write Transact-SQL job step output to step history
    TABLE_OVERWRITE = 8  # Write log to table (overwrite existing history)
    TABLE_APPEND = 16  # Write log to table (append to existing history)
    JOB_HISTORY = 32  # Write all output to job history
    WIN_EVENT = 64  # Create a Windows event to use as a signal for the Cmd jobstep to abort


class JobSubsystem(Enum):
    """
    SQL job step: subsystems
    """
    SQL = 'TSQL'  # (default) Transact-SQL statement
    CMD = 'CmdExec'  # Operating-system command or executable program
    PS = 'PowerShell'  # PowerShell Script


def __parse_db_path(db_path):
    server, db = db_path.split('.')
    return server, db


def execute_wo_transaction(sql_queries: [str], server: str, db: str):
    conn_str = CONNECTION_STRING.format('{' + conf.driver + '}', server, db)
    with odbc_connect(conn_str, autocommit=True, timeout=600) as conn:
        cursor = conn.cursor()
        for sql_query in sql_queries:
            logger.debug('Execute query:\n' + sql_query)
            cursor.execute(sql_query)
            while cursor.nextset():
                pass


def sql_select(sql_query: str, server: str, db: str):
    conn_str = CONNECTION_STRING.format(conf.driver, server, db)
    with odbc_connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        return cursor.fetchall()


def sql_select_1st_row(sql_query: str, server: str, db: str):
    for row in sql_select(sql_query, server, db):
        return row


def sql_update(sql_query, server, db, expected_result=True):
    if not sql_query:
        return
    conn_str = CONNECTION_STRING.format(conf.driver, server, db)
    with odbc_connect(conn_str) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql_query)
            if expected_result:
                result = cursor.fetchone()
                if result.RESULT != "OK":
                    logger.error(result.RESULT)
                    logger.info(sql_query)
                    return result.RESULT
        except (DataError, ProgrammingError, OperationalError) as ex:
            logger.info(sql_query)
            logger.exception(ex)
            return ex.args[-1]


def restore_db(server, db, backup_path, db_folder=None, initial_db_file_names=None,
               recovery_mode=None, set_single_user=True, set_multi_user=True):
    queries = []
    restore_query = ""
    modify_file = ""
    if set_single_user:
        queries.append(f"alter database [{db}] set single_user with rollback immediate;\n")

    restore_query += f"restore database [{db}] from disk = N'{backup_path}' \n" \
                     f"with file = 1, nounload, replace, stats = 5\n"
    if initial_db_file_names and initial_db_file_names[0] != db:
        restore_query += f"\t, move N'{initial_db_file_names[0]}' to N'{db_folder}\\{db}.mdf'\n"
        modify_file += f"alter database [{db}] modify file (name = {initial_db_file_names[0]}, newname = {db});\n"
    if initial_db_file_names and initial_db_file_names[1] != f'{db}_log':
        restore_query += f"\t, move N'{initial_db_file_names[1]}' to N'{db_folder}\\{db}_log.ldf'\n"
        modify_file += f"alter database [{db}] modify file (name = {initial_db_file_names[1]}, newname = {db}_log);\n"
    queries.append(restore_query)

    if modify_file:
        queries.append(modify_file)
    if recovery_mode:
        queries.append(f"alter database [{db}] set recovery {recovery_mode} with no_wait\n")
    if set_multi_user:
        queries.append(f"alter database [{db}] set multi_user\n")

    execute_wo_transaction(queries, server, conf.master_db)


def drop_user(login: str, server: str, db: str):
    execute_wo_transaction([f"exec sp_dropuser '{login}'"], server, db)


def grant_permission(permission: str, sql_object: str, principal: str, server: str, db: str):
    query = f"grant {permission} on [{sql_object}] to [{principal}];"
    sql_update(query, server, db, expected_result=False)
    logger.debug(query)


def get_table_structure(db_path):
    tables = {}
    query = "select t.name as table_name, c.name as column_name " \
            "from sys.objects t " \
            "join sys.columns c on c.object_id = t.object_id " \
            "where t.type_desc  = 'USER_TABLE' and t.name != 'sysdiagrams'" \
            "order by  t.name, c.column_id"
    server, db = __parse_db_path(db_path)
    for row in sql_select(query, server, db):
        if row.table_name in tables:
            tables[row.table_name].append(row.column_name)
        else:
            tables[row.table_name] = [row.column_name]
    return tables


def get_referenced_objects(server, db, name, schema='dbo'):
    query = f"select    r.referenced_entity_name as name, o.type " \
            f"from      sys.dm_sql_referenced_entities('{schema}.{name}', 'OBJECT') r " \
            f"left join sys.objects o on r.referenced_id = o.object_id " \
            f"where     r.referenced_class_desc = 'OBJECT_OR_COLUMN' " \
            f"          and r.referenced_id != object_id('{schema}.{name}') "
    cursor = sql_select(query, server, db)
    return cursor


def get_referencing_objects(server, db, name, schema='dbo'):
    query = f"select    r.referencing_entity_name as name, o.type " \
            f"from      sys.dm_sql_referencing_entities('{schema}.{name}', 'OBJECT') r " \
            f"left join sys.objects o on r.referencing_id = o.object_id "
    cursor = sql_select(query, server, db)
    return cursor


def get_sql_code(sql_object, server, db):
    code = ''
    is_code = True

    for row in sql_select("exec sp_helptext '{0}'".format(sql_object), server=server, db=db):
        if match(r'/[*]+\r\n$', row.Text):
            is_code = False
        if is_code and not match(r'[ \t]*--.*\r\n$', row.Text) and not match(r'[ \t]*\r\n$', row.Text):
            code += row.Text
        if match(r'[*]+/\r\n$', row.Text):
            is_code = True
    return code


def get_table_script(t, server, db):
    sql_script = "exec dbo.SYS_GenerateTableScript " \
                 "@table_name='{0}', " \
                 "@exclude_fk = 1, " \
                 "@exclude_indexes = 1, " \
                 "@exclude_collations = 1, " \
                 "@exclude_default_core_function = 1, " \
                 "@exclude_created_fields = 1, " \
                 "@exclude_updated_fields = 1"
    cursor = sql_select(sql_script.format(t), server=server, db=db)
    for row in cursor:
        return row.Text
    raise UserWarning('Failed during table script generation')


def save_table_structure(file_path, db_path):
    tables = get_table_structure(db_path)
    tables_text = str(tables).replace('],', '],\n').replace('{', '{\n ').replace('}', '\n}')
    with open(file_path, 'w') as f:
        f.write(tables_text)


def load_table_structure(file_path):
    with open(file_path, 'r') as f:
        return eval(f.read())


def compare_tables(tables_src, tables_trg):
    missed_tables = []
    missed_columns = {}
    for t in tables_src:
        if t not in tables_trg:
            missed_tables.append(t)
            continue
        for c in tables_src[t]:
            if c not in tables_trg[t]:
                if t in missed_columns:
                    missed_columns[t].append(c)
                else:
                    missed_columns[t] = [c]
    return missed_tables, missed_columns


def compare_table_structures(file_path, db_path):
    tables_template = load_table_structure(file_path)
    tables_current = get_table_structure(db_path)
    mt, mc = compare_tables(tables_template, tables_current)
    if mt:
        print('Tables missed in the current db:')
        for t in mt:
            print(t)
    if mc:
        print('Tables with missed columns in the current db:')
        for t in mc:
            for c in mc[t]:
                print(f'{t}.{c}')
    mt, mc = compare_tables(tables_current, tables_template)
    if mt:
        print('New tables in the current db:')
        for t in mt:
            print(t)
    if mc:
        print('Tables with new columns in the current db:')
        for t in mc:
            for c in mc[t]:
                print(f'{t}.{c}')


def get_dbs(server: str) -> list:
    query = "select @@SERVERNAME as server" \
            "      ,d.name " \
            "      ,d.create_date " \
            "      ,d.compatibility_level " \
            "      ,d.user_access_desc " \
            "      ,d.is_read_only " \
            "      ,d.state_desc " \
            "      ,d.is_in_standby " \
            "      ,d.is_cleanly_shutdown " \
            "      ,d.recovery_model_desc " \
            "      ,d.is_fulltext_enabled " \
            "      ,d.is_master_key_encrypted_by_server " \
            "      ,d.is_broker_enabled " \
            "      ,d.is_encrypted " \
            "from sys.databases d " \
            "where name not in ('master','tempdb','model','msdb')"
    cursor = sql_select(query, server, conf.master_db)
    return [MsDatabase(row) for row in cursor]


def get_tables(server, db, like_filter=None):
    query = "select name from sys.tables"
    if like_filter:
        query += f" where name like '{like_filter}'"
    cursor = sql_select(query, server, db)
    return [t.name for t in cursor]


def get_views(server, db, like_filter=None):
    query = f"select name from sys.objects"
    if like_filter:
        query += f" where name like '{like_filter}'"
    cursor = sql_select(query, server, db)
    return [t.name for t in cursor]


def get_columns(object_name, server, db):
    # First join to sys.types - trying to get system types to avoid custom user types
    # Second join to sys.types - getting the original type
    query = "select    c.name, isnull(t.name, ut.name) as type, c.max_length, c.precision, c.scale, c.is_nullable " \
            "from      sys.columns c " \
            "left join sys.types t on t.user_type_id = c.system_type_id " \
            "left join sys.types ut on ut.user_type_id = c.user_type_id " \
            "where     c.object_id = object_id('" + object_name + "') " \
            "order by  c.column_id"
    return sql_select(query, server, db)


def get_simple_type(column):
    if column.type == 'timestamp':
        return 'varchar(50)'
    if column.type in ['varchar', 'char']:
        return f"{column.type}({'max' if column.max_length == -1 or column.max_length > 8000 else column.max_length})"
    if column.type in ['nvarchar', 'nchar']:
        return f"{column.type}({'max' if column.max_length == -1 or column.max_length > 4000 else column.max_length})"
    if column.type == 'decimal':
        return f"{column.type}({column.precision}, {column.scale})"
    return column.type


def get_sql_message(sql_message_id, server, db):
    query = f"select text as message from sys.messages where language_id = 1033 and message_id = {sql_message_id}"
    cursor = sql_select(query, server, db)
    for row in cursor:
        return row.message


class MsDatabase:
    def __init__(self, data_row):
        self.server = data_row.server
        self.name = data_row.name
        self.create_date = data_row.create_date
        self.compatibility_level = data_row.compatibility_level
        self.user_access = data_row.user_access_desc
        self.is_read_only = data_row.is_read_only
        self.state = data_row.state_desc
        self.is_in_standby = data_row.is_in_standby
        self.is_cleanly_shutdown = data_row.is_cleanly_shutdown
        self.recovery_model = data_row.recovery_model_desc
        self.is_fulltext_enabled = data_row.is_fulltext_enabled
        self.is_master_key_encrypted_by_server = data_row.is_master_key_encrypted_by_server
        self.is_broker_enabled = data_row.is_broker_enabled
        self.is_encrypted = data_row.is_encrypted


def sql_job_add_next_step(server: str,
                          job_id: str,
                          step_name: str,
                          database_name: str,
                          command: str,
                          cmdexec_success_code: int = 0,
                          on_success_action: int = JobAction.QUIT_WITH_SUCCESS.value,
                          on_fail_action: int = JobAction.QUIT_WITH_SUCCESS.value,
                          retry_attempts: int = 0,
                          retry_interval: int = 0,
                          os_run_priority: int = 0,
                          subsystem: str = JobSubsystem.SQL.value,
                          flags: int = JobFlag.OUTPUT_FILE_OVERWRITE.value):
    query = f"select isnull((select max(step_id) from [dbo].[sysjobsteps] where job_id = '{job_id}'), 0) + 1 as step_id"
    step_id = sql_select_1st_row(query, server, conf.ms_db).step_id

    query = f"exec [dbo].[sp_add_jobstep] " \
            f"@job_id = '{job_id}'" \
            f",@step_name = N'{step_name}'" \
            f",@step_id = {step_id}" \
            f",@cmdexec_success_code = {cmdexec_success_code}" \
            f",@on_success_action = {on_success_action}" \
            f",@on_fail_action = {on_fail_action}" \
            f",@retry_attempts = {retry_attempts}" \
            f",@retry_interval = {retry_interval}" \
            f",@os_run_priority = {os_run_priority}" \
            f",@subsystem = N'{subsystem}'" \
            f",@command = N'{command}'" \
            f",@database_name = N'{database_name}'" \
            f",@flags = {flags}"
    result = sql_update(query, server, conf.ms_db, expected_result=False)
    logger.debug(f"Step {step_name} created")
    return result


def get_sql_job_steps(server: str, job_id: str, ordering: str = 'asc'):
    query = "select j.name as job_name, s.* " \
            "from dbo.sysjobsteps s " \
            "join dbo.sysjobs j on j.job_id = s.job_id " \
            f"where s.job_id = '{job_id}' " \
            f"order by s.step_id {ordering}"
    return sql_select(query, server, conf.ms_db)


def sql_job_remove_steps(server: str, job_id: str):
    for step in get_sql_job_steps(server, job_id, 'desc'):
        query = f"exec [dbo].[sp_delete_jobstep] @job_id = '{job_id}' ,@step_id = {step.step_id}"
        result = sql_update(query, server, conf.ms_db, expected_result=False)
        if result:
            return result
        logger.debug(f"SQl job {step.job_name}: step {step.step_name} deleted")
