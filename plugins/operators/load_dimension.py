from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    sql = """
        INSERT INTO {} 
        {}; 
        COMMIT;
    """ 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.load_sql=load_sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        # Hooks 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # clear out dimenson table 
        self.log.info(f"Cleaning out old data from {self.table}")
        redshift.run("TRUNCATE TABLE {};".format(self.table))
        
        formatted_sql = LoadDimensionOperator.sql.format(
            self.table,
            self.load_sql
        )
        redshift.run(formatted_sql)
