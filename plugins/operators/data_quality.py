from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_dict=dict(),
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_dict=table_dict

    def execute(self, context):
        # Hooks 
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for tbl in self.table_dict.keys():
            # check row counts
            self.log.info(f'Checking row count for {tbl}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tbl}")
            if records is None or len(records[0]) <1:
                self.log.info(f"Row count data check failed for {tbl}")
                raise ValueError(f"Row count data check failed for {tbl}")
            else:
                self.log.info(f"Row count data check passed for {tbl} ({records[0][0]} records found)")
            
            # check null counts 
            self.log.info(f'Checking null count for {tbl}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tbl} WHERE {self.table_dict[tbl]} IS NULL")
            if records is None or len(records[0]) <1:
                self.log.info(f"Null count data check failed for {tbl}")
                raise ValueError(f"Null count data check failed for {tbl}")
            else:
                self.log.info(f"Null count data check passed for {tbl} ({records[0][0]} records found)")