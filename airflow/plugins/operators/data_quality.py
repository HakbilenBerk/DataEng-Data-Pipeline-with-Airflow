from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to run data quality checks by comparing test query results and geiven expected result
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql = "",
                 result = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.result = result

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Test query is running: {}".format(self.sql))
        self.log.info("Expected result: {}".format(str(self.result)))
        records = redshift.get_records(self.sql)
        if records [0] != self.result:
            raise ValueError("Quality check did not pass the test! {} != {}".format(records,self.result))
        else:
            self.log.info("Quality check passed the test! {} == {}".format(records,self.result))