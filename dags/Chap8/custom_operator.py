import json
import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from Chap8.hooks import MovielensHook


class MovielensFetchRatingsOperator(BaseOperator):
    #1 define templates task
    template_fields=("_start_date", "_end_date", "_output_path")
    #2 take default arguments
    @apply_defaults
    def __init__(self,conn_id,start_date,end_date,output_path,**kwargs):
        #predefined parameters
        super(MovielensFetchRatingsOperator,self).__init__(**kwargs)
        self._conn_id=conn_id
        self._start_date=start_date
        self._end_date=end_date
        self._output_path=output_path
    def execute(self,context):
        hook=MovielensHook(self._conn_id)
        try:
            data=hook.get_ratings(
                    start_date=self._start_date,
                    end_date=self._end_date,
                    batch_size=100
                )
        except:
            print('connect api fail')
        outout_dir=os.path.dirname(self._output_path)
        os.makedirs(outout_dir,exist_ok=True)
        with open(self._output_path,"w") as f:
            json.dump(data,fp=f)
        

