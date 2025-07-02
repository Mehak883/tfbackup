import logging
import azure.functions as func
from . import main
app = func.FunctionApp()

@app.timer_trigger(schedule="0 30 19 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
    main.main(myTimer)