from bokeh.models import ColumnDataSource
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.io import curdoc
from bokeh.layouts import column, row, widgetbox
import pandas as pd
from bokeh.io import show
from bokeh.models import Div
from kafka import KafkaConsumer
from json import loads


def get_message(consumer):
    for message in consumer:
        location = message.value["payload"]["User"]["Location"]
        if location is not None:
            try:
            	new_value = data.loc[location]["Number"] + 1
            except KeyError:
                new_value = 0
            data.loc[location] = {"Location": location, "Number": new_value}
        return data


def my_callback():
    data_kafka = get_message(consumer)
    data_kafka = data_kafka.sort_values(["Number", "Location"], ascending=False)
    data_table.source.data = data_kafka
    
   
consumer = KafkaConsumer(
    'twitter_status_connect',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-pierre',
     value_deserializer=lambda x: loads(x.decode('utf-8')))


data = pd.DataFrame(columns=["Lieu", "Location", "Number"])
data = data.set_index(['Lieu'])


source = ColumnDataSource(data=data)
Columns = [TableColumn(field=Ci, title=Ci) for Ci in data.columns] # bokeh columns
data_table = DataTable(columns=Columns, source=source,
                       width=500,
                       sizing_mode="stretch_both"
                      #fit_columns=True
                      ) # bokeh table


inputs = widgetbox(data_table)
curdoc().add_root(column(inputs))
curdoc().add_periodic_callback(my_callback, 10)


