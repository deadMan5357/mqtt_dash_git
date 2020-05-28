import dash
from datetime import datetime
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
import paho.mqtt.subscribe as subscribe
from class_etl import *

df = []

app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

conn = connect_sql()

app = dash.Dash(__name__)
app.layout = html.Div(
    [html.H2("lathe_1"),
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=1*7000
        )
    ]
)

@app.callback(Output('live-graph', 'figure'),
              [Input('graph-update', 'n_intervals')])

def update_graph_scatter(input_data):
    #print(pathname)
    message = subscribe.simple("/lathe_1", hostname="mqtt.eclipse.org", retained=False, msg_count=2)
    msg = str(message[0].payload,"utf-8")
    output = msg.split('_')
    output.insert(len(output),fetch_sql_time(conn))
    df.insert(len(df),output)
    X = [row[4] for row in df]
    Y = [row[0] for row in df]
    Y1 = [row[1] for row in df]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=X, y=Y,name = 'current_A'))
    fig.add_trace(go.Scatter(x=X, y=Y1,name = 'current_B'))

    return fig


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8080 ,debug=True)
