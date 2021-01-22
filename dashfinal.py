#!/usr/bin/env python
# coding: utf-8

# In[1]:


import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
from collections import deque
import sqlite3
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
import pandas_datareader.data as web
import datetime


# In[2]:


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

button = (html.A(html.Button('Code: Github', className='mr-2'),
    href='https://github.com/muchanem/scrum03')
),

card_main = dbc.Card(
    [
        dbc.CardImg(src="/assets/ncssm1.jpg", top=True, bottom=False,
                    title="Credit Suisse", alt=''),
    ],
    color="light",   # https://bootswatch.com/default/ for more card colors
    inverse=True,   # change color of text (black or white)
    outline=False,  # True = remove the block colors from the background and header
)
card_graph1 = dbc.Card(
    [
        dbc.CardImg(src="/assets/graph1.png", top=True, bottom=False,
                    title="Saathvik was here", alt=''),
        dbc.CardBody(
            [
                html.H4("Linear Regression: Weighted Sentiment and Volume", className="card-title"),
                
            ]
        ),
    ],
    color="dark",   # https://bootswatch.com/default/ for more card colors
    inverse=True,   # change color of text (black or white)
    outline=False,  # True = remove the block colors from the background and header
)
card_graph2 = dbc.Card(
    [
        dbc.CardImg(src="/assets/graph1.png", top=True, bottom=False,
                    title="Graph 2", alt=''),
        dbc.CardBody(
            [
                html.H4("Linear Regression: Normal Sentiment and Volume", className="card-title"),
                
            ]
        ),
    ],
    color="dark",   # https://bootswatch.com/default/ for more card colors
    inverse=True,   # change color of text (black or white)
    outline=False,  # True = remove the block colors from the background and header
)
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(card_main, width=2,style={'width': '10%', 'display': 'inline-block',
                                 'border-radius': '15px',
                                 'box-shadow': '2px 2px 2px grey',
                                 'background-color': '#f9f9f9',
                                 'padding': '10px',
                                 'margin-bottom': '10px'}),
        dbc.Col(html.H1('Live Sentiment Dashboard',className='text-center mb-4 font-weight-bolder text-black-50',))
    ]),
    dbc.Row([
        dbc.Col(html.H1('Real World Data Science with Credit Suisse',className='text-center mb-4 text-muted')),
    ]),
    dbc.Row([
        dbc.Col(button, className='row justify-content-center'),
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='live-graph', animate=True),
            dcc.Interval(
                id='graph-update',
                n_intervals=1*1000)])
    ]),
    dbc.Row([
        dbc.Col(card_graph1, width={'size':4, 'offset':1}, style={'width': '30%', 'display': 'inline-block',
                                 'border-radius': '15px',
                                 'box-shadow': '4px 4px 4px grey',
                                 'background-color': '#f9f9f9',
                                 'padding': '10px',
                                 'margin-bottom': '10px'}),
        dbc.Col(card_graph2, width={'size':4, 'offset':2}, style={'width': '30%', 'display': 'inline-block',
                                 'border-radius': '15px',
                                 'box-shadow': '4px 4px 4px grey',
                                 'background-color': '#f9f9f9',
                                 'padding': '10px',
                                 'margin-bottom': '10px'}),
    ])
        
], fluid=True, style={'padding': '15px 40px 40px 40px'})

@app.callback(Output('live-graph', 'figure'), [Input ('graph-update', 'n_intervals')])

def update_graph_scatterg(graph_update):
    try:
        conn = sqlite3.connect('twitter.db')
        c = conn.cursor()
        df = pd.read_sql("SELECT * FROM sentiment WHERE tweet LIKE '%Biden%' ORDER BY unix DESC LIMIT 1000", conn)
        df.sort_values('unix', inplace=True)
        df['sentiment_smoothed'] = df['sentiment'].rolling(int(len(df)/5)).mean()
        
        df['date'] = pd.to_datetime(df['unix'],unit='ms')
        df.set_index('date', inplace=True)
        
        df = df.resample('3s').mean()
        df.dropna(inplace=True)
        X = df.index[-100:]
        Y = df.sentiment_smoothed[-100:]

        data = plotly.graph_objs.Scatter(
                x=X,
                y=Y,
                name='Scatter',
                mode= 'lines+markers'
                )

        return {'data': [data],'layout' : go.Layout(xaxis=dict(range=[min(X),max(X)]),
                                                    yaxis=dict(range=[min(Y),max(Y)]),)}

    except Exception as e:
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')


if __name__ == '__main__':
    app.run_server()


# In[ ]:




