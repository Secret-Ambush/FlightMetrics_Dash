#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import os
import sqlalchemy
import snowflake.sqlalchemy
import snowflake.connector.pandas_tools

# execute to ensure local proxy variables not used
for k in ['HTTP_PROXY', 'HTTPS_PROXY']:
    os.environ.pop(k, None)
    os.environ.pop(k.lower(), None)

engine = sqlalchemy.create_engine(
    os.environ['snowflake_conn'],
    execution_options=dict(autocommit=True)
)


import dash
from dash import Dash, html, ctx, callback, dcc
import pandas as pd
import plotly.express as px
import sqlalchemy
from dash.dependencies import Input, Output, State
from snowflake.sqlalchemy import URL
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template


app = dash.Dash(external_stylesheets=[dbc.themes.SLATE])
load_figure_template('DARKLY')

server = app.server

# Dash app layout
app.layout = html.Div(
    children=[
        html.Br(),
        html.H1("Metric Analysis", style={'textAlign': 'center'}),
        html.Br(),
        html.Br(),
        html.Div(
            html.Img(
            src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d0/Emirates_logo.svg/1200px-Emirates_logo.svg.png",
            style={"width": "100px", "height": "100px", "margin": "20px", "align-self": "flex-start", "margin-top": "-140px"}
            )
        ),
        html.Div(
            children=[
              html.Div(
                    children=[
                        html.Label("Origin"),  # Add label for origin dropdown
                        dcc.Dropdown(
                            id="origin-dropdown",
                            placeholder="Select Origin",
                            multi=True,
                            style={"background-color": "#f5f5f5", "color": "black"}  # Modify dropdown style
                        )
                    ],
                    style={"width": "200px", "display": "inline-block"}
                ),
                html.Div(
                    children=[
                        html.Label("Destination"),  # Add label for destination dropdown
                        dcc.Dropdown(
                            id="dest-dropdown",
                            placeholder="Select Destination",
                            multi=True,
                            style={"background-color": "#f5f5f5", "color": "black"}  # Modifying dropdown style
                        )
                    ],
                    style={"width": "200px", "display": "inline-block"}
                ),
                html.Div(
                    children=[
                        html.Label("Snap Date"),  # Add label for snap date dropdown
                        dcc.Dropdown(
                            id="snap-date-dropdown",
                            placeholder="Select Snap Date",
                            multi=False,
                            value = "2021-09-15",
                            style={"background-color": "#f5f5f5", "color": "black"}  # Modifying dropdown style
                        )
                    ],
                    style={"width": "200px", "display": "inline-block"}
                ),
                html.Div(
                    children=[
                        html.Label("Dep Date (booking curve)"),  # Add label for seg date dropdown
                        dcc.Dropdown(
                            id="seg-date-dropdown",
                            placeholder="Select Seg Dep Date",
                            multi=False,
                            value = "2021-09-15",
                            style={"background-color": "#f5f5f5", "color": "black"}  # Modifying dropdown style
                        )
                    ],
                    style={"width": "200px", "display": "inline-block"}
                ),
                html.Div(
                    children=[
                        html.Label("Flight Number"),  # Add label for flight number dropdown
                        dcc.Dropdown(
                            id="flight-no",
                            placeholder="Select Flight number",
                            multi=True,
                            style={"background-color": "#f5f5f5", "color": "black"}  # Modifying dropdown style
                        )
                    ],
                    style={"width": "200px", "display": "inline-block"}
                ),
                html.Div(
                    html.Button("Add", id="add-button", n_clicks=0),
                    style={"width": "100px", "margin": "10px"}  # Modify button style
                ),
                html.Div(
                    html.Button("Clear All", id="clear-button", n_clicks=0),
                    style={"width": "100px", "margin": "10px"}  # Modify button style
                ),
            ],
            style={"display": "flex", "justify-content": "space-between"}
        ),
        html.Br(),
        html.Div(id="selected-options"),
        html.Div(id="stored-options", style={"display": "none"}),
        html.Div(id="errortxt"),
        html.Div(
            html.Button("PLOT", id="submit-button", n_clicks=0),
            style={"display": "flex", "justify-content": "center", "margin-top": "10px"}
        ),
        html.Br(),
        dcc.Markdown("#### **‏‏‎ ‎‏‏‎ ‎‏‏‎ ‎Booking Curve:**"),
        html.Div(
            style={"display": "flex", "justify-content": "center", "align-items": "center", "height": "75vh"},
            children=[
                html.Div(
                dcc.Graph(id="graph"),  # Add a div to hold the graph
                style={"width": "80%", "height": "400px"},
                )
            ]
        ),
        dcc.Markdown("#### **‏‏‎ ‎‏‏‎ ‎‏‏‎ ‎Aggregate Graph:**"),
        html.Div(
            style={"display": "flex", "justify-content": "center", "align-items": "center", "height": "75vh"},
            children=[
                html.Div(
                dcc.Graph(id="aggr_graph"),  # Add a div to hold the graph
                style={"width": "80%", "height": "400px"},
                )
            ]
        ), 
    ]
)

# Define separate lists to store selected options
origin_list = []
destination_list = []
snap_date_list = []
flight_number_list = []
seg_date_list = []

snowflake_table = 'INVENTORY_SAMPLE_COPY'

# Callback to update selected origins list
@app.callback(
    Output("origin-dropdown", "options"),
    [Input("snap-date-dropdown", "value")]
)
def update_origin_options(selected_snap_dates):
    if selected_snap_dates:
        snap_dates_str = selected_snap_dates
        origin_options_query = f"""
            SELECT DISTINCT LEG_ORIGIN
            FROM {snowflake_table} ORDER BY LEG_ORIGIN
        """
    else:
        origin_options_query = f"""
            SELECT DISTINCT LEG_ORIGIN
            FROM {snowflake_table} ORDER BY LEG_ORIGIN
        """

    with engine.connect() as conn:
        origin_options = conn.execute(origin_options_query)
        return [{"label": row[0], "value": row[0]} for row in origin_options]

# Callback to update selected destinations list
@app.callback(
    Output("dest-dropdown", "options"),
    [Input("origin-dropdown", "value")]
)
def update_destinations(selected_origins):
    print(selected_origins)
    if selected_origins:
        origins_str = "','".join(selected_origins)
        query = f"""
            SELECT DISTINCT LEG_DESTINATION
            FROM {snowflake_table}
            WHERE LEG_ORIGIN IN ('{origins_str}')
            ORDER BY LEG_DESTINATION
        """
        with engine.connect() as conn:
            result = conn.execute(query)
            destinations = [row[0] for row in result]
            return [{"label": dest, "value": dest} for dest in destinations]
    return []
# Callback to update selected snap dates list
@app.callback(
    Output("snap-date-dropdown", "options"),
    [Input("origin-dropdown", "value"),
     Input("dest-dropdown", "value")]
)
def update_snap_dates(selected_origins, selected_destinations):
    if selected_origins and selected_destinations:
        origins_str = "','".join(selected_origins)
        destinations_str = "','".join(selected_destinations)
        snap_date_options_query = f"""
            SELECT DISTINCT SNAP_DATE
            FROM {snowflake_table}
            WHERE LEG_ORIGIN IN ('{origins_str}')
            AND LEG_DESTINATION IN ('{destinations_str}') ORDER BY SNAP_DATE DESC
        """

        with engine.connect() as conn:
            snap_date_options = conn.execute(snap_date_options_query)
            return [{"label": row[0], "value": row[0]} for row in snap_date_options]

    return []

# Callback to update selected seg dates list
@app.callback(
    Output("seg-date-dropdown", "options"),
    [Input("origin-dropdown", "value"),
     Input("dest-dropdown", "value")]
)
def update_seg_dates(selected_origins, selected_destinations):
    if selected_origins and selected_destinations:
        origins_str = "','".join(selected_origins)
        destinations_str = "','".join(selected_destinations)
        seg_date_options_query = f"""
            SELECT DISTINCT SEG_DEP_DATE
            FROM {snowflake_table}
            WHERE LEG_ORIGIN IN ('{origins_str}')
            AND LEG_DESTINATION IN ('{destinations_str}') ORDER BY SEG_DEP_DATE DESC
        """

        with engine.connect() as conn:
            seg_date_options = conn.execute(seg_date_options_query)
            return [{"label": row[0], "value": row[0]} for row in seg_date_options]

    return []

# Callback to update selected flight numbers list
@app.callback(
    Output("flight-no", "options"),
    [Input("origin-dropdown", "value"),
     Input("dest-dropdown", "value"),
     Input("snap-date-dropdown", "value"),
    Input("seg-date-dropdown", "value")]
)
def update_flight_numbers(selected_origins, selected_destinations, selected_snap_dates, selected_seg_dates):
    if selected_origins and selected_destinations and selected_snap_dates and selected_seg_dates:
        origins_str = "','".join(selected_origins)
        destinations_str = "','".join(selected_destinations)
        snap_dates_str = selected_snap_dates
        seg_dates_str = selected_seg_dates
        flight_number_options_query = f"""
            SELECT DISTINCT FLTNUM
            FROM {snowflake_table}
            WHERE LEG_ORIGIN IN ('{origins_str}')
            AND LEG_DESTINATION IN ('{destinations_str}')
            AND SNAP_DATE = '{snap_dates_str}' ORDER BY FLTNUM
        """
        
        with engine.connect() as conn:
            flight_number_options = conn.execute(flight_number_options_query)
            return [{"label": row[0], "value": row[0]} for row in flight_number_options]

    return []

#Define a function to generate the selected options text
def generate_options_text():
    global origin_list, destination_list, snap_date_list, flight_number_list
    options_text = "Selected Options:\n\n"
    options_text += f"Origin: {', '.join(filter(None, origin_list))}\n"
    options_text += f"Destination: {', '.join(filter(None, destination_list))}\n"
    options_text += f"Snap Date: {snap_date_list[0] if snap_date_list else ' '}\n"
    options_text += f"Seg Date: {seg_date_list[0] if seg_date_list else ' '}\n"
    options_text += f"Flight Number: {', '.join(filter(None, flight_number_list))}"
    return options_text

# Callback to display selected options
@app.callback(
    Output("selected-options", "children"),
    [Input("stored-options", "children"),
     Input("add-button", "n_clicks"),
     Input("clear-button", "n_clicks")]
)
def display_selected_options(stored_options, add_clicks, clear_clicks):
    options_text = generate_options_text()
    return html.Pre(options_text)

@app.callback(
    [Output("stored-options", "children"),
     Output("origin-dropdown", "value"),
     Output("dest-dropdown", "value"),
     Output("snap-date-dropdown", "value"),
     Output("seg-date-dropdown", "value"),
     Output("flight-no", "value")],
    [Input("add-button", "n_clicks"),
     Input("clear-button", "n_clicks")],
    [State("origin-dropdown", "value"),
     State("dest-dropdown", "value"),
     State("snap-date-dropdown", "value"),
     State("seg-date-dropdown", "value"),
     State("flight-no", "value")]
)
def update_selected_options(add_clicks, clear_clicks, origin_value, dest_value, snap_date_value, seg_date_value, flight_no_value):
    global origin_list, destination_list, snap_date_list, seg_date_list, flight_number_list
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    
    if "add-button" == ctx.triggered_id and all([origin_value, dest_value, snap_date_value, seg_date_value, flight_no_value]):
        origin_list += origin_value
        destination_list += dest_value
        snap_date_list.append(snap_date_value)
        seg_date_list.append(seg_date_value)
        flight_number_list += flight_no_value
        print(snap_date_value)
    
    if "clear-button" == ctx.triggered_id:
        origin_list = []
        destination_list = []
        snap_date_list = []
        seg_date_list = []
        flight_number_list = []
    
    stored_options = {
        "origin_list": origin_list,
        "destination_list": destination_list,
        "snap_date_list": snap_date_list[0] if snap_date_list else '',
        "seg_date_list": seg_date_list[0] if seg_date_list else '',
        "flight_number_list": flight_number_list
    }

    return stored_options, None, None, None, None, None

# Callback to update and display the graph
@app.callback(
    Output("graph", "figure"),
    Output("errortxt", "children"),
    [Input("submit-button", "n_clicks")]
)
def update_graph(n_clicks):
    global origin_list, destination_list, seg_date_list, snap_date_list, flight_number_list

    # Check if all required options are selected
    if not all([origin_list, destination_list, seg_date_list, flight_number_list]):
        error_msg = "⚠ Select all the options"
        return {}, error_msg

    # Call the plot function with selected options
    fig = plot(flight_number_list, origin_list, destination_list, seg_date_list)

    # Return the plotly figure
    return fig, ""

# Define the plot function to generate the plotly figure
def plot(flt_nums, leg_origins, leg_destinations, seg_date):
    # Create a list to store the individual DataFrames
    dfs = []
    seg_date = seg_date[0]

    for flt_num, leg_origin, leg_destination in zip(flt_nums, leg_origins, leg_destinations):
        if None in (flt_num, leg_origin, leg_destination):
            print(f"Skipping iteration due to 'None' value: FLTNUM: {flt_num}, LEG_ORIGIN: {leg_origin}, LEG_DESTINATION: {leg_destination}")
            continue
        
        # Prepare the SQL query
        query = f"SELECT * FROM INVENTORY_SAMPLE_COPY WHERE FLTNUM = {flt_num} AND LEG_ORIGIN = '{leg_origin}' AND LEG_DESTINATION = '{leg_destination}' AND SEG_DEP_DATE = '{seg_date}' ORDER BY SNAP_DATE ASC"

        # Execute the query and fetch the results into a DataFrame
        df = pd.read_sql(query, engine).rename(columns=str.upper)

        # Check if the data exists
        if df.empty:
            print(f"No data available for FLTNUM: {flt_num}, LEG_ORIGIN: {leg_origin}, LEG_DESTINATION: {leg_destination}, SEG_DEP_DATE: {seg_date}")
        else:
            # Append the current DataFrame to the list
            dfs.append(df)

    # Concatenate the individual DataFrames into a single DataFrame
    combined_df = pd.concat(dfs)

    # Determine the number of elements in the list
    num_elements = len(dfs)

    # Generate a color palette based on the number of elements
    colors = px.colors.qualitative.Plotly[:num_elements]

    # Create a scatterplot using Plotly Express
    fig = px.scatter(combined_df, x='SNAP_DATE')

    # Plot each individual DataFrame with a different color
    for i in range(num_elements):
        flt_num = flt_nums[i]
        df = dfs[i]
        label = f"({flt_num})"
        color = colors[i]

        # Add scatter traces for each data series
        fig.add_scatter(x=df['SNAP_DATE'], y=df['SEATS_SOLD'], mode='lines+markers', name=f'Passengers {label}', marker_color=color, visible='legendonly')
        fig.add_scatter(x=df['SNAP_DATE'], y=df['CAPACITY'], mode='lines+markers', name=f'Capacity {label}', marker_color=color, visible='legendonly')
        fig.add_scatter(x=df['SNAP_DATE'], y=df['SEAT_FACTOR'], mode='lines+markers', name=f'Seat-Factor {label}', marker_color=color, visible='legendonly')
        fig.add_scatter(x=df['SNAP_DATE'], y=df['REVENUE'], mode='lines+markers', name=f'Revenue {label}', marker_color=color, visible=True)
        fig.add_scatter(x=df['SNAP_DATE'], y=df['RASK'], mode='lines+markers', name=f'RASK {label}', marker_color=color, visible='legendonly')
        fig.add_scatter(x=df['SNAP_DATE'], y=df['YIELD'], mode='lines+markers', name=f'Yield {label}', marker_color=color, visible='legendonly')

    # Set axis labels
    fig.update_layout(
        xaxis_title='SNAP_DATE',
        yaxis_title='Values'
    )

    # Return the plotly figure
    return fig
# Callback to update and display the aggregate graph
@app.callback(
    Output("aggr_graph", "figure"),
    [Input("submit-button", "n_clicks")]
)
def update_aggregate_graph(n_clicks):
    global origin_list, destination_list, seg_date_list, snap_date_list, flight_number_list

    # Check if all required options are selected
    if not all([origin_list, destination_list, snap_date_list, flight_number_list]):
        return {}

    # Call the aggregate plot function with selected options
    aggr_fig = aggregate_plot(flight_number_list, origin_list, destination_list, snap_date_list)

    # Return the plotly figure for the aggregate graph
    return aggr_fig

def aggregate(x):

        dataframes = x
        combined_df = dataframes[0]  # Start with the first dataframe

        for i in range(1, len(dataframes)):
            combined_df = pd.merge(combined_df, dataframes[i], on='SEG_DEP_DATE', how='outer')

        # Calculate the sum of "seats_sold" and create a new column "SEATS_SOLD"
        combined_df['SEATS_SOLD'] = combined_df.filter(like='SEATS_SOLD').fillna(0).sum(axis=1)
        combined_df['CAPACITY'] = combined_df.filter(like='CAPACITY').fillna(0).sum(axis=1)
        combined_df['REVENUE'] = combined_df.filter(like='REVENUE').fillna(0).sum(axis=1)
        combined_df['DISTANCE'] = combined_df.filter(like='DISTANCE').fillna(0).sum(axis=1)
        combined_df['ASK'] = combined_df['CAPACITY'] * combined_df['DISTANCE']
        combined_df['RPK'] = combined_df['SEATS_SOLD'] * combined_df['DISTANCE']
        combined_df['SEAT_FACTOR'] = combined_df.apply(lambda row: row['RPK'] / row['ASK'] if row['ASK'] != 0 else 0, axis=1)
        combined_df['RASK'] = combined_df.apply(lambda row: row['REVENUE'] / row['ASK'] if row['ASK'] != 0 else 0, axis=1)
        combined_df['YIELD'] = combined_df.apply(lambda row: row['REVENUE'] / row['RPK'] if row['RPK'] != 0 else 0, axis=1)

        # Keep only the required columns in the new dataframe
        columns_to_keep = ['SNAP_DATE_x', 'SEG_DEP_DATE', 'SEATS_SOLD', 'CAPACITY', 'REVENUE', 'DISTANCE', 'ASK', 'RPK', 'SEAT_FACTOR', 'RASK', 'YIELD']
        new_df = combined_df[columns_to_keep]

        # Rename the columns in the new dataframe
        new_df.columns = ['SNAP_DATE', 'SEG_DEP_DATE', 'SEATS_SOLD', 'CAPACITY', 'REVENUE', 'DISTANCE', 'ASK', 'RPK', 'SEAT_FACTOR', 'RASK', 'YIELD']
        new_df = new_df.sort_values(by='SEG_DEP_DATE')

        # Print the new dataframe
        return new_df

def aggr_plot(df):
        # Create a scatterplot using Plotly Express
        fig = px.scatter(df, x='SEG_DEP_DATE')

        # Add scatter traces for each data series
        fig.add_scatter(x=df['SEG_DEP_DATE'], y=df['SEATS_SOLD'], mode='lines+markers', name='Passengers')
        fig.add_scatter(x=df['SEG_DEP_DATE'], y=df['CAPACITY'], mode='lines+markers', name='Capacity')
        fig.add_scatter(x=df['SEG_DEP_DATE'], y=df['SEAT_FACTOR'], mode='lines+markers', name='Seat-Factor')
        fig.add_scatter(x=df['SEG_DEP_DATE'], y=df['REVENUE'], mode='lines+markers',  name='Revenue')
        fig.add_scatter(x=df['SEG_DEP_DATE'], y=df['RASK'], mode='lines+markers',  name='RASK')
        fig.add_scatter(x=df['SEG_DEP_DATE'], y=df['YIELD'], mode='lines+markers',  name='Yield')

        # Set axis labels
        fig.update_layout(
            xaxis_title='SEG_DEP_DATE',
            yaxis_title='Values'
        )

        # Show the scatterplot
        return fig

def aggregate_plot(flt_nums, leg_origins, leg_destinations, snap_dates):
    # Create a list to store the individual DataFrames
    dfs = []
    snap_date = snap_dates[0]

    for flt_num, leg_origin, leg_destination in zip(flt_nums, leg_origins, leg_destinations):
        if None in (flt_num, leg_origin, leg_destination):
            print(f"Skipping iteration due to 'None' value: FLTNUM: {flt_num}, LEG_ORIGIN: {leg_origin}, LEG_DESTINATION: {leg_destination}")
            continue
        
        # Prepare the SQL query
        query = f"SELECT * FROM INVENTORY_SAMPLE_COPY WHERE FLTNUM = {flt_num} AND LEG_ORIGIN = '{leg_origin}' AND LEG_DESTINATION = '{leg_destination}' AND SNAP_DATE = '{snap_date}' ORDER BY SEG_DEP_DATE ASC"

        # Execute the query and fetch the results into a DataFrame
        df = pd.read_sql(query, engine).rename(columns=str.upper)

        # Check if the data exists
        if df.empty:
            print(f"No data available for FLTNUM: {flt_num}, LEG_ORIGIN: {leg_origin}, LEG_DESTINATION: {leg_destination}, SNAP_DATE: {snap_date}")
        else:
            # Append the current DataFrame to the list
            dfs.append(df)
        
    newdf = aggregate(dfs)
    fig = aggr_plot(newdf)
    return fig

# Run the app
if __name__ == "__main__":
    app.run_server()


# In[ ]:




