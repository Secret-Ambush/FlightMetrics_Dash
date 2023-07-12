import pandas as pd
import os
import dash_bootstrap_components as dbc
import dash
from dash import Dash, html, ctx, callback, dcc
import pandas as pd
import plotly.express as px
import sqlalchemy
from dash.dependencies import Input, Output, State
from snowflake.sqlalchemy import URL
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import plotly.graph_objects as go
import plotly.io as pio
from termcolor import colored

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
            html.A(
                html.Img(
                    src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d0/Emirates_logo.svg/1200px-Emirates_logo.svg.png",
                    style={"width": "100px", "height": "100px", "margin": "20px", "align-self": "flex-start", "margin-top": "-140px"}
                ),
                href="https://www.emirates.com/ae/english/"  # Specify the URL to redirect to
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
#                             value = "2021-09-15",
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
#                             value = "2021-09-15",
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
                    dbc.Button("Add", id="add-button", n_clicks=0),
                    style={"width": "100px", "margin": "10px"}  # Modify button style
                ),
                html.Div(
                    dbc.Button("Clear All", id="clear-button", n_clicks=0, color = "danger"),
                    style={"width": "100px", "margin": "10px"}  # Modify button style
                ),
            ],
            style={"display": "flex", "justify-content": "space-between"}
        ),
        html.Br(),
        html.Div(id="selected-options"),
        html.Div(id="stored-options", style={"display": "none"}),
        html.Br(),
        html.Br(),
        html.Div(
            dbc.Button("PLOT", id="submit-button", color = "success", n_clicks=0),
            style={"display": "flex", "justify-content": "center", "margin-top": "10px"}
        ),
        html.Br(),
        html.Div(id="errortxt", style={'textAlign': 'center','fontSize': '20px'}),
        html.Br(),
        html.Br(),
        html.Br(),
        dbc.Stack([
        dcc.Markdown("#### **‏‏‎ ‎‏‏‎ ‎‏‏‎ ‎Booking Curve (Aggregated):**"),
        dcc.Loading(
            id="loading-2",
            type="default",
            children=[
                html.Div(
                    style={"display": "flex", "justify-content": "center", "align-items": "center", "height": "40vh"},
                    children=[
                        html.Div(
                            dcc.Graph(id="graph"),  # Add a div to hold the graph
                            style={"width": "80%", "height": "400px"},
                        )
                    ]
                )
            ],
        ),
        html.Br(),
        dcc.Markdown("#### **‏‏‎ ‎‏‏‎ ‎‏‏‎ ‎Aggregate Graph:**"),
        html.Br(),
        html.Div(id="errortxt2", style={'textAlign': 'center', 'fontSize': '20px'}),
        dcc.Loading(
            id="loading-1",
            type="default",
            children=[
                html.Div(
                    style={"display": "flex", "justify-content": "center", "align-items": "center", "height": "40vh"},
                    children=[
                        html.Div(
                            dcc.Graph(id="aggr_graph"),  # Add a div to hold the graph
                            style={"width": "80%", "height": "400px"},
                        )
                    ]
                )
            ],
        ),
        html.Br(),
        html.Br(),
        ],
            gap = 5,
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

@callback(Output("loading-output-1", "children"), Input("aggr_graph", "value"))
def input_triggers_spinner(value):
    time.sleep(5)
    return value

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
    [Input("origin-dropdown", "value"),
    Input("dest-dropdown", "value")]
)
def update_destinations(selected_origins, selected_destinations):
    dest = list(selected_destinations or [])
    if selected_origins:
        query = f"""
            SELECT DISTINCT LEG_DESTINATION
            FROM {snowflake_table}
            WHERE LEG_ORIGIN = '{selected_origins[-1]}'
            ORDER BY LEG_DESTINATION
        """
        with engine.connect() as conn:
            result = conn.execute(query)
            destinations=[]
            destinations = [row[0] for row in result]
            if len(dest):
                destinations = selected_destinations + destinations
            return [{"label": row, "value": row} for row in destinations]
    return []


# Callback to update selected snap dates list
@app.callback(
    Output("snap-date-dropdown", "options"),
    [Input("origin-dropdown", "value"),
     Input("dest-dropdown", "value")]
)
def update_snap_dates(selected_origins, selected_destinations):
    if selected_origins and selected_destinations:
        snap_date_options_query = f"""
            SELECT DISTINCT SNAP_DATE
            FROM {snowflake_table}
            WHERE LEG_ORIGIN = '{selected_origins[-1]}'
            AND LEG_DESTINATION = '{selected_destinations[-1]}' ORDER BY SNAP_DATE DESC
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
        seg_date_options_query = f"""
            SELECT DISTINCT SEG_DEP_DATE
            FROM {snowflake_table}
            WHERE LEG_ORIGIN = '{selected_origins[-1]}'
            AND LEG_DESTINATION = '{selected_destinations[-1]}' ORDER BY SEG_DEP_DATE DESC
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
    Input("seg-date-dropdown", "value"),
    Input("flight-no", "value")]
)
def update_flight_numbers(selected_origins, selected_destinations, selected_snap_dates, selected_seg_dates, selected_flightno):
    flight = list(selected_flightno or [])
    if selected_origins and selected_destinations and selected_snap_dates and selected_seg_dates:
        snap_dates_str = selected_snap_dates
        seg_dates_str = selected_seg_dates
        flight_number_options_query = f"""
            SELECT DISTINCT FLTNUM
            FROM {snowflake_table}
            WHERE LEG_ORIGIN = '{selected_origins[-1]}'
            AND LEG_DESTINATION = '{selected_destinations[-1]}'
            AND SNAP_DATE = '{snap_dates_str}' ORDER BY FLTNUM
        """
        with engine.connect() as conn:
            flight_number_options = conn.execute(flight_number_options_query)
            options = [row[0] for row in flight_number_options]
            if len(flight):
                options = selected_flightno + options
            return [{"label": row, "value": row} for row in options]
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
    if not(len(origin_list)==len(destination_list)==len(flight_number_list)):
        error_msg = "⚠Number of selected options for Origin, Destination and Flight Number does not match. You might not get the output for all graphs⚠"
    if not all([origin_list, destination_list, seg_date_list, flight_number_list]):
        revq = f"SELECT MAX(REVENUE) as REVENUE FROM INVENTORY_SAMPLE_COPY"
        revt = pd.read_sql(revq, engine).rename(columns=str.upper)
        rev = revt.at[0,'REVENUE']
        df = pd.read_sql(f"SELECT FLTNUM, LEG_ORIGIN, LEG_DESTINATION, REVENUE FROM INVENTORY_SAMPLE_COPY WHERE REVENUE = '{rev}'",engine)
        print(df)
        error_msg = "Displaying the flight where Revenue is maximum FLTNUM:" + df['fltnum'].values + ", ORG: " + df['leg_origin'].values + ", DEST: " + df['leg_destination'].values + ". ✈ Select your flight for the respective graph"
    if all([origin_list, destination_list, seg_date_list, flight_number_list]):    
        error_msg = "Displaying the graph for your selection"
    # Call the plot function with selected options
    fig = booking_plot(flight_number_list, origin_list, destination_list, seg_date_list)
    # Return the plotly figure
    return fig,error_msg

def book_aggr(x):
    
        dataframes = x
        combined_df = dataframes[0]  # Start with the first dataframe

        n = len(dataframes) // 3 + len(dataframes)

        # Process the dataframes three at a time
        for i in range(0, n, 3):
            # Get the current three dataframes
            current_dataframes = dataframes[i:i + 3]

            # Combine the current dataframes based on "seg_dep_date"
            combined_df = current_dataframes[0].reset_index()  # Start with the first dataframe
            for j in range(1, len(current_dataframes)):
                combined_df = pd.merge(combined_df, current_dataframes[j], on='SNAP_DATE', how='outer')

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
            columns_to_keep = ['SNAP_DATE', 'SEATS_SOLD', 'CAPACITY', 'REVENUE', 'DISTANCE', 'ASK', 'RPK', 'SEAT_FACTOR', 'RASK', 'YIELD']
            new_df = combined_df[columns_to_keep]

            # Rename the columns in the new dataframe
            new_df.columns = ['SNAP_DATE', 'SEATS_SOLD', 'CAPACITY', 'REVENUE', 'DISTANCE', 'ASK', 'RPK', 'SEAT_FACTOR', 'RASK', 'YIELD']
            # Append the new dataframe tothe list
            dataframes.append(new_df)
            
        new_df = new_df.sort_values(by='SNAP_DATE')
        # Print the new dataframes
        print("YES!")
        return new_df

def book_plot(df):
        # Create a scatterplot using Plotly Express
        fig = go.Figure()
        fig.update_layout(plot_bgcolor='black')

        # Add scatter traces for each data series
        fig.add_trace(go.Scatter(x=df['SNAP_DATE'], y=df['SEATS_SOLD'], mode='lines+markers', name='Passengers', visible='legendonly'))
        fig.add_trace(go.Scatter(x=df['SNAP_DATE'], y=df['CAPACITY'], mode='lines+markers', name='Capacity', visible='legendonly'))
        fig.add_trace(go.Scatter(x=df['SNAP_DATE'], y=df['SEAT_FACTOR'], mode='lines+markers', name='Seat-Factor', visible='legendonly'))
        fig.add_trace(go.Scatter(x=df['SNAP_DATE'], y=df['REVENUE'], mode='lines+markers', name='Revenue', visible= True,line=dict(color='violet')))
        fig.add_trace(go.Scatter(x=df['SNAP_DATE'], y=df['RASK'], mode='lines+markers', name='RASK', visible='legendonly'))
        fig.add_trace(go.Scatter(x=df['SNAP_DATE'], y=df['YIELD'], mode='lines+markers', name='Yield', visible='legendonly',line=dict(color='yellow')))

        # Set axis labels
        fig.update_layout(
            xaxis_title='SNAP_DATE',
            yaxis_title='Values'
        )
#         pio.show(fig)
        return fig

def booking_plot(flt_nums, leg_origins, leg_destinations, seg_dates):
    # Create a list to store the individual DataFrames
    dfs = []
    
    if(len(seg_dates)!=0):
        seg_date = seg_dates[0]
    
    if(len(seg_dates)==0):
        revq = f"SELECT MAX(REVENUE) as REVENUE FROM INVENTORY_SAMPLE_COPY"
        revt = pd.read_sql(revq, engine).rename(columns=str.upper)
        rev = revt.at[0,'REVENUE']
        q = f"SELECT max(SEG_DEP_DATE) SEG_DEP_DATE FROM INVENTORY_SAMPLE_COPY WHERE REVENUE = '{rev}'"
        latest = pd.read_sql(q, engine).rename(columns=str.upper)
        seg_date = latest.at[0, 'SEG_DEP_DATE']
        
    if(len(flt_nums)==0 and len(leg_origins)==0 and len(leg_destinations)==0 and len(seg_dates)==0):

        # Prepare the SQL query
        query = f"SELECT SNAP_DATE,SUM(SEATS_SOLD) SEATS_SOLD,SUM(CAPACITY) CAPACITY,SUM(REVENUE) REVENUE, SUM(DISTANCE) DISTANCE FROM INVENTORY_SAMPLE_COPY WHERE SEG_DEP_DATE = '{seg_date}' GROUP BY SNAP_DATE ORDER BY SNAP_DATE ASC"
        
        # Execute the query and fetch the results into a DataFrame
        df = pd.read_sql(query, engine).rename(columns=str.upper)
        
        df['ASK'] = df['CAPACITY']*df['DISTANCE']
        df['RASK'] = df['REVENUE']/df['ASK']
        df['RPK'] = df['SEATS_SOLD']*df['DISTANCE']
        df['YIELD'] = df['REVENUE']/df['RPK']
        df['SEAT_FACTOR'] = df['RPK']/df['ASK']
        
        # Check if the data exists
        if df.empty:
            print(f"No data available")
        else:
            # Append the current DataFrame to the list
            dfs.append(df)
        
        fig = book_plot(df)
        return fig
    
    else:
        for flt_num, leg_origin, leg_destination in zip(flt_nums, leg_origins, leg_destinations):

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
        
        newdf = book_aggr(dfs)
        fig = book_plot(newdf)
        return fig
    
# Callback to update and display the aggregate graph
@app.callback(
    Output("aggr_graph", "figure"),
    Output("errortxt2", "children"),
    [Input("submit-button", "n_clicks")]
)
def update_aggregate_graph(n_clicks):
    global origin_list, destination_list, seg_date_list, snap_date_list, flight_number_list

    # Call the aggregate plot function with selected options
    aggr_fig,errormsg = aggregate_plot(flight_number_list, origin_list, destination_list, snap_date_list)

    # Return the plotly figure for the aggregate graph
    return aggr_fig,errormsg

def aggregate(x):
    
        dataframes = x
        combined_df = dataframes[0]  # Start with the first dataframe

        n = len(dataframes) // 3 + len(dataframes)

        # Process the dataframes three at a time
        for i in range(0, n, 3):
            # Get the current three dataframes
            current_dataframes = dataframes[i:i + 3]

            # Combine the current dataframes based on "seg_dep_date"
            combined_df = current_dataframes[0].reset_index()  # Start with the first dataframe
            for j in range(1, len(current_dataframes)):
                combined_df = pd.merge(combined_df, current_dataframes[j], on='SEG_DEP_DATE', how='outer')

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
            columns_to_keep = ['SEG_DEP_DATE', 'SEATS_SOLD', 'CAPACITY', 'REVENUE', 'DISTANCE', 'ASK', 'RPK', 'SEAT_FACTOR', 'RASK', 'YIELD']
            new_df = combined_df[columns_to_keep]

            # Rename the columns in the new dataframe
            new_df.columns = ['SEG_DEP_DATE', 'SEATS_SOLD', 'CAPACITY', 'REVENUE', 'DISTANCE', 'ASK', 'RPK', 'SEAT_FACTOR', 'RASK', 'YIELD']
            # Append the new dataframe tothe list
            dataframes.append(new_df)
            
        new_df = new_df.sort_values(by='SEG_DEP_DATE')
        # Print the new dataframes
        return new_df

def aggr_plot(df):
        # Create a scatterplot using Plotly Express
        fig = go.Figure()
        fig.update_layout(plot_bgcolor='black')

        # Add scatter traces for each data series
        fig.add_trace(go.Scatter(x=df['SEG_DEP_DATE'], y=df['SEATS_SOLD'], mode='lines+markers', name='Passengers', visible='legendonly'))
        fig.add_trace(go.Scatter(x=df['SEG_DEP_DATE'], y=df['CAPACITY'], mode='lines+markers', name='Capacity', visible='legendonly'))
        fig.add_trace(go.Scatter(x=df['SEG_DEP_DATE'], y=df['SEAT_FACTOR'], mode='lines+markers', name='Seat-Factor', visible='legendonly'))
        fig.add_trace(go.Scatter(x=df['SEG_DEP_DATE'], y=df['REVENUE'], mode='lines+markers', name='Revenue', visible= True))
        fig.add_trace(go.Scatter(x=df['SEG_DEP_DATE'], y=df['RASK'], mode='lines+markers', name='RASK', visible='legendonly'))
        fig.add_trace(go.Scatter(x=df['SEG_DEP_DATE'], y=df['YIELD'], mode='lines+markers', name='Yield', visible='legendonly'))

        # Set axis labels
        fig.update_layout(
            xaxis_title='SEG_DEP_DATE',
            yaxis_title='Values'
        )

        return fig

def aggregate_plot(flt_nums, leg_origins, leg_destinations, snap_dates):
    # Create a list to store the individual DataFrames
    dfs = []
    
    if(len(snap_dates)!=0):
        snap_date = snap_dates[0]
    
    if(len(snap_dates)==0):
        q = f"SELECT max(SNAP_DATE) as SNAP_DATE FROM INVENTORY_SAMPLE_COPY"
        latest = pd.read_sql(q, engine).rename(columns=str.upper)
        snap_date = latest.at[0, 'SNAP_DATE']
        
    if(len(flt_nums)==0 and len(leg_origins)==0 and len(leg_destinations)==0 and len(snap_dates)==0):

        # Prepare the SQL query
        query = f"SELECT SEG_DEP_DATE,SUM(SEATS_SOLD) SEATS_SOLD,SUM(CAPACITY) CAPACITY,SUM(REVENUE) REVENUE, SUM(DISTANCE) DISTANCE FROM INVENTORY_SAMPLE_COPY WHERE SNAP_DATE = '{snap_date}' GROUP BY SEG_DEP_DATE ORDER BY SEG_DEP_DATE ASC"
        
        # Execute the query and fetch the results into a DataFrame
        df = pd.read_sql(query, engine).rename(columns=str.upper)
        
        df['ASK'] = df['CAPACITY']*df['DISTANCE']
        df['RASK'] = df['REVENUE']/df['ASK']
        df['RPK'] = df['SEATS_SOLD']*df['DISTANCE']
        df['YIELD'] = df['REVENUE']/df['RPK']
        df['SEAT_FACTOR'] = df['RPK']/df['ASK']
        
        # Check if the data exists
        if df.empty:
            print(f"No data available")
        else:
            # Append the current DataFrame to the list
            dfs.append(df)
        
        msg = "The aggregate of "+str(len(df))+" flights, for the latest Snap Date : "+str(snap_date)
        fig = aggr_plot(df)
        return fig,msg
    
    else:
        for flt_num, leg_origin, leg_destination in zip(flt_nums, leg_origins, leg_destinations):

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
        return fig,"Required plot: "
#--------------------------------------------------------------------------------------------------------------------

# Run the app
if __name__ == "__main__":
    app.run_server()
