import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
from dash.exceptions import PreventUpdate
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

gpa = pd.read_csv('https://raw.githubusercontent.com/wadefagen/datasets/master/gpa/uiuc-gpa-dataset.csv')

all_subjects = gpa.Subject.unique()
all_statnumber = gpa[gpa.Subject == 'STAT'].Number.unique()
all_number = gpa.Number.unique()

app.layout = html.Div([
    html.H1('Grade difference between different instructors for every courses at UIUC by Jeffrey Zhao'),
    html.Div('''
        The goal of this project is to see the difference between instructors for a specific class the user selected
    '''),
    html.Label("Subject:", style = {'fontSize':20}),
    dcc.Dropdown(
        id='subject_dpdn',
        options=[{'label': i, 'value': i} for i in all_subjects],
        searchable = True,
        placeholder = 'Please select or search the subject...'
    ),
    html.Label("Course number:", style = {'fontSize':20}),
    dcc.Dropdown(
        id='number_dpdn',
        options=[],
        placeholder = 'Please select or search the course number...'
    ),
    dcc.Graph(id='stat430'),
])

#set number options
@app.callback(
    Output('number_dpdn', 'options'),
    Input('subject_dpdn', 'value'))
def set_number_dropdown_value(selected_subject):
    gpa_subject_selected = gpa[gpa['Subject'] == selected_subject]
    return [{'label': i, 'value': i} for i in gpa_subject_selected.Number.unique()]

#set number values
@app.callback(
    Output('number_dpdn', 'value'),
    Input('number_dpdn', 'options'))
def set_number_value(avaliable_options):
    return [x['value'] for x in avaliable_options]

#Populate the graphs
@app.callback(
    Output('stat430', 'figure'),
    Input('subject_dpdn', 'value'),
    Input('number_dpdn', 'value'))
def update_graph(subject_dpdn, number_dpdn):
    gb_gpa = gpa.loc[(gpa['Subject'] == subject_dpdn) & (gpa['Number'] == number_dpdn)]
    if len(str(gb_gpa)) == 0 :
        raise PreventUpdate
    else :
        gb = gb_gpa.groupby(['Primary Instructor'], as_index = False)[["A+", "A", "A-","B+","B","B-", "C+","C","C-", "D+","D","D-","F","W"]].sum()
        gb['Total A'] = gb.iloc[:,1:4].sum(axis = 1)
        gb['Total B'] = gb.iloc[:,4:7].sum(axis = 1)
        gb['Total C'] = gb.iloc[:,7:10].sum(axis = 1)
        gb['Total D'] = gb.iloc[:,10:13].sum(axis = 1)
        gb['Totals'] = gb.iloc[:,13:20].sum(axis = 1)
        gb['Total A'] = gb['Total A']/gb['Totals'] 
        gb['Total B'] = gb['Total B']/gb['Totals'] 
        gb['Total C'] = gb['Total C']/gb['Totals'] 
        gb['Total D'] = gb['Total D']/gb['Totals']
        gb['F'] = gb['F']/gb['Totals']
        gb['W'] = gb['W']/gb['Totals']
        fig = px.bar(gb, y="Primary Instructor", x = ["Total A", "Total B", "Total C","Total D", "F","W"],
            barmode = 'relative', orientation = 'h', template = "plotly_dark", title = "Grade difference between instructors of the course selected")
        return fig

if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_ui=False, dev_tools_props_check=False)
