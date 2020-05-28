import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])


machine_name_input = dbc.FormGroup(
    [
        dbc.Label("machine name", html_for="example-email"),
        dbc.Input(type="text", id="machine-name-input"),

    ]
)

sensor_type_input = dbc.FormGroup(
    [
        dbc.Label("sensor type"),
        dbc.Input(type="text", id="sensor-type-input"),

    ]
)

power_type_input = dbc.FormGroup(
    [
        dbc.Label("power supply type"),
        dbc.Input(type="text", id="power-type-input"),

    ]
)

power_factor_input = dbc.FormGroup(
    [
        dbc.Label("power factor"),
        dbc.Input(type="text", id="power-factor-input"),

    ]
)

voltage_input = dbc.FormGroup(
    [
        dbc.Label("voltage"),
        dbc.Input(type="text", id="voltage-input"),

    ]
)

peak_current_input = dbc.FormGroup(
    [
        dbc.Label("peak current"),
        dbc.Input(type="text", id="peak-current-input"),

    ]
)

min_current_input = dbc.FormGroup(
    [
        dbc.Label("min current"),
        dbc.Input(type="text", id="min-current-input"),

    ]
)

description_input = dbc.FormGroup(
    [
        dbc.Label("Description"),
         dbc.Textarea(bs_size="lg", placeholder="enter the description",id="Description-input"),

    ]
)

form = dbc.Form([machine_name_input, sensor_type_input,power_type_input,voltage_input,peak_current_input,min_current_input,description_input])

app.layout = html.Div([form])
if __name__ == "__main__":
    app.run_server()
