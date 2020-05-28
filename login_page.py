import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])


email_input = dbc.FormGroup(
    [
        dbc.Label("Email", html_for="example-email"),
        dbc.Input(type="email", id="example-email", placeholder="Enter email"),

    ]
)

password_input = dbc.FormGroup(
    [
        dbc.Label("Password", html_for="example-password"),
        dbc.Input(
            type="password",
            id="example-password",
            placeholder="Enter password",
            width = '16px'
        ),
    ]
)

form = dbc.Form([email_input, password_input])

app.layout = html.Div([
    dbc.Alert("enter the credentials", color="success"),form]
)

if __name__ == "__main__":
    app.run_server()
